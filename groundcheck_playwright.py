#!/usr/bin/env python3
import argparse
import asyncio
import csv
import os
import re
from typing import List, NotRequired, TypedDict
from urllib.parse import quote

from playwright.async_api import (
    Browser,
    ElementHandle,
    Page,
    TimeoutError as PlaywrightTimeoutError,
)
from playwright.async_api import async_playwright
from dotenv import load_dotenv
from rapidfuzz import fuzz
from tqdm import tqdm

COORD_PATTERN = re.compile(r"!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)")
FALLBACK_PATTERN = re.compile(r"@(-?\d+\.\d+),(-?\d+\.\d+),")
WORD_CLEANER = re.compile(r"[^a-z0-9\s]+")
load_dotenv()

SIMILARITY_THRESHOLD = 50.0
NAV_TIMEOUT_MS = 20000
RESULT_WAIT_MS = 8000
DEFAULT_CITY = os.getenv("CITY", "Alak")


class SearchResult(TypedDict):
    element: ElementHandle
    name: str
    address: str
    href: str


class GeocodeResult(TypedDict):
    lat: float | None
    lng: float | None
    url: str
    gmaps_name: str
    gmaps_address: str
    gmaps_status: NotRequired[str]
    score: float
    error: NotRequired[str]


def extract_lat_lng(url: str):
    match = COORD_PATTERN.search(url)
    if not match:
        match = FALLBACK_PATTERN.search(url)
    if not match:
        return None, None
    return float(match.group(1)), float(match.group(2))


def normalize_text(value: str) -> str:
    normalized = value.lower().strip()
    normalized = WORD_CLEANER.sub(" ", normalized)
    normalized = " ".join(normalized.split())
    return normalized


def similarity_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return float(fuzz.WRatio(a, b))


def similarity_score(
    input_name: str, input_address: str, gmaps_name: str, gmaps_address: str
) -> float:
    combined_input = f"{input_name} {input_address}".strip()
    combined_target = f"{gmaps_name} {gmaps_address}".strip()
    return similarity_ratio(
        normalize_text(combined_input), normalize_text(combined_target)
    )


def city_is_valid(address: str, city: str) -> bool:
    if not city:
        return True
    if not address:
        return False
    return city.lower() in address.lower()


async def get_detail_info(page: Page) -> tuple[str, str]:
    gmaps_name = ""
    gmaps_address = ""
    name_el = await page.query_selector("h1.DUwDvf")
    if name_el:
        text = await name_el.inner_text()
        gmaps_name = text.strip() if text else ""
    address_el = await page.query_selector('button[data-item-id="address"]')
    if address_el:
        text = await address_el.inner_text()
        gmaps_address = text.strip() if text else ""
    return gmaps_name, gmaps_address


async def get_place_status(page: Page) -> str:
    keywords = [
        "permanently closed",
        "temporarily closed",
        "tutup permanen",
        "tutup sementara",
    ]
    try:
        text = await page.inner_text("body")
    except PlaywrightTimeoutError:
        return ""
    lowered = text.lower()
    for keyword in keywords:
        if keyword in lowered:
            return keyword
    return ""


async def get_search_results(page: Page, limit: int = 5) -> List[SearchResult]:
    results = await page.query_selector_all('div[role="article"]')
    output = []
    for result in results[:limit]:
        name_el = await result.query_selector(".qBF1Pd")
        if name_el:
            text = await name_el.inner_text()
            gmaps_name = text.strip() if text else ""
        else:
            gmaps_name = ""
        address_el = await result.query_selector(".W4Efsd")
        if address_el:
            text = await address_el.inner_text()
            gmaps_address = text.strip() if text else ""
        else:
            gmaps_address = ""
        link_el = await result.query_selector('a[href*="/maps/place/"]')
        href = await link_el.get_attribute("href") if link_el else ""
        output.append(
            {
                "element": result,
                "name": gmaps_name,
                "address": gmaps_address,
                "href": href or "",
            }
        )
    return output


async def geocode_name(
    page: Page,
    query: str,
    input_name: str,
    input_address: str,
    wait_ms: int,
    city: str = "",
) -> GeocodeResult:
    search_url = f"https://www.google.com/maps/search/{quote(query)}"
    await page.goto(search_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    try:
        await page.wait_for_selector(
            "h1.DUwDvf, div[role='article']", timeout=RESULT_WAIT_MS
        )
    except PlaywrightTimeoutError:
        # Continue anyway; sometimes Maps shows results without matching selectors yet.
        pass
    await page.wait_for_timeout(wait_ms)
    current_url = page.url

    if "/place/" in current_url:
        gmaps_name, gmaps_address = await get_detail_info(page)
        gmaps_status = await get_place_status(page)
        if city and not city_is_valid(gmaps_address, city):
            return {
                "lat": None,
                "lng": None,
                "url": current_url,
                "gmaps_name": gmaps_name,
                "gmaps_address": gmaps_address,
                "gmaps_status": gmaps_status,
                "score": 0.0,
                "error": f"Lokasi bukan di {city}",
            }
        score = similarity_score(input_name, input_address, gmaps_name, gmaps_address)
        lat, lng = extract_lat_lng(current_url)
        return {
            "lat": lat,
            "lng": lng,
            "url": current_url,
            "gmaps_name": gmaps_name,
            "gmaps_address": gmaps_address,
            "gmaps_status": gmaps_status,
            "score": score,
        }

    results = await get_search_results(page, limit=5)
    if not results:
        return {
            "lat": None,
            "lng": None,
            "url": current_url,
            "gmaps_name": "",
            "gmaps_address": "",
            "score": 0.0,
            "error": "timeout menunggu hasil",
        }

    best = None
    best_score = 0.0
    best_index = -1
    for idx, result in enumerate(results, start=1):
        score = similarity_score(
            input_name, input_address, result["name"], result["address"]
        )
        print(f"  [{idx}] {result['name'][:40]} - {result['address'][:40]}")
        print(f"      Similarity: {score:.1f}%")
        if score > best_score:
            best_score = score
            best = result
            best_index = idx

    if not best or best_score < SIMILARITY_THRESHOLD:
        error_message = (
            "Tidak ada hasil yang cocok "
            f"(best score: {best_score:.1f}%, threshold: {SIMILARITY_THRESHOLD:.0f}%)"
        )
        return {
            "lat": None,
            "lng": None,
            "url": current_url,
            "gmaps_name": best["name"] if best else "",
            "gmaps_address": best["address"] if best else "",
            "score": best_score,
            "error": error_message,
        }

    print(f"Best match: [{best_index}] with score {best_score:.1f}%")
    print(f"   Nama: {best.get('name', '')}")
    print(f"   Alamat: {best.get('address', '')}")

    href = best.get("href", "")
    if href:
        if href.startswith("/"):
            href = f"https://www.google.com{href}"
        await page.goto(
            href, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS
        )
    else:
        await best["element"].click()
        await page.wait_for_timeout(wait_ms)

    try:
        await page.wait_for_url("**/maps/place/**", timeout=RESULT_WAIT_MS)
    except PlaywrightTimeoutError:
        pass
    await page.wait_for_timeout(wait_ms)
    detail_url = page.url
    gmaps_name, gmaps_address = await get_detail_info(page)
    gmaps_status = await get_place_status(page)
    print("Validating location is in city...")
    if city and not city_is_valid(gmaps_address, city):
        return {
            "lat": None,
            "lng": None,
            "url": detail_url,
            "gmaps_name": gmaps_name,
            "gmaps_address": gmaps_address,
            "gmaps_status": gmaps_status,
            "score": 0.0,
            "error": f"Lokasi bukan di {city}",
        }
    lat, lng = extract_lat_lng(detail_url)

    final_score = similarity_score(input_name, input_address, gmaps_name, gmaps_address)
    return {
        "lat": lat,
        "lng": lng,
        "url": detail_url,
        "gmaps_name": gmaps_name,
        "gmaps_address": gmaps_address,
        "gmaps_status": gmaps_status,
        "score": max(best_score, final_score),
    }


async def geocode_address_only(
    page: Page, address: str, input_name: str, wait_ms: int, city: str = ""
) -> GeocodeResult:
    query = f"{address}, {city}".strip()
    search_url = f"https://www.google.com/maps/search/{quote(query)}"
    await page.goto(search_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    try:
        await page.wait_for_selector(
            "h1.DUwDvf, div[role='article']", timeout=RESULT_WAIT_MS
        )
    except PlaywrightTimeoutError:
        # Continue anyway; sometimes Maps shows results without matching selectors yet.
        pass
    await page.wait_for_timeout(wait_ms)
    current_url = page.url

    if "/place/" in current_url:
        gmaps_name, gmaps_address = await get_detail_info(page)
        gmaps_status = await get_place_status(page)
        if city and not city_is_valid(gmaps_address, city):
            return {
                "lat": None,
                "lng": None,
                "url": current_url,
                "gmaps_name": gmaps_name,
                "gmaps_address": gmaps_address,
                "gmaps_status": gmaps_status,
                "score": 0.0,
                "error": f"Lokasi bukan di {city}",
            }
        score = (
            similarity_score(input_name, address, gmaps_name, gmaps_address)
            if input_name
            else 100
        )
        lat, lng = extract_lat_lng(current_url)
        return {
            "lat": lat,
            "lng": lng,
            "url": current_url,
            "gmaps_name": gmaps_name,
            "gmaps_address": gmaps_address,
            "gmaps_status": gmaps_status,
            "score": score,
        }

    results = await get_search_results(page, limit=5)
    if not results:
        return {
            "lat": None,
            "lng": None,
            "url": current_url,
            "gmaps_name": "",
            "gmaps_address": "",
            "score": 0.0,
            "error": "timeout menunggu hasil",
        }

    best = None
    best_score = 0.0
    best_index = -1
    for idx, result in enumerate(results, start=1):
        score = (
            similarity_score(input_name, address, result["name"], result["address"])
            if input_name
            else similarity_score("", address, "", result["address"])
        )
        print(f"  [{idx}] {result['name'][:40]} - {result['address'][:40]}")
        print(f"      Similarity: {score:.1f}%")
        if score > best_score:
            best_score = score
            best = result
            best_index = idx

    if not best or best_score < SIMILARITY_THRESHOLD:
        error_message = (
            "Tidak ada hasil yang cocok "
            f"(best score: {best_score:.1f}%, threshold: {SIMILARITY_THRESHOLD:.0f}%)"
        )
        return {
            "lat": None,
            "lng": None,
            "url": current_url,
            "gmaps_name": best["name"] if best else "",
            "gmaps_address": best["address"] if best else "",
            "score": best_score,
            "error": error_message,
        }

    print(f"Best match: [{best_index}] with score {best_score:.1f}%")
    print(f"   Nama: {best.get('name', '')}")
    print(f"   Alamat: {best.get('address', '')}")

    href = best.get("href", "")
    if href:
        if href.startswith("/"):
            href = f"https://www.google.com{href}"
        await page.goto(
            href, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS
        )
    else:
        await best["element"].click()
        await page.wait_for_timeout(wait_ms)

    try:
        await page.wait_for_url("**/maps/place/**", timeout=RESULT_WAIT_MS)
    except PlaywrightTimeoutError:
        pass
    await page.wait_for_timeout(wait_ms)
    detail_url = page.url
    gmaps_name, gmaps_address = await get_detail_info(page)
    gmaps_status = await get_place_status(page)
    print("Validating location is in city...")
    if city and not city_is_valid(gmaps_address, city):
        return {
            "lat": None,
            "lng": None,
            "url": detail_url,
            "gmaps_name": gmaps_name,
            "gmaps_address": gmaps_address,
            "gmaps_status": gmaps_status,
            "score": 0.0,
            "error": f"Lokasi bukan di {city}",
        }
    lat, lng = extract_lat_lng(detail_url)

    final_score = (
        similarity_score(input_name, address, gmaps_name, gmaps_address)
        if input_name
        else similarity_score("", address, "", gmaps_address)
    )
    return {
        "lat": lat,
        "lng": lng,
        "url": detail_url,
        "gmaps_name": gmaps_name,
        "gmaps_address": gmaps_address,
        "gmaps_status": gmaps_status,
        "score": max(best_score, final_score),
    }


async def geocodeAllDirektoriUsaha(
    input_file: str,
    result_dir: str = "result",
    concurrency: int = 5,
    city: str = "",
) -> None:
    output_success = os.path.join(result_dir, "hasil_geocoding_sukses.csv")
    output_failed = os.path.join(result_dir, "hasil_geocoding_gagal.csv")

    if not os.path.exists(input_file):
        print(f"File {input_file} tidak ditemukan.")
        return

    os.makedirs(result_dir, exist_ok=True)

    with open(input_file, newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        all_data = list(reader)

    print(f"Mulai geocoding untuk {len(all_data)} data usaha...")
    print(f"Parallel processing: {concurrency} concurrent requests\n")

    success_results = []
    failed_results = []
    processed_count = 0

    geocode_success_headers = [
        "idsbr",
        "perusahaan_id",
        "nama_usaha",
        "alamat_usaha",
        "sumber_data",
        "gmaps_nama",
        "gmaps_alamat",
        "gmaps_status",
        "similarity_score",
        "latitude",
        "longitude",
        "url",
        "hasilgc",
        "geocode_method",
    ]
    geocode_failed_headers = [
        "idsbr",
        "perusahaan_id",
        "nama_usaha",
        "alamat_usaha",
        "sumber_data",
        "latitude",
        "longitude",
        "url",
        "hasilgc",
        "error",
        "geocode_method",
    ]

    def save_progress():
        with open(output_success, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=geocode_success_headers)
            writer.writeheader()
            writer.writerows(success_results)

        with open(output_failed, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=geocode_failed_headers)
            writer.writeheader()
            writer.writerows(failed_results)

    async def process_item(usaha: dict, browser: Browser, wait_ms: int) -> bool:
        nonlocal processed_count
        idsbr = usaha.get("idsbr", "")
        perusahaan_id = usaha.get("perusahaan_id") or idsbr or ""
        nama_usaha = usaha.get("nama_usaha", "")
        alamat_usaha = usaha.get("alamat_usaha", "")
        sumber_data = usaha.get("sumber_data", "")

        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="id-ID",
        )
        page = await context.new_page()

        try:
            query = f"{nama_usaha} {city}".strip() if city else nama_usaha
            result = await geocode_name(
                page, query, nama_usaha, alamat_usaha, wait_ms, city
            )

            processed_count += 1
            progress = f"[{processed_count}/{len(all_data)}]"

            if result["lat"] is not None and result["lng"] is not None:
                success_data = {
                    "idsbr": idsbr,
                    "perusahaan_id": perusahaan_id,
                    "nama_usaha": nama_usaha,
                    "alamat_usaha": alamat_usaha,
                    "sumber_data": sumber_data,
                    "gmaps_nama": result["gmaps_name"] or "",
                    "gmaps_alamat": result["gmaps_address"] or "",
                    "gmaps_status": result.get("gmaps_status") or "",
                    "similarity_score": result["score"] or 100,
                    "latitude": result["lat"],
                    "longitude": result["lng"],
                    "url": result["url"],
                    "hasilgc": 1,
                    "geocode_method": "name_address",
                }
                success_results.append(success_data)
                print(
                    f"{progress} OK {nama_usaha[:40]} -> {result['lat']}, {result['lng']} "
                    f"(match: {result['score']:.1f}%)"
                )
                return True

            failed_data = {
                "idsbr": idsbr,
                "perusahaan_id": perusahaan_id,
                "nama_usaha": nama_usaha,
                "alamat_usaha": alamat_usaha,
                "sumber_data": sumber_data,
                "latitude": None,
                "longitude": None,
                "url": result["url"],
                "hasilgc": 0,
                "error": result.get("error") or "Tidak ditemukan",
                "geocode_method": "name_address",
            }
            failed_results.append(failed_data)
            print(f"{progress} FAIL {nama_usaha[:40]} -> {failed_data['error']}")
            return False
        except Exception as error:
            processed_count += 1
            progress = f"[{processed_count}/{len(all_data)}]"
            failed_data = {
                "idsbr": idsbr,
                "perusahaan_id": perusahaan_id,
                "nama_usaha": nama_usaha,
                "alamat_usaha": alamat_usaha,
                "sumber_data": sumber_data,
                "latitude": None,
                "longitude": None,
                "url": "",
                "hasilgc": 0,
                "error": str(error),
                "geocode_method": "name_address",
            }
            failed_results.append(failed_data)
            print(f"{progress} FAIL {nama_usaha[:40]} -> Error: {error}")
            return False
        finally:
            await context.close()

    semaphore = asyncio.Semaphore(concurrency)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            with tqdm(total=len(all_data), desc="Geocoding batch", unit="row") as bar:
                for i in range(0, len(all_data), concurrency):
                    batch = all_data[i : i + concurrency]

                    async def run_with_semaphore(item):
                        async with semaphore:
                            return await process_item(item, browser, 3000)

                    await asyncio.gather(*(run_with_semaphore(item) for item in batch))
                    bar.update(len(batch))
                    save_progress()
                    print(
                        "Progress saved: "
                        f"{len(success_results)} sukses, "
                        f"{len(failed_results)} gagal\n"
                    )
        finally:
            await browser.close()

    save_progress()

    print("GEOCODING SELESAI")
    print(f"Total data: {len(all_data)}")
    print(f"Berhasil: {len(success_results)}")
    print(f"Gagal: {len(failed_results)}")
    print(f"File sukses: {output_success}")
    print(f"File gagal: {output_failed}")


async def retryGeocodeFailedByAddress(
    input_file: str, result_dir: str = "result", concurrency: int = 5, city: str = ""
) -> None:
    output_success = os.path.join(result_dir, "hasil_geocoding_sukses.csv")
    output_retry_success = os.path.join(result_dir, "hasil_retry_sukses.csv")
    output_retry_failed = os.path.join(result_dir, "hasil_retry_gagal.csv")

    if not os.path.exists(input_file):
        print(f"File {input_file} tidak ditemukan.")
        print("Jalankan dulu geocodeAllDirektoriUsaha untuk menghasilkan file tersebut")
        return

    with open(input_file, newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        failed_data = list(reader)

    print(f"Retry geocoding untuk {len(failed_data)} data yang gagal...")
    print("Strategi: Menggunakan ALAMAT saja (tanpa nama usaha)")
    print(f"Parallel processing: {concurrency} concurrent requests\n")

    retry_success_results = []
    retry_failed_results = []
    processed_count = 0

    existing_success_results = []
    if os.path.exists(output_success):
        with open(output_success, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_success_results = list(reader)

    geocode_success_headers = [
        "idsbr",
        "perusahaan_id",
        "nama_usaha",
        "alamat_usaha",
        "sumber_data",
        "gmaps_nama",
        "gmaps_alamat",
        "gmaps_status",
        "similarity_score",
        "latitude",
        "longitude",
        "url",
        "hasilgc",
        "geocode_method",
    ]
    geocode_failed_headers = [
        "idsbr",
        "perusahaan_id",
        "nama_usaha",
        "alamat_usaha",
        "sumber_data",
        "latitude",
        "longitude",
        "url",
        "hasilgc",
        "error",
        "geocode_method",
    ]

    def save_progress():
        with open(output_retry_success, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=geocode_success_headers)
            writer.writeheader()
            writer.writerows(retry_success_results)

        with open(output_retry_failed, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=geocode_failed_headers)
            writer.writeheader()
            writer.writerows(retry_failed_results)

        if retry_success_results:
            combined_success = existing_success_results + retry_success_results
            with open(output_success, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=geocode_success_headers)
                writer.writeheader()
                writer.writerows(combined_success)

    async def process_item(usaha: dict, browser: Browser, wait_ms: int) -> bool:
        nonlocal processed_count
        idsbr = usaha.get("idsbr", "")
        perusahaan_id = usaha.get("perusahaan_id") or idsbr or ""
        nama_usaha = usaha.get("nama_usaha", "")
        alamat_usaha = usaha.get("alamat_usaha", "")
        sumber_data = usaha.get("sumber_data", "")

        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="id-ID",
        )
        page = await context.new_page()

        try:
            result = await geocode_address_only(
                page, alamat_usaha, nama_usaha, wait_ms, city
            )

            processed_count += 1
            progress = f"[{processed_count}/{len(failed_data)}]"

            if result["lat"] is not None and result["lng"] is not None:
                success_data = {
                    "idsbr": idsbr,
                    "perusahaan_id": perusahaan_id,
                    "nama_usaha": nama_usaha,
                    "alamat_usaha": alamat_usaha,
                    "sumber_data": sumber_data,
                    "gmaps_nama": result["gmaps_name"] or "",
                    "gmaps_alamat": result["gmaps_address"] or "",
                    "gmaps_status": result.get("gmaps_status") or "",
                    "similarity_score": result["score"] or 100,
                    "latitude": result["lat"],
                    "longitude": result["lng"],
                    "url": result["url"],
                    "hasilgc": 1,
                    "geocode_method": "address_only",
                }
                retry_success_results.append(success_data)
                print(
                    f"{progress} OK {nama_usaha[:40]} -> {result['lat']}, {result['lng']} "
                    f"(match: {result['score']:.1f}%) [by address]"
                )
                return True

            failed_data_item = {
                "idsbr": idsbr,
                "perusahaan_id": perusahaan_id,
                "nama_usaha": nama_usaha,
                "alamat_usaha": alamat_usaha,
                "sumber_data": sumber_data,
                "latitude": None,
                "longitude": None,
                "url": result["url"],
                "hasilgc": 0,
                "error": result.get("error") or "Tidak ditemukan (alamat)",
                "geocode_method": "address_only",
            }
            retry_failed_results.append(failed_data_item)
            print(f"{progress} FAIL {nama_usaha[:40]} -> {failed_data_item['error']}")
            return False
        except Exception as error:
            processed_count += 1
            progress = f"[{processed_count}/{len(failed_data)}]"
            failed_data_item = {
                "idsbr": idsbr,
                "perusahaan_id": perusahaan_id,
                "nama_usaha": nama_usaha,
                "alamat_usaha": alamat_usaha,
                "sumber_data": sumber_data,
                "latitude": None,
                "longitude": None,
                "url": "",
                "hasilgc": 0,
                "error": str(error),
                "geocode_method": "address_only",
            }
            retry_failed_results.append(failed_data_item)
            print(f"{progress} FAIL {nama_usaha[:40]} -> Error: {error}")
            return False
        finally:
            await context.close()

    semaphore = asyncio.Semaphore(concurrency)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            with tqdm(total=len(failed_data), desc="Retry batch", unit="row") as bar:
                for i in range(0, len(failed_data), concurrency):
                    batch = failed_data[i : i + concurrency]

                    async def run_with_semaphore(item):
                        async with semaphore:
                            return await process_item(item, browser, 3000)

                    await asyncio.gather(*(run_with_semaphore(item) for item in batch))
                    bar.update(len(batch))
                    save_progress()
                    print(
                        "Progress saved: "
                        f"{len(retry_success_results)} sukses, "
                        f"{len(retry_failed_results)} gagal\n"
                    )
        finally:
            await browser.close()

    save_progress()

    print("RETRY GEOCODING SELESAI")
    print(f"Total data retry: {len(failed_data)}")
    print(f"Berhasil: {len(retry_success_results)}")
    print(f"Tetap gagal: {len(retry_failed_results)}")
    print(f"File retry sukses: {output_retry_success}")
    print(f"File retry gagal: {output_retry_failed}")
    total_success = len(existing_success_results) + len(retry_success_results)
    print(f"File sukses update: {output_success} ({total_success} total)")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Geocode CSV with Playwright.")
    parser.add_argument("--input", required=True, help="Input CSV path.")
    parser.add_argument("--output", help="Output CSV path (single mode only).")
    parser.add_argument(
        "--city",
        default=DEFAULT_CITY,
        help="Optional city suffix (defaults to CITY env var).",
    )
    parser.add_argument("--wait-ms", type=int, default=3000, help="Wait time.")
    parser.add_argument("--concurrency", type=int, default=5, help="Concurrency level.")
    parser.add_argument("--result-dir", default="result", help="Result directory.")
    parser.add_argument(
        "--headless",
        action="store_true",
        default=True,
        help="Run browser in headless mode (default).",
    )
    parser.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        help="Run browser with UI.",
    )
    parser.add_argument(
        "--mode",
        choices=["single", "batch", "retry"],
        default="single",
        help=(
            "Mode: single (original), batch (geocodeAllDirektoriUsaha), "
            "retry (retryGeocodeFailedByAddress)"
        ),
    )
    args = parser.parse_args()

    if args.mode == "batch":
        await geocodeAllDirektoriUsaha(
            args.input, args.result_dir, args.concurrency, args.city
        )
        return

    if args.mode == "retry":
        await retryGeocodeFailedByAddress(
            args.input, args.result_dir, args.concurrency, args.city
        )
        return

    if not args.output:
        raise SystemExit("--output is required for single mode.")

    with open(args.input, newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        raw_fieldnames = reader.fieldnames or []
        if isinstance(raw_fieldnames, str):
            fieldnames = [raw_fieldnames]
        else:
            fieldnames = list(raw_fieldnames)
        if "nama_usaha" not in fieldnames:
            raise SystemExit("Input CSV must include 'nama_usaha' column.")
        rows = list(reader)

    output_fields = fieldnames + [
        "gmaps_nama",
        "gmaps_alamat",
        "gmaps_status",
        "similarity_score",
        "latitude",
        "longitude",
        "url",
        "error",
    ]
    city = args.city.strip()
    total = len(rows)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=args.headless)
        page = await browser.new_page()

        output_rows = []
        with tqdm(total=total, desc="Geocoding single", unit="row") as bar:
            for index, row in enumerate(rows, start=1):
                name = (row.get("nama_usaha") or "").strip()
                address = (row.get("alamat_usaha") or "").strip()
                if not name:
                    row["latitude"] = ""
                    row["longitude"] = ""
                    row["url"] = ""
                    row["gmaps_nama"] = ""
                    row["gmaps_alamat"] = ""
                    row["similarity_score"] = ""
                    row["error"] = "nama_usaha empty"
                    output_rows.append(row)
                    bar.update(1)
                    continue

                query = f"{name} {city}".strip() if city else name
                print(f"[{index}/{total}] {query[:60]}")

                try:
                    result = await geocode_name(
                        page, query, name, address, args.wait_ms
                    )
                    lat = result["lat"]
                    lng = result["lng"]
                    url = result["url"]
                    row["gmaps_nama"] = result["gmaps_name"]
                    row["gmaps_alamat"] = result["gmaps_address"]
                    row["gmaps_status"] = result.get("gmaps_status") or ""
                    row["similarity_score"] = f"{result['score']:.1f}"
                    if lat is None or lng is None:
                        row["latitude"] = ""
                        row["longitude"] = ""
                        row["url"] = url
                        row["error"] = result.get("error") or "coordinates not found"
                    else:
                        row["latitude"] = f"{lat:.6f}"
                        row["longitude"] = f"{lng:.6f}"
                        row["url"] = url
                        row["error"] = ""
                except PlaywrightTimeoutError:
                    row["latitude"] = ""
                    row["longitude"] = ""
                    row["url"] = page.url if page else ""
                    row["gmaps_nama"] = ""
                    row["gmaps_alamat"] = ""
                    row["similarity_score"] = ""
                    row["error"] = "timeout"
                except Exception as exc:
                    row["latitude"] = ""
                    row["longitude"] = ""
                    row["url"] = page.url if page else ""
                    row["gmaps_nama"] = ""
                    row["gmaps_alamat"] = ""
                    row["similarity_score"] = ""
                    row["error"] = str(exc)

                output_rows.append(row)
                bar.update(1)

        await browser.close()

    with open(args.output, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(output_rows)


if __name__ == "__main__":
    asyncio.run(main())
