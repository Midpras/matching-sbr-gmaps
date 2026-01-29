# Groundcheck Geocoding (Playwright)

Tool ini hanya fokus pada proses geocoding Google Maps dari file spreadsheet.

## Prasyarat

- Python 3.10+
- Dependensi: `playwright`, `tqdm`, `rapidfuzz`, `python-dotenv`
- Setelah install deps, jalankan: `python -m playwright install`
- Dapat menggunakan package manager uv dan jalankan `uv add playwright tqdm rapidfuzz   python-dotenv` dan `uv run playwright install`. Jalankan `uv run groundcheck_playwright.py` dan masukkan argumentnya.

## Struktur Input CSV

Kolom minimum yang dibutuhkan:

- `idsbr`
- `nama_usaha`
- `alamat_usaha`
- `sumber_data` (boleh kosong)

## Output CSV

Semua mode batch/retry menghasilkan file di folder `result` (atau `--result-dir`):

- `hasil_geocoding_sukses.csv`
- `hasil_geocoding_gagal.csv`
- `hasil_retry_sukses.csv`
- `hasil_retry_gagal.csv`

Kolom output tambahan:

- `gmaps_nama`
- `gmaps_alamat`
- `similarity_score`
- `latitude`
- `longitude`
- `url`
- `hasilgc`
- `geocode_method`
- `error` (hanya untuk gagal)

## Mode

### 1) single

Memproses input CSV dan menulis hasil ke satu file output.

- Hasil disimpan ke `--output`.
- Cocok untuk testing kecil atau satu file hasil.

### 2) batch

Memproses seluruh data dengan paralel (`--concurrency`).

- Hasil sukses dan gagal dipisah ke file output.
- Data di bawah threshold atau tidak ketemu masuk ke `hasil_geocoding_gagal.csv`.

### 3) retry

Memproses ulang file gagal dari file `hasil_geocoding_gagal.csv` dan matching ke google maps dengan menggunakan alamat.

- Hasil baru disimpan ke `hasil_retry_sukses.csv` dan `hasil_retry_gagal.csv`.
- `hasil_geocoding_sukses.csv` akan di-update (append) dengan hasil retry sukses.

## Argumen

- `--input` (wajib): path file CSV input.
- `--output` (wajib untuk mode `single`): path file CSV output.
- `--mode`: `single` (default), `batch`, `retry`.
- `--city`: tambahan kota untuk query. Default diambil dari env `CITY` jika ada.
- `--wait-ms`: delay tambahan setelah load halaman (default 3000).
- `--concurrency`: jumlah proses paralel untuk mode batch/retry (default 5).
- `--result-dir`: folder output batch/retry (default `result`).
- `--headless`: jalankan browser tanpa UI (default).
- `--no-headless`: jalankan browser dengan UI.

## Contoh

Single:

```
python groundcheck_playwright.py --mode single --input direktori_alak.csv --output hasil_geocode.csv
```

Batch:

```
python groundcheck_playwright.py --mode batch --input direktori_alak.csv --concurrency 5
```

Retry:

```
python groundcheck_playwright.py --mode retry --input result/hasil_geocoding_gagal.csv --concurrency 5
```

## Catatan

- Threshold similarity saat ini `50%`.
- Jika kota tidak cocok (validasi `CITY`), data akan dianggap gagal.
- Google Maps bisa menampilkan halaman consent; jika sering timeout, coba `--no-headless` untuk melihat kondisi halaman.
- Hasil tidak selalu akurat dan harap digunakan dengan bijak.
- Koordinat yang diambil akan memiliki format xx,xxxxx dan tidak kompatibel jika dientri di Matchapro, maka perlu dilakukan replace separator pada koordinat dari koma (,) ke titik (.)
