import re
import time
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import parse_qs, urlparse, urljoin

import pandas as pd
import urllib3
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://urbania.pe/buscar/venta-de-propiedades-en-lima?sort=more_recent&page={page}"
EXPORT_CSV = True
OUTPUT_CSV_PATH = Path("output/OFIM_01.csv")
# Pausa ligera entre páginas para reducir 403 / throttling en listados grandes.
DELAY_SECONDS = 0.35

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-PE,es;q=0.9",
}

_HTTP = urllib3.PoolManager()


def _decode_body(data: bytes, content_type: str | None) -> str:
    charset = "utf-8"
    if content_type:
        m = re.search(r"charset=([^;\s]+)", content_type, re.I)
        if m:
            charset = m.group(1).strip().strip('"').strip("'")
    try:
        return data.decode(charset)
    except (LookupError, UnicodeDecodeError):
        return data.decode("utf-8", errors="replace")


def fetch_html(url: str, max_redirects: int = 15) -> Tuple[str, str, int]:
    """
    Devuelve (html, url_final, status).
    urllib3 + redirect=False evita problemas con Location no UTF-8 y replica mejor la URL final.
    """
    current = url
    for _ in range(max_redirects):
        r = _HTTP.request(
            "GET",
            current,
            headers=HEADERS,
            redirect=False,
            timeout=urllib3.Timeout(connect=10.0, read=30.0),
        )
        status = r.status
        ct = r.headers.get("Content-Type")
        if status in (301, 302, 303, 307, 308):
            loc = r.headers.get("Location")
            if not loc:
                return _decode_body(r.data or b"", ct), current, status
            if isinstance(loc, bytes):
                loc = loc.decode("latin-1", errors="replace")
            current = urljoin(current, loc.strip())
            continue
        return _decode_body(r.data or b"", ct), current, status
    raise requests.TooManyRedirects(f"Demasiadas redirecciones desde {url!r}")


def page_number_from_url(url: str) -> Optional[int]:
    """
    Número de página solo si viene explícito en ?page=N.
    Si falta el parámetro, devolver None: NO asumir 1 (evita cortar cuando Urbania omite page en la URL final).
    """
    qs = parse_qs(urlparse(url).query)
    vals = qs.get("page")
    if not vals:
        return None
    try:
        return int(vals[0])
    except ValueError:
        return None


href_1: list[str] = []
cont = 1
prev_listing_paths_fingerprint: Optional[Tuple[str, ...]] = None

while True:
    print(cont)
    url = BASE_URL.format(page=cont)
    try:
        html, final_url, status = fetch_html(url)
    except (requests.RequestException, OSError, urllib3.exceptions.HTTPError) as exc:
        print(f"Peticion fallida en pagina {cont}: {exc}")
        break

    if status == 403:
        print(f"403 en pagina {cont}; reintentando una vez tras pausa...")
        time.sleep(2.0)
        try:
            html, final_url, status = fetch_html(url)
        except (requests.RequestException, OSError, urllib3.exceptions.HTTPError) as exc:
            print(f"Reintento fallido: {exc}")
            break

    if status == 403:
        print("403 persistente; fin del scrape por bloqueo del servidor.")
        break

    if status >= 400:
        print(f"HTTP {status} en pagina {cont}; se detiene.")
        break

    # Solo si la URL final incluye ?page= confiamos en la canonicalización (no cortar por None).
    effective_page = page_number_from_url(final_url)
    if effective_page is not None and cont > effective_page:
        break

    soup = BeautifulSoup(html, "html.parser")
    postings = soup.select('[data-to-posting]')
    n_html = len(postings)

    if n_html == 0:
        break

    listing_paths = tuple(
        sorted(
            p
            for item in postings
            for p in [item.get("data-to-posting")]
            if p and isinstance(p, str) and p.startswith("/")
        )
    )

    if prev_listing_paths_fingerprint is not None and listing_paths == prev_listing_paths_fingerprint:
        break
    prev_listing_paths_fingerprint = listing_paths

    for item in postings:
        path = item.get("data-to-posting")
        if path and path.startswith("/"):
            href_1.append(f"https://urbania.pe{path}")

    cont += 1
    if DELAY_SECONDS > 0:
        time.sleep(DELAY_SECONDS)

links_df = pd.DataFrame({"href": list(dict.fromkeys(href_1))})

print("\nPrimeras 5 filas de links_df:")
print(links_df.head(5))
print(f"\nTotal links unicos: {len(links_df)}")

if EXPORT_CSV:
    OUTPUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    links_df.to_csv(OUTPUT_CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"\nCSV exportado en: {OUTPUT_CSV_PATH}")
