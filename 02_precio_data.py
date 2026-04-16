import base64
import json
import random
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup

from listing_id import (
    canonical_listing_url,
    is_listing_id_hex,
    listing_id_from_cell,
    listing_id_from_url,
    tipo_from_urbania_listing_url,
)
from urbania_recencia import recencia_dias_a_fecha_publicacion, texto_publicacion_a_recencia_dias


LINKS_CSV_PATH = Path("output/OFIM_01.csv")
EXPORT_CSV = True
OUTPUT_CSV_PATH = Path("output/OFIM_02.csv")
#
# Ampliar la base con más avisos desde OFIM_01.csv (salida de 01):
#   SCRAPE_URLS_FROM_CSV = None
#   INCREMENTAL_SKIP_SEEN = True   # evita duplicar lo ya guardado en precio_data_final
#   MAX_LINKS = 2000               # o None para todos los links aún no vistos
#   LINKS_SLICE_MODE = "random"    # o "first" / "last"
#   RANDOM_SEED = 42               # opcional, para muestra reproducible
#
# Solo reprocesar filas que ya tienes en un CSV (p. ej. clean), sin links.csv:
#   SCRAPE_URLS_FROM_CSV = Path("output/OFIM_03.csv")
#   IGNORE_INCREMENTAL_FOR_TABULAR_URL_CSV = True   # repite todas las URLs de ese CSV
#
# Solo URLs que ya están en este CSV (columnas url_canonical y/o link con http). None = usar solo LINKS_CSV_PATH.
SCRAPE_URLS_FROM_CSV: Optional[Path] = None
SCRAPE_URLS_CSV_SEP: str = "|"
# Con SCRAPE_URLS_FROM_CSV definido: si True, no aplica INCREMENTAL_SKIP_SEEN (repide todas las filas del CSV). Si False, solo avisos aún no vistos en OUTPUT.
IGNORE_INCREMENTAL_FOR_TABULAR_URL_CSV: bool = True
# OFIM_01.csv lo genera 01 con separador coma (default). Desde 02 el CSV de salida usa "|".
LINKS_CSV_SEP = ","
OUTPUT_CSV_SEP = "|"
# Cantidad de links a scrapear. None = todos los del CSV.
MAX_LINKS: Optional[int] = 2000
# Cómo elegir el subconjunto cuando MAX_LINKS no es None:
#   "first" = los primeros MAX_LINKS del CSV
#   "last"  = los últimos MAX_LINKS del CSV
#   "random" = MAX_LINKS URLs al azar
LINKS_SLICE_MODE: str = "random"
# Solo si LINKS_SLICE_MODE == "random". None = muestra distinta cada ejecución.
RANDOM_SEED: Optional[int] = None
# Extracción diaria: no reprocesar avisos ya guardados (columna `link` = id hex en el CSV de salida).
INCREMENTAL_SKIP_SEEN: bool = True
# Si True: reprocesa avisos ya guardados que siguen sin lat/lon. Para solo arreglar la columna `tipo` del CSV limpio, usa
# `python 03_clean_precio_data.py --refresh-tipo-clean` y deja esto en False.
BACKFILL_MISSING_COORDS: bool = False
# CSV para decidir qué avisos necesitan coords (solo si BACKFILL_MISSING_COORDS). None = OUTPUT_CSV_PATH.
BACKFILL_COORDS_REFERENCE_CSV: Optional[Path] = None
# Si True: solo cola de backfill de coords (sin avisos nuevos desde links). Tiene efecto solo con BACKFILL_MISSING_COORDS.
BACKFILL_COORDS_ONLY: bool = False
# Separador del CSV de referencia de coords.
BACKFILL_COORDS_CSV_SEP: str = "|"
# Si True y ya existe OUTPUT_CSV_PATH, se concatenan filas nuevas y se deduplica por `link` (queda la última).
APPEND_TO_OUTPUT_CSV: bool = True
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "es-PE,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://urbania.pe/",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

# Pool reutilizable: urllib3 no pasa por el decodificador UTF-8 de requests al leer Location.
_HTTP = urllib3.PoolManager()

def _decode_response_body(data: bytes, content_type: str | None) -> str:
    charset = "utf-8"
    if content_type:
        match = re.search(r"charset=([^;\s]+)", content_type, re.I)
        if match:
            charset = match.group(1).strip().strip('"').strip("'")
    try:
        return data.decode(charset)
    except (LookupError, UnicodeDecodeError):
        return data.decode("utf-8", errors="replace")


class _SafeHttpResponse:
    """Suficiente para BeautifulSoup: .text, .status_code, .raise_for_status()."""

    __slots__ = ("status_code", "url", "_body", "_content_type")

    def __init__(self, status_code: int, url: str, body: bytes, content_type: str | None) -> None:
        self.status_code = status_code
        self.url = url
        self._body = body
        self._content_type = content_type

    @property
    def text(self) -> str:
        return _decode_response_body(self._body, self._content_type)

    def raise_for_status(self) -> None:
        if 400 <= self.status_code < 600:
            raise requests.HTTPError(f"{self.status_code} Client Error for url: {self.url}")


def get_response_safe(url: str, max_redirects: int = 15) -> _SafeHttpResponse:
    """
    En algunas versiones de requests/urllib3, aun con allow_redirects=False,
    el envío sigue intentando decodificar Location como UTF-8 y revienta.

    Usamos urllib3 con redirect=False y seguimos Location a mano (latin-1 / urljoin).
    """
    current = url
    for _ in range(max_redirects):
        r = _HTTP.request(
            "GET",
            current,
            headers=HEADERS,
            redirect=False,
            timeout=urllib3.Timeout(connect=10.0, read=25.0),
        )
        status = r.status
        ct = r.headers.get("Content-Type")

        if status in (301, 302, 303, 307, 308):
            location = r.headers.get("Location")
            if not location:
                return _SafeHttpResponse(status, current, r.data or b"", ct)
            if isinstance(location, bytes):
                location = location.decode("latin-1", errors="replace")
            current = urljoin(current, location.strip())
            continue

        return _SafeHttpResponse(status, current, r.data or b"", ct)

    raise requests.TooManyRedirects(
        f"Se superaron {max_redirects} redirecciones empezando en {url!r}"
    )


def _slice_balanced_json_object(html: str, start: int) -> str | None:
    """
    Desde el primer '{' de window.__PRELOADED_STATE__, toma el objeto JSON completo.
    Ignora llaves dentro de strings JSON (comillas dobles).
    """
    if start >= len(html) or html[start] != "{":
        return None
    depth = 0
    i = start
    in_str = False
    esc = False
    while i < len(html):
        ch = html[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            i += 1
            continue
        if ch == '"':
            in_str = True
            i += 1
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return html[start : i + 1]
        i += 1
    return None


# Patrones típicos Urbania / Navent en el HTML (por si json.loads falla en parte del documento).
_GEO_LAT_LON_SAME_OBJ = re.compile(
    r'"latitude"\s*:\s*(-?\d+\.?\d*)\s*,\s*"longitude"\s*:\s*(-?\d+\.?\d*)',
    re.I,
)
_GEO_LON_LAT_SAME_OBJ = re.compile(
    r'"longitude"\s*:\s*(-?\d+\.?\d*)\s*,\s*"latitude"\s*:\s*(-?\d+\.?\d*)',
    re.I,
)


def _coords_sobre_peru(lat: float, lon: float) -> bool:
    """Filtro laxo para descartar basura al usar regex sobre HTML enorme."""
    return -20.5 < lat < -0.4 and -81.5 < lon < -68.0


def _pair_from_strings(lat_s: str, lon_s: str) -> tuple[str, str] | None:
    try:
        lat, lon = float(lat_s), float(lon_s)
    except ValueError:
        return None
    if not _coords_sobre_peru(lat, lon):
        return None
    return (str(lat), str(lon))


def _normalize_geo_pair(lat: object, lon: object) -> tuple[str, str] | None:
    if lat is None or lon is None:
        return None
    try:
        la, lo = float(lat), float(lon)
    except (TypeError, ValueError):
        return None
    if not _coords_sobre_peru(la, lo):
        return None
    return (str(la), str(lo))


def _geo_from_posting_location_dict(pl: object) -> tuple[object, object] | None:
    if not isinstance(pl, dict):
        return None
    pg = pl.get("postingGeolocation") or {}
    if isinstance(pg, dict):
        geo = pg.get("geolocation") or {}
        if isinstance(geo, dict):
            lat, lon = geo.get("latitude"), geo.get("longitude")
            if lat is not None and lon is not None:
                return lat, lon
        lat, lon = pg.get("latitude"), pg.get("longitude")
        if lat is not None and lon is not None:
            return lat, lon
    lat, lon = pl.get("latitude"), pl.get("longitude")
    if lat is not None and lon is not None:
        return lat, lon
    coord = pl.get("coordinates") or {}
    if isinstance(coord, dict):
        lat = coord.get("latitude") or coord.get("lat")
        lon = coord.get("longitude") or coord.get("lng") or coord.get("lon")
        if lat is not None and lon is not None:
            return lat, lon
    return None


def _geo_from_posting_like(obj: object) -> tuple[object, object] | None:
    """Objeto estilo aviso con postingLocation (listado o detalle)."""
    if not isinstance(obj, dict):
        return None
    pl = obj.get("postingLocation")
    if pl is not None:
        g = _geo_from_posting_location_dict(pl)
        if g is not None:
            return g
    # A veces el detalle trae postingLocation plano en la raíz del sub-árbol
    if "postingGeolocation" in obj or "coordinates" in obj:
        return _geo_from_posting_location_dict(obj)
    return None


def _geo_from_geolocation_shaped(d: object) -> tuple[object, object] | None:
    if not isinstance(d, dict):
        return None
    lat, lon = d.get("latitude"), d.get("longitude")
    if lat is None or lon is None:
        return None
    return lat, lon


def _find_geo_in_preloaded_state(obj: object) -> tuple[object, object] | None:
    """Recorre el estado (listado listPostings o detalle) buscando coordenadas del aviso."""
    g = _geo_from_posting_like(obj)
    if g is not None and g[0] is not None and g[1] is not None:
        return g
    if isinstance(obj, dict):
        for key in ("geolocation", "location", "postingGeolocation"):
            if key in obj:
                gg = _geo_from_geolocation_shaped(obj[key])
                if gg is not None:
                    return gg
        for v in obj.values():
            found = _find_geo_in_preloaded_state(v)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_geo_in_preloaded_state(item)
            if found is not None:
                return found
    return None


def _sanitize_preloaded_json_blob(blob: str) -> str:
    """Quita tokens JS que rompen json.loads (p.ej. undefined)."""
    return re.sub(r"\bundefined\b", "null", blob)


def _loads_preloaded_state(blob: str) -> dict[str, object] | None:
    for candidate in (blob, _sanitize_preloaded_json_blob(blob)):
        try:
            out = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(out, dict):
            return out
    return None


def _preloaded_assignment_brace_index(html: str, assign_pos: int) -> int | None:
    """Tras '=' de __PRELOADED_STATE__, localiza el inicio del objeto JSON."""
    chunk = html[assign_pos : assign_pos + 80]
    json_parse = re.search(r"JSON\.parse\s*\(\s*(['\"])", chunk, re.I)
    if json_parse:
        return None  # otro formato; no soportado aquí
    brace = html.find("{", assign_pos)
    return brace if brace != -1 else None


def _iter_preloaded_state_blobs(html: str):
    """Localiza posibles asignaciones de __PRELOADED_STATE__ (variantes de window.)."""
    for m in re.finditer(r"__PRELOADED_STATE__\s*=", html):
        brace = _preloaded_assignment_brace_index(html, m.end())
        if brace is None:
            continue
        blob = _slice_balanced_json_object(html, brace)
        if blob:
            yield blob


def extract_lat_lon_from_preloaded_state(html: str) -> tuple[str, str]:
    """
    Parsea __PRELOADED_STATE__ incrustado en el HTML y devuelve (lat, lon).
    Vacío si no hay estado, falla el JSON o el aviso no tiene geo (p.ej. mapa genérico).
    """
    for blob in _iter_preloaded_state_blobs(html):
        state = _loads_preloaded_state(blob)
        if not state:
            continue
        found = _find_geo_in_preloaded_state(state)
        if found is None:
            continue
        lat, lon = found
        pair = _normalize_geo_pair(lat, lon)
        if pair:
            return pair
    return "", ""


def _decode_b64_coord(b64: str) -> str | None:
    """Decodifica un string base64 que contiene un número decimal (lat o lon)."""
    try:
        decoded = base64.b64decode(b64).decode("utf-8").strip()
        float(decoded)  # valida que sea número
        return decoded.rstrip("0").rstrip(".")
    except Exception:
        return None


_MAP_LAT_OF = re.compile(
    r"""(?:const|let|var)\s+mapLatOf\s*=\s*["']([A-Za-z0-9+/=]+)["']""",
)
_MAP_LNG_OF = re.compile(
    r"""(?:const|let|var)\s+mapLngOf\s*=\s*["']([A-Za-z0-9+/=]+)["']""",
)


def extract_lat_lon_from_b64_vars(html: str) -> tuple[str, str]:
    """
    Urbania inyecta las coordenadas en variables JS codificadas en base64:
      const mapLatOf = "LTEyLjExMzM3MTYwMDAwMDAwMA==";
      const mapLngOf = "LTc2Ljk2ODYwODAwMDAwMDAwMA==";
    """
    m_lat = _MAP_LAT_OF.search(html)
    m_lng = _MAP_LNG_OF.search(html)
    if not m_lat or not m_lng:
        return "", ""
    lat = _decode_b64_coord(m_lat.group(1))
    lon = _decode_b64_coord(m_lng.group(1))
    if lat is None or lon is None:
        return "", ""
    pair = _pair_from_strings(lat, lon)
    return pair if pair else ("", "")


def extract_lat_lon_from_html(html: str) -> tuple[str, str]:
    """Intenta base64 vars (ficha), luego PRELOADED_STATE (listado), luego regex."""
    lat, lon = extract_lat_lon_from_b64_vars(html)
    if lat and lon:
        return lat, lon
    lat, lon = extract_lat_lon_from_preloaded_state(html)
    if lat and lon:
        return lat, lon
    return extract_lat_lon_regex_fallback(html)


def extract_lat_lon_regex_fallback(html: str) -> tuple[str, str]:
    """Último recurso: primer par lat/lon válido dentro de literales en scripts."""
    for rx in (_GEO_LAT_LON_SAME_OBJ, _GEO_LON_LAT_SAME_OBJ):
        for m in rx.finditer(html):
            if rx is _GEO_LAT_LON_SAME_OBJ:
                pair = _pair_from_strings(m.group(1), m.group(2))
            else:
                pair = _pair_from_strings(m.group(2), m.group(1))
            if pair:
                return pair
    return "", ""


def lat_lon_from_static_map_src(src: str) -> tuple[str, str]:
    """
    Mapas estáticos (p.ej. Google) suelen llevar center=lat,lng o markers=...|lat,lng.
    Ignora el placeholder no-location-map de Urbania.
    """
    if not src or "no-location-map" in src.lower():
        return "", ""
    try:
        qs = parse_qs(urlparse(src).query)
    except Exception:
        return "", ""
    for key in ("center", "markers", "path"):
        if key not in qs:
            continue
        raw = unquote(str(qs[key][0]))
        for sep in ("|", "%7C"):
            parts = raw.split(sep) if sep in raw else [raw]
            for part in parts:
                part = part.strip()
                if "," not in part:
                    continue
                # "color:red" style
                if ":" in part.split(",")[0]:
                    continue
                a, b = part.split(",", 1)
                a, b = a.strip(), b.strip()
                for lat_s, lon_s in ((a, b),):
                    pair = _pair_from_strings(lat_s, lon_s)
                    if pair:
                        return pair
    return "", ""


def _collapse_whitespace(text: str) -> str:
    """Quita saltos de línea, tabs y espacios repetidos (típico del HTML de icon-feature)."""
    return re.sub(r"\s+", " ", text).strip()


def extract_antiguedad_publicacion(html: str, soup: Optional[BeautifulSoup] = None) -> str:
    """
    Antigüedad del aviso en el portal (ej. 'Publicado hace 5 días').

    Urbania lo inyecta en <script> como `const antiquity = '...'`; el nodo con clase
    userViews-module__post-antiquity-views___* suele aparecer solo tras hidratar React,
    por eso el HTML estático de requests casi nunca tiene ese class.
    """
    # No siempre termina en `;` antes del salto de línea.
    m = re.search(
        r"const\s+antiquity\s*=\s*(['\"])(.*?)\1",
        html,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if m:
        inner = (m.group(2) or "").strip()
        if inner:
            return _collapse_whitespace(inner)

    m2 = re.search(
        r"const\s+antiquity\s*=\s*(false|null|true)\b",
        html,
        flags=re.IGNORECASE,
    )
    if m2:
        v = m2.group(1).lower()
        return "" if v in ("false", "null") else v

    if soup is not None:
        node = soup.select_one('[class*="post-antiquity-views"], [class*="post-antiquity"]')
        if node:
            t = _collapse_whitespace(node.get_text(separator=" ", strip=True))
            if t:
                return t

    return ""


def _amount_to_str(value: Optional[float]) -> str:
    if value is None:
        return ""
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return str(value)


def _parse_amount_token(token: str) -> Optional[float]:
    cleaned = token.replace(",", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def desagregar_price_tokens(price_tokens: str) -> Dict[str, str]:
    """
    S/..._venta -> venta_pen; USD..._venta -> venta_usd; S/..._venta_mant -> mant (siempre soles).
    Incluye alquiler_* y mant_alquiler por si el aviso lo trae.
    """
    out: Dict[str, str] = {
        "venta_pen": "",
        "venta_usd": "",
        "mant": "",
        "alquiler_pen": "",
        "alquiler_usd": "",
        "mant_alquiler": "",
    }
    text = _collapse_whitespace(price_tokens)
    if not text:
        return out

    for part in [p.strip() for p in text.split("|") if p.strip()]:
        match = re.match(
            r"^(S/|USD)\s*([\d,]+(?:\.\d+)?)_(venta|alquiler)(?:_(mant))?$",
            part,
            flags=re.IGNORECASE,
        )
        if not match:
            continue
        cur = match.group(1).upper()
        amt = _parse_amount_token(match.group(2))
        mode = match.group(3).lower()
        is_mant = match.group(4) is not None
        val = _amount_to_str(amt)
        soles = cur == "S/"

        if mode == "venta":
            if is_mant and soles:
                out["mant"] = val
            elif not is_mant and soles:
                out["venta_pen"] = val
            elif not is_mant and not soles:
                out["venta_usd"] = val
        else:
            if is_mant and soles:
                out["mant_alquiler"] = val
            elif not is_mant and soles:
                out["alquiler_pen"] = val
            elif not is_mant and not soles:
                out["alquiler_usd"] = val

    return out


def _icon_extract_number(text: str, patterns: Tuple[str, ...]) -> Optional[float]:
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            return _parse_amount_token(m.group(1))
    return None


def desagregar_icon_features(icon_features: str) -> Dict[str, str]:
    """m_tot, m_cub, baños, dorm, estac, antiguedad (años o 'a estrenar')."""
    text = _collapse_whitespace(icon_features)
    out: Dict[str, str] = {
        "m_tot": "",
        "m_cub": "",
        "baños": "",
        "dorm": "",
        "estac": "",
        "antiguedad": "",
    }
    if not text:
        return out

    v = _icon_extract_number(
        text,
        (
            r"(\d+(?:[.,]\d+)?)\s*m²\s*tot\.?",
            r"m²\s*tot\.?\s*(?:\||:)?\s*(\d+(?:[.,]\d+)?)",
        ),
    )
    out["m_tot"] = _amount_to_str(v)

    v = _icon_extract_number(
        text,
        (
            r"(\d+(?:[.,]\d+)?)\s*m²\s*(?:cub\.?|tech(?:ada)?)",
            r"m²\s*(?:cub\.?|tech(?:ada)?)\s*(?:\||:)?\s*(\d+(?:[.,]\d+)?)",
        ),
    )
    out["m_cub"] = _amount_to_str(v)

    v = _icon_extract_number(
        text,
        (
            r"(\d+(?:[.,]\d+)?)\s*bañ(?:o|os)",
            r"bañ(?:o|os)\s*(?:\||:)?\s*(\d+(?:[.,]\d+)?)",
        ),
    )
    out["baños"] = _amount_to_str(v)

    v = _icon_extract_number(
        text,
        (
            r"(\d+(?:[.,]\d+)?)\s*dorm\.?",
            r"dorm\.?\s*(?:\||:)?\s*(\d+(?:[.,]\d+)?)",
        ),
    )
    out["dorm"] = _amount_to_str(v)

    v = _icon_extract_number(
        text,
        (
            r"(\d+(?:[.,]\d+)?)\s*(?:estac\.?|estacionamientos?)",
            r"(?:estac\.?|estacionamientos?)\s*(?:\||:)?\s*(\d+(?:[.,]\d+)?)",
        ),
    )
    out["estac"] = _amount_to_str(v)

    if re.search(r"\ba\s+estrenar\b", text, flags=re.IGNORECASE):
        out["antiguedad"] = "a estrenar"
    else:
        v = _icon_extract_number(
            text,
            (
                r"(\d+(?:[.,]\d+)?)\s*años",
                r"años\s*(?:\||:)?\s*(\d+(?:[.,]\d+)?)",
            ),
        )
        out["antiguedad"] = _amount_to_str(v)

    return out


def _empty_desagregado_row() -> Dict[str, str]:
    return {**desagregar_price_tokens(""), **desagregar_icon_features("")}


def split_price_tokens(block_text: str) -> List[str]:
    tokens = (
        block_text.replace("S/ ", "S/")
        .replace("USD ", "USD")
        .replace("· ", "")
        .replace(" ", "\n")
        .split("\n")
    )
    tokens = [t.strip() for t in tokens if t.strip()]
    return tokens


def tagged_price_tokens(soup: BeautifulSoup) -> List[str]:
    result: List[str] = []
    for node in soup.select(".price-item-container"):
        tokens = split_price_tokens(node.get_text(" ", strip=True))
        mode = None
        if "alquiler" in tokens:
            mode = "alquiler"
            tokens.remove("alquiler")
        elif "venta" in tokens:
            mode = "venta"
            tokens.remove("venta")

        if mode:
            mant_index = tokens.index("Mantenimiento") if "Mantenimiento" in tokens else None
            enriched: List[str] = []
            for idx, token in enumerate(tokens):
                if token == "Mantenimiento":
                    continue
                suffix = f"_{mode}"
                if mant_index is not None and idx == mant_index + 1:
                    suffix = f"_{mode}_mant"
                enriched.append(f"{token}{suffix}")
            result.extend(enriched)
        else:
            result.extend(tokens)
    return result


def _load_seen_listing_ids(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    try:
        prev = pd.read_csv(path, sep=OUTPUT_CSV_SEP, encoding="utf-8-sig", usecols=["link"])
    except (ValueError, pd.errors.EmptyDataError, KeyError):
        return set()

    out: Set[str] = set()
    for v in prev["link"].dropna():
        lid = listing_id_from_cell(v)
        if lid:
            out.add(lid)
    return out


def _coord_cell_filled(value: object) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    s = str(value).strip()
    return bool(s) and s.lower() not in ("nan", "none")


def _load_listing_ids_missing_coords(path: Path, csv_sep: str | None = None) -> Set[str]:
    """Ids (hex) de filas en la salida donde falta lat o lon."""
    sep = OUTPUT_CSV_SEP if csv_sep is None else csv_sep
    if not path.exists():
        return set()
    try:
        prev = pd.read_csv(path, sep=sep, encoding="utf-8-sig")
    except (ValueError, pd.errors.EmptyDataError):
        return set()
    if "link" not in prev.columns:
        return set()
    lat_col = "lat" if "lat" in prev.columns else None
    lon_col = "lon" if "lon" in prev.columns else None
    if lat_col is None and lon_col is None:
        return {x for x in prev["link"].dropna().map(listing_id_from_cell) if x}

    out: Set[str] = set()
    for _, row in prev.iterrows():
        lat_ok = _coord_cell_filled(row[lat_col]) if lat_col else False
        lon_ok = _coord_cell_filled(row[lon_col]) if lon_col else False
        if lat_ok and lon_ok:
            continue
        lid = listing_id_from_cell(row.get("link"))
        if lid:
            out.add(lid)
    return out


def _collect_backfill_urls_from_output(path: Path, csv_sep: str | None = None) -> List[str]:
    """
    URLs canónicas del CSV de salida con lat o lon vacíos (para avisos que ya no están en links.csv).
    """
    sep = OUTPUT_CSV_SEP if csv_sep is None else csv_sep
    if not path.exists():
        return []
    try:
        prev = pd.read_csv(path, sep=sep, encoding="utf-8-sig")
    except (ValueError, pd.errors.EmptyDataError):
        return []
    if "url_canonical" not in prev.columns:
        return []
    urls: List[str] = []
    seen_url: Set[str] = set()
    lat_col = "lat" if "lat" in prev.columns else None
    lon_col = "lon" if "lon" in prev.columns else None
    for _, row in prev.iterrows():
        lat_ok = _coord_cell_filled(row[lat_col]) if lat_col else False
        lon_ok = _coord_cell_filled(row[lon_col]) if lon_col else False
        if lat_ok and lon_ok:
            continue
        raw = row.get("url_canonical")
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            continue
        u = str(raw).strip()
        if not u.startswith("http"):
            continue
        canon = canonical_listing_url(u)
        if not canon or canon in seen_url:
            continue
        seen_url.add(canon)
        urls.append(canon)
    return urls


def _migrate_recencia_a_fecha_en_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Si el CSV viejo trae `recencia` (días) y no `fecha_publicacion`, migra y quita `recencia`."""
    out = df.copy()
    if "fecha_publicacion" not in out.columns:
        out["fecha_publicacion"] = ""
    if "recencia" not in out.columns:
        return out
    mask = out["fecha_publicacion"].isna() | (out["fecha_publicacion"].astype(str).str.strip().isin(["", "nan"]))
    sub = out.loc[mask, "recencia"]
    out.loc[mask, "fecha_publicacion"] = sub.map(recencia_dias_a_fecha_publicacion)
    return out.drop(columns=["recencia"], errors="ignore")


def parse_listing(url: str) -> Dict[str, object]:
    canon = canonical_listing_url(url)
    lid = listing_id_from_url(url)
    row: Dict[str, object] = {
        **_empty_desagregado_row(),
        "tipo": "",
        "distrito": "",
        "detail_li_2": "",
        "fecha_publicacion": "",
        "extra_info": "",
        "error": "",
        "url_canonical": canon,
        "link": lid,
        "static_map_src": "",
        "lat": "",
        "lon": "",
    }
    try:
        response = get_response_safe(url)
        response.raise_for_status()
    except Exception as exc:
        row["error"] = f"request_error: {exc}"
        return row

    html = response.text
    soup = BeautifulSoup(html, "html.parser")

    try:
        lat, lon = extract_lat_lon_from_html(html)
        row["lat"], row["lon"] = lat, lon
    except Exception:
        pass

    _rec = texto_publicacion_a_recencia_dias(extract_antiguedad_publicacion(html, soup))
    row["fecha_publicacion"] = recencia_dias_a_fecha_publicacion(_rec)

    try:
        raw_prices = " | ".join(tagged_price_tokens(soup))
        row.update(desagregar_price_tokens(raw_prices))
    except Exception:
        pass

    try:
        tipo_url = tipo_from_urbania_listing_url(canon)
        if tipo_url:
            row["tipo"] = tipo_url
        else:
            title_type = soup.select_one(".title-type-sup")
            title_text = title_type.get_text(" ", strip=True) if title_type else ""
            row["tipo"] = "Departamento" if "Departamento" in title_text else "Otro"
    except Exception:
        pass

    try:
        bread_items = [x.get_text(" ", strip=True) for x in soup.select(".breadcrumb .bread-item")]
        if len(bread_items) > 5:
            row["distrito"] = bread_items[5].replace("_distrito", "").strip()
    except Exception:
        pass

    try:
        icon_parts: List[str] = []
        for node in soup.select(".icon-feature"):
            raw = node.get_text(separator=" ", strip=True)
            if raw:
                icon_parts.append(_collapse_whitespace(raw))
        row.update(desagregar_icon_features(" | ".join(icon_parts)))
    except Exception:
        pass

    try:
        li_candidates = [x.get_text(" ", strip=True) for x in soup.select("section ul li")]
        if len(li_candidates) > 1:
            row["detail_li_2"] = li_candidates[1]
    except Exception:
        pass

    try:
        static_map = soup.select_one("#static-map")
        if static_map and static_map.get("src"):
            row["static_map_src"] = static_map.get("src")
    except Exception:
        pass

    try:
        if (not row.get("lat") or not row.get("lon")) and row.get("static_map_src"):
            slat, slon = lat_lon_from_static_map_src(str(row["static_map_src"]))
            if slat and slon:
                row["lat"], row["lon"] = slat, slon
    except Exception:
        pass

    try:
        # Se aproxima al bloque textual que antes salía del XPath largo.
        article_text = soup.select_one("article")
        if article_text:
            compact_text = re.sub(r"\s+", " ", article_text.get_text(" ", strip=True))
            row["extra_info"] = compact_text[:1200]
    except Exception:
        pass

    row["tipo"] = row["tipo"].replace("_tipo", "").strip()

    return row


def _load_urls_from_tabular_export(path: Path, sep: str) -> List[str]:
    """URLs para scrapear: una por aviso (link hex se ignora), orden de aparición en el CSV."""
    df = pd.read_csv(path, sep=sep, encoding="utf-8-sig")
    by_lid: Dict[str, str] = {}
    order: List[str] = []

    def consider(val: object) -> None:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return
        s = str(val).strip()
        if not s.startswith("http"):
            return
        canon = canonical_listing_url(s)
        if not canon:
            return
        lid = listing_id_from_url(canon)
        if not lid or lid in by_lid:
            return
        by_lid[lid] = canon
        order.append(canon)

    if "url_canonical" in df.columns:
        for v in df["url_canonical"]:
            consider(v)
    if "link" in df.columns:
        for v in df["link"]:
            consider(v)
    return order


def main() -> None:
    tabular_path = SCRAPE_URLS_FROM_CSV
    use_tabular = tabular_path is not None and tabular_path.exists()
    if tabular_path is not None and not use_tabular:
        print(f"Aviso: SCRAPE_URLS_FROM_CSV={tabular_path!r} no existe; usando {LINKS_CSV_PATH.name}.")

    if use_tabular:
        href_all = _load_urls_from_tabular_export(tabular_path, SCRAPE_URLS_CSV_SEP)
        if not href_all:
            raise ValueError(f"No hay URLs http en {tabular_path} (columnas url_canonical / link).")
        print(f"Origen de URLs: {tabular_path.name} ({len(href_all)} avisos).")
    else:
        if not LINKS_CSV_PATH.exists():
            raise FileNotFoundError(f"No existe el archivo de links: {LINKS_CSV_PATH}")

        links_df = pd.read_csv(LINKS_CSV_PATH, sep=LINKS_CSV_SEP, encoding="utf-8-sig")
        if "href" not in links_df.columns:
            raise ValueError("El CSV de links debe tener una columna llamada 'href'.")

        href_all = (
            links_df["href"]
            .dropna()
            .astype(str)
            .map(str.strip)
            .loc[lambda s: s.str.startswith("http")]
            .drop_duplicates()
            .tolist()
        )

    use_ref = BACKFILL_COORDS_REFERENCE_CSV is not None and BACKFILL_COORDS_REFERENCE_CSV.exists()
    ref_path = BACKFILL_COORDS_REFERENCE_CSV if use_ref else OUTPUT_CSV_PATH
    ref_sep = BACKFILL_COORDS_CSV_SEP if use_ref else OUTPUT_CSV_SEP
    if BACKFILL_COORDS_REFERENCE_CSV is not None and not use_ref:
        print(
            f"Aviso: BACKFILL_COORDS_REFERENCE_CSV={BACKFILL_COORDS_REFERENCE_CSV!r} no existe; "
            f"usando {OUTPUT_CSV_PATH} para detectar coords faltantes."
        )

    missing_geo: Set[str] = set()
    if BACKFILL_MISSING_COORDS:
        missing_geo = _load_listing_ids_missing_coords(ref_path, ref_sep)
        if missing_geo:
            print(
                f"Backfill coordenadas ({ref_path.name}): {len(missing_geo)} avisos sin lat y/o lon "
                f"(detección desde {'referencia' if use_ref else 'OUTPUT_CSV_PATH'})."
            )

    href_1: List[str]

    if BACKFILL_COORDS_ONLY:
        if not missing_geo:
            print("BACKFILL_COORDS_ONLY: no hay avisos sin coords en el CSV de referencia; terminando.")
            return
        by_id_back: Dict[str, str] = {}
        for u in href_all:
            lid = listing_id_from_url(u)
            if lid in missing_geo:
                by_id_back.setdefault(lid, u)
        for u in _collect_backfill_urls_from_output(ref_path, ref_sep):
            lid = listing_id_from_url(u)
            if lid in missing_geo:
                by_id_back.setdefault(lid, u)
        href_1 = list(by_id_back.values())
        n_orphan = len(missing_geo) - len(by_id_back)
        if n_orphan:
            print(
                f"Aviso: {n_orphan} avisos sin coords no tienen URL en links.csv ni url_canonical en referencia; se omiten."
            )
        print(f"BACKFILL_COORDS_ONLY: {len(href_1)} URLs a pedir (sin avisos nuevos desde links).")

    else:
        href_1 = href_all

        apply_incremental = INCREMENTAL_SKIP_SEEN and not (
            use_tabular and IGNORE_INCREMENTAL_FOR_TABULAR_URL_CSV
        )
        if apply_incremental:
            seen = _load_seen_listing_ids(OUTPUT_CSV_PATH)
            if seen:
                before = len(href_1)

                def _keep_url_for_incremental(u: str) -> bool:
                    lid = listing_id_from_url(u)
                    if lid not in seen:
                        return True
                    return bool(BACKFILL_MISSING_COORDS and lid in missing_geo)

                href_1 = [u for u in href_1 if _keep_url_for_incremental(u)]
                n_back = sum(1 for u in href_1 if listing_id_from_url(u) in missing_geo)
                print(
                    f"Incremental: {before - len(href_1)} URLs ya vistas excluidas; "
                    f"quedan {len(href_1)} (incl. {n_back} reprocesos por coords vacías según {ref_path.name})."
                )
        elif use_tabular and INCREMENTAL_SKIP_SEEN and IGNORE_INCREMENTAL_FOR_TABULAR_URL_CSV:
            print("Incremental omitido: repasando todas las URLs del CSV tabular.")

        if BACKFILL_MISSING_COORDS:
            extra = _collect_backfill_urls_from_output(ref_path, ref_sep)
            by_id = {listing_id_from_url(u): u for u in href_1}
            n_added = 0
            for u in extra:
                lid = listing_id_from_url(u)
                if not lid or lid in by_id:
                    continue
                by_id[lid] = u
                n_added += 1
            if n_added:
                print(f"Backfill: +{n_added} URLs tomadas del CSV de referencia (no estaban en links.csv).")
            href_1 = list(by_id.values())

    if not href_1:
        print("No hay URLs para procesar.")
        return

    total_disponibles = len(href_1)
    mode = (LINKS_SLICE_MODE or "first").strip().lower()
    if MAX_LINKS is not None:
        n = min(max(0, MAX_LINKS), total_disponibles)
        if n == 0:
            href_1 = []
        elif mode == "random":
            if RANDOM_SEED is not None:
                random.seed(RANDOM_SEED)
            href_1 = random.sample(href_1, k=n)
            print(
                f"Muestra aleatoria: {n} links de {total_disponibles} "
                f"(MAX_LINKS={MAX_LINKS}, RANDOM_SEED={RANDOM_SEED!r})."
            )
        elif mode == "last":
            href_1 = href_1[-n:]
            print(f"Últimos {n} links de {total_disponibles} (MAX_LINKS={MAX_LINKS}, LINKS_SLICE_MODE=last).")
        else:
            if mode != "first":
                print(f"LINKS_SLICE_MODE={LINKS_SLICE_MODE!r} no reconocido; usando 'first'.")
            href_1 = href_1[:n]
            print(f"Primeros {n} links de {total_disponibles} (MAX_LINKS={MAX_LINKS}, LINKS_SLICE_MODE=first).")

    total = len(href_1)
    datos: List[Dict[str, str]] = []

    for idx, url in enumerate(href_1):
        try:
            row = parse_listing(url)
        except Exception as exc:
            # Cualquier error inesperado no detiene el script; se registra en el CSV.
            canon = canonical_listing_url(url)
            row = {
                **_empty_desagregado_row(),
                "tipo": "",
                "distrito": "",
                "detail_li_2": "",
                "fecha_publicacion": "",
                "extra_info": "",
                "error": f"unexpected_error: {exc}",
                "url_canonical": canon,
                "link": listing_id_from_url(url),
                "static_map_src": "",
                "lat": "",
                "lon": "",
            }
        datos.append(row)

        if idx < 5:
            print(f"\n=== Corrida {idx + 1}/5 (link {idx + 1}/{total}) ===")
            print(list(row.values()))
        else:
            print(f"Links procesados: {idx + 1}/{total}", end="\r", flush=True)

    if total > 5:
        print()

    new_df = pd.DataFrame(datos)

    if APPEND_TO_OUTPUT_CSV and OUTPUT_CSV_PATH.exists():
        try:
            old_df = pd.read_csv(OUTPUT_CSV_PATH, sep=OUTPUT_CSV_SEP, encoding="utf-8-sig")
        except pd.errors.EmptyDataError:
            old_df = pd.DataFrame()
        if len(old_df.columns) and len(new_df.columns):
            if "link" in old_df.columns:
                old_df = old_df.copy()
                raw = old_df["link"]
                if "url_canonical" not in old_df.columns:
                    old_df["url_canonical"] = raw.map(
                        lambda x: (
                            canonical_listing_url(x)
                            if pd.notna(x)
                            and str(x).strip()
                            and not is_listing_id_hex(str(x).strip())
                            else ""
                        )
                    )
                old_df["link"] = raw.map(
                    lambda x: listing_id_from_cell(x)
                    if pd.notna(x) and str(x).strip() and str(x).strip().lower() != "nan"
                    else ""
                )
            final_df = pd.concat([old_df, new_df], ignore_index=True, sort=False)
            final_df = final_df.drop_duplicates(subset=["link"], keep="last")
            final_df = _migrate_recencia_a_fecha_en_dataframe(final_df)
        else:
            final_df = _migrate_recencia_a_fecha_en_dataframe(new_df)
    else:
        final_df = _migrate_recencia_a_fecha_en_dataframe(new_df)

    _order = [
        "venta_pen",
        "venta_usd",
        "mant",
        "alquiler_pen",
        "alquiler_usd",
        "mant_alquiler",
        "tipo",
        "distrito",
        "m_tot",
        "m_cub",
        "baños",
        "dorm",
        "estac",
        "antiguedad",
        "fecha_publicacion",
        "detail_li_2",
        "extra_info",
        "error",
        "url_canonical",
        "link",
        "static_map_src",
        "lat",
        "lon",
    ]
    _rest = [c for c in final_df.columns if c not in _order]
    final_df = final_df[[c for c in _order if c in final_df.columns] + _rest]

    if EXPORT_CSV:
        OUTPUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_csv(OUTPUT_CSV_PATH, index=False, encoding="utf-8-sig", sep=OUTPUT_CSV_SEP)
        print(f"\nCSV exportado en: {OUTPUT_CSV_PATH}")


if __name__ == "__main__":
    main() 