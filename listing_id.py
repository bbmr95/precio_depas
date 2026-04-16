"""URL canónica de aviso + id fijo (hash) para deduplicar y columnas compactas."""

from __future__ import annotations

import hashlib
import re
from urllib.parse import urlsplit, urlunsplit

# Longitud fija del id en hex (16 = 8 bytes de SHA-256).
LISTING_ID_HEX_DIGITS = 16
_LISTING_ID_RE = re.compile(rf"^[0-9a-f]{{{LISTING_ID_HEX_DIGITS}}}$", re.I)

# Código al inicio del slug: .../clasificado/{codigo}-venta-de-...
_URBANIA_CLASIFICADO_CODE = re.compile(r"/clasificado/([a-z0-9]+)-", re.I)

# Prefijos de Urbania / Navent en la URL → etiqueta legible para columna `tipo`.
URBANIA_LISTING_TYPE_BY_CODE: dict[str, str] = {
    "alclcain": "Alquiler de casa",
    "alcltein": "Alquiler de terreno",
    "veclapin": "Venta de departamento",
    "veclappa": "Venta de departamento con dueño directo",
    "veclcain": "Venta de casa",
    "veclcapa": "Venta de casa con dueño directo",
    "veclcdin": "Venta de casa de campo",
    "veclcdpa": "Venta de casa de campo con dueño directo",
    "veclcpin": "Venta de casa de playa",
    "veclhtin": "Venta de hotel",
    "vecllcin": "Venta de local comercial",
    "vecllcpa": "Venta de local comercial con dueño directo",
    "veclniin": "Venta de local industrial",
    "veclnipa": "Venta de local industrial con dueño directo",
    "veclocin": "Venta de oficina",
    "veclotin": "Venta de otros",
    "vecltein": "Venta de terreno",
    "vecltepa": "Venta de terreno con dueño directo",
}


def urbania_slug_code_from_url(url: object) -> str:
    """Primer token del slug tras /clasificado/ (ej. veclapin en veclapin-venta-de-departamento-...)."""
    u = canonical_listing_url(url)
    if not u:
        return ""
    m = _URBANIA_CLASIFICADO_CODE.search(u)
    return m.group(1).lower() if m else ""


def tipo_from_urbania_listing_url(url: object) -> str:
    """Tipo de inmueble según prefijo del enlace; vacío si no es clasificado o código desconocido."""
    code = urbania_slug_code_from_url(url)
    return URBANIA_LISTING_TYPE_BY_CODE.get(code, "") if code else ""


def canonical_listing_url(url: object) -> str:
    """Misma lógica que acortar URL: scheme + host + path, sin query ni fragmento."""
    if url is None:
        return ""
    u = str(url).strip()
    if not u or u.lower() == "nan":
        return ""
    try:
        p = urlsplit(u)
        return urlunsplit((p.scheme or "https", p.netloc, p.path, "", ""))
    except Exception:
        return u


def listing_id_from_url(url: object) -> str:
    """Id hexadecimal de largo fijo derivado de la URL canónica."""
    c = canonical_listing_url(url)
    if not c:
        return ""
    digest = hashlib.sha256(c.encode("utf-8")).hexdigest()
    return digest[:LISTING_ID_HEX_DIGITS]


def is_listing_id_hex(value: object) -> bool:
    if value is None:
        return False
    s = str(value).strip()
    return bool(_LISTING_ID_RE.match(s))


def listing_id_from_cell(value: object) -> str:
    """
    Interpreta una celda de CSV: ya es id hex, o URL completa / canónica.
    """
    if value is None:
        return ""
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    if is_listing_id_hex(s):
        return s.lower()
    return listing_id_from_url(s)
