"""Convierte textos tipo 'Publicado hace X días' a días aproximados (más reciente = menor)."""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Optional

# Ajuste al pasar de "hace N días" a fecha de publicación (restar a hoy).
RECENCIA_DIAS_CORRECCION = 1
# Por encima de esto no se emite fecha ISO; se usa el texto "1 año".
RECENCIA_DIAS_MAX_FECHA_EXACTA = 365


def texto_publicacion_a_recencia_dias(text: object) -> Optional[int]:
    """
    Días aproximados como entero, o None si no se puede interpretar.
    Ej.: 'Publicado desde ayer' -> 1, 'hace 2 semanas' -> 14.
    """
    if text is None:
        return None
    if isinstance(text, bool):
        return None
    if isinstance(text, (int, float)):
        if text != text:  # NaN
            return None
        return int(text)

    t = re.sub(r"\s+", " ", str(text)).strip().lower()
    if not t or t in ("nan", "none", ""):
        return None

    if re.fullmatch(r"\d+", t):
        return int(t)

    if "ayer" in t and "hace" not in t:
        return 1
    if re.search(r"\b(hoy|today)\b", t) and "ayer" not in t:
        return 0

    if re.search(r"m[aá]s de\s+un\s+a[nñ]o", t) or re.search(r"m[aá]s de\s+1\s+a[nñ]o", t):
        return 366
    m = re.search(r"m[aá]s de\s+(\d+)\s*a[nñ]os?", t)
    if m:
        n = int(m.group(1))
        return n * 365 + 1

    m = re.search(r"hace\s+(\d+)\s*d[ií]as?", t)
    if m:
        return int(m.group(1))

    m = re.search(r"hace\s+(\d+)\s*semanas?", t)
    if m:
        return int(m.group(1)) * 7

    m = re.search(r"hace\s+(\d+)\s*mes(?:es)?", t)
    if m:
        return int(m.group(1)) * 30

    m = re.search(r"hace\s+(\d+)\s*a[nñ]os?", t)
    if m:
        return int(m.group(1)) * 365

    if re.search(r"\b(un|una)\s+d[ií]a\b", t) or re.search(r"\b1\s+d[ií]a\b", t):
        return 1
    if re.search(r"\b(un|una)\s+semana\b", t):
        return 7
    if re.search(r"\b(un|una)\s+mes\b", t):
        return 30
    if re.search(r"\b(un|un)\s+a[nñ]o\b", t):
        return 365

    m = re.search(r"(\d+)", t)
    if m:
        return int(m.group(1))

    return None


def recencia_dias_a_fecha_publicacion(dias: object) -> str:
    """
    Convierte días de antigüedad del aviso (desde texto_publicacion_a_recencia_dias) en:
    - fecha ISO (YYYY-MM-DD), o
    - el texto "1 año" si la publicación supera RECENCIA_DIAS_MAX_FECHA_EXACTA días.
    """
    if dias is None:
        return ""
    if isinstance(dias, float):
        if dias != dias:
            return ""
        dias = int(dias)
    if not isinstance(dias, int):
        try:
            dias = int(dias)
        except (TypeError, ValueError):
            return ""
    if dias < 0:
        return ""
    if dias > RECENCIA_DIAS_MAX_FECHA_EXACTA:
        return "1 año"
    delta = dias + RECENCIA_DIAS_CORRECCION
    pub = date.today() - timedelta(days=delta)
    return pub.isoformat()
