"""
Limpieza del CSV de salida de 02: filas con precio, quitar columnas auxiliares,
normalizar `link`, y asegurar columna `fecha_publicacion` (desde 02 o migrando `recencia` si aún existe).
Correcciones puntuales (solo tipo desde URL, etc.) → 04_correcciones.py.
"""

import re
from pathlib import Path
from typing import Dict, Optional
import pandas as pd

from listing_id import canonical_listing_url, is_listing_id_hex
from urbania_recencia import recencia_dias_a_fecha_publicacion


INPUT_CSV_PATH = Path("output/OFIM_02.csv")
EXPORT_CSV = True
OUTPUT_CSV_PATH = Path("output/OFIM_03.csv")
CSV_SEP = "|"
EXPORT_EXCEL = True
OUTPUT_EXCEL_PATH = Path("output/OFIM_03.xlsx")


def shorten_listing_url(url: object) -> str:
    """Quita query string y fragmento (n_src, n_pg, etc.) para un link más corto."""
    return canonical_listing_url(url)


def _normalize_link_for_output(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    if is_listing_id_hex(s):
        return s.lower()
    return canonical_listing_url(s)


def aplicar_solo_link(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza la columna `link` (hex o URL canónica)."""
    out = df.copy()
    if "link" in out.columns:
        out["link"] = out["link"].map(_normalize_link_for_output)
    return out


def _move_column_after(df: pd.DataFrame, col: str, after: str) -> pd.DataFrame:
    if col not in df.columns or after not in df.columns:
        return df
    cols = [c for c in df.columns if c != col]
    idx = cols.index(after) + 1
    cols.insert(idx, col)
    return df[cols]


def asegurar_fecha_publicacion(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Si viene `fecha_publicacion` de 02, se conserva.
    - Si solo existe `recencia` (días, datos viejos), se convierte con la misma regla que en 02 y se elimina `recencia`.
    """
    out = df.copy()
    if "fecha_publicacion" not in out.columns:
        out["fecha_publicacion"] = ""
    if "recencia" in out.columns:
        mask = out["fecha_publicacion"].isna() | (
            out["fecha_publicacion"].astype(str).str.strip().isin(["", "nan"])
        )
        sub = out.loc[mask, "recencia"]
        out.loc[mask, "fecha_publicacion"] = sub.map(recencia_dias_a_fecha_publicacion)
        out = out.drop(columns=["recencia"], errors="ignore")
    if "fecha_publicacion" in out.columns and "antiguedad" in out.columns:
        out = _move_column_after(out, "fecha_publicacion", "antiguedad")
    return out


def to_float(value: str) -> Optional[float]:
    if value is None:
        return None
    cleaned = value.replace(",", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def normalize_text(value: str) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def extract_numeric(text: str, patterns: list[str]) -> Optional[float]:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return to_float(match.group(1))
    return None


def parse_price_tokens(price_tokens: str) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {
        "precio_venta_soles": None,
        "precio_venta_usd": None,
        "precio_alquiler_soles": None,
        "precio_alquiler_usd": None,
        "mantenimiento_venta_soles": None,
        "mantenimiento_venta_usd": None,
        "mantenimiento_alquiler_soles": None,
        "mantenimiento_alquiler_usd": None,
    }

    token_text = normalize_text(price_tokens)
    if not token_text:
        return out

    parts = [p.strip() for p in token_text.split("|") if p.strip()]
    for part in parts:
        match = re.search(
            r"^(S/|USD)\s*([\d,]+(?:\.\d+)?)_(venta|alquiler)(?:_(mant))?$",
            part,
            flags=re.IGNORECASE,
        )
        if not match:
            continue

        currency_raw = match.group(1).upper()
        amount = to_float(match.group(2))
        mode = match.group(3).lower()
        is_mant = bool(match.group(4))

        currency = "usd" if currency_raw == "USD" else "soles"

        if is_mant:
            key = f"mantenimiento_{mode}_{currency}"
        else:
            key = f"precio_{mode}_{currency}"
        out[key] = amount

    return out


def parse_icon_features(icon_features: str) -> Dict[str, Optional[float]]:
    text = normalize_text(icon_features)
    out: Dict[str, Optional[float]] = {
        "m2_tot": None,
        "m2_cub": None,
        "dormitorios": None,
        "banos": None,
        "medios_banos": None,
        "estacionamientos": None,
        "antiguedad_anos": None,
        "a_estrenar": None,
    }

    if not text:
        return out

    out["m2_tot"] = extract_numeric(
        text,
        [
            r"(\d+(?:[.,]\d+)?)\s*m²\s*tot\.?",
            r"m²\s*tot\.?\s*(?:\||:)?\s*(\d+(?:[.,]\d+)?)",
        ],
    )
    out["m2_cub"] = extract_numeric(
        text,
        [
            r"(\d+(?:[.,]\d+)?)\s*m²\s*(?:cub\.?|tech(?:ada)?)",
            r"m²\s*(?:cub\.?|tech(?:ada)?)\s*(?:\||:)?\s*(\d+(?:[.,]\d+)?)",
        ],
    )
    out["dormitorios"] = extract_numeric(
        text,
        [
            r"(\d+(?:[.,]\d+)?)\s*dorm\.?",
            r"dorm\.?\s*(?:\||:)?\s*(\d+(?:[.,]\d+)?)",
        ],
    )
    out["banos"] = extract_numeric(
        text,
        [
            r"(\d+(?:[.,]\d+)?)\s*bañ(?:o|os)",
            r"bañ(?:o|os)\s*(?:\||:)?\s*(\d+(?:[.,]\d+)?)",
        ],
    )
    out["medios_banos"] = extract_numeric(
        text,
        [
            r"(\d+(?:[.,]\d+)?)\s*medios?\s*bañ(?:o|os)",
            r"medios?\s*bañ(?:o|os)\s*(?:\||:)?\s*(\d+(?:[.,]\d+)?)",
        ],
    )
    out["estacionamientos"] = extract_numeric(
        text,
        [
            r"(\d+(?:[.,]\d+)?)\s*(?:estac\.?|estacionamientos?)",
            r"(?:estac\.?|estacionamientos?)\s*(?:\||:)?\s*(\d+(?:[.,]\d+)?)",
        ],
    )
    out["antiguedad_anos"] = extract_numeric(
        text,
        [
            r"(\d+(?:[.,]\d+)?)\s*años",
            r"años\s*(?:\||:)?\s*(\d+(?:[.,]\d+)?)",
        ],
    )
    out["a_estrenar"] = 1.0 if re.search(r"\ba estrenar\b", text, flags=re.IGNORECASE) else 0.0

    return out


def _has_any_price_new_format(row: pd.Series) -> bool:
    cols = ["venta_pen", "venta_usd", "mant", "alquiler_pen", "alquiler_usd", "mant_alquiler"]
    for c in cols:
        if c not in row.index:
            continue
        v = row[c]
        if pd.isna(v):
            continue
        if str(v).strip() != "":
            return True
    return False


def clean_dataset(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()

    if "price_tokens" not in work.columns and "venta_pen" in work.columns:
        mask = work.apply(_has_any_price_new_format, axis=1)
        work = work.loc[mask].copy()
        for col in ["distrito", "tipo"]:
            if col in work.columns:
                work[col] = (
                    work[col]
                    .astype(str)
                    .str.replace("_distrito", "", regex=False)
                    .str.replace("_tipo", "", regex=False)
                )
        drop_cols = ["detail_li_2", "static_map_src", "extra_info", "error"]
        work = work.drop(columns=[c for c in drop_cols if c in work.columns], errors="ignore")
        work = asegurar_fecha_publicacion(work)
        return aplicar_solo_link(work)

    work["price_tokens"] = work["price_tokens"].fillna("").astype(str)
    work["icon_features"] = work["icon_features"].fillna("").astype(str)
    work = work.loc[work["price_tokens"].str.strip() != ""].copy()

    price_cols = work["price_tokens"].apply(parse_price_tokens).apply(pd.Series)
    icon_cols = work["icon_features"].apply(parse_icon_features).apply(pd.Series)

    result = pd.concat([work, price_cols, icon_cols], axis=1)

    for col in ["distrito", "tipo"]:
        if col in result.columns:
            result[col] = result[col].astype(str).str.replace("_distrito", "", regex=False).str.replace("_tipo", "", regex=False)

    drop_cols = [
        "price_tokens",
        "icon_features",
        "detail_li_2",
        "static_map_src",
        "extra_info",
        "error",
    ]
    result = result.drop(columns=[c for c in drop_cols if c in result.columns], errors="ignore")

    result = asegurar_fecha_publicacion(result)
    return aplicar_solo_link(result)


def main() -> None:
    if not INPUT_CSV_PATH.exists():
        raise FileNotFoundError(f"No existe el archivo de entrada: {INPUT_CSV_PATH}")

    df = pd.read_csv(INPUT_CSV_PATH, encoding="utf-8-sig", sep=CSV_SEP)
    clean_df = clean_dataset(df)

    print("\nPrimeras 5 filas de la base limpia:")
    print(clean_df.head(5))

    if EXPORT_CSV:
        OUTPUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        clean_df.to_csv(OUTPUT_CSV_PATH, index=False, encoding="utf-8-sig", sep=CSV_SEP)
        print(f"\nCSV limpio exportado en: {OUTPUT_CSV_PATH}")

    if EXPORT_EXCEL:
        OUTPUT_EXCEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        clean_df.to_excel(OUTPUT_EXCEL_PATH, index=False)
        print(f"Excel limpio exportado en: {OUTPUT_EXCEL_PATH}")


if __name__ == "__main__":
    main()
