#!/usr/bin/env python3
"""
Correcciones puntuales sobre CSV ya exportados (no forma parte del flujo normal 01→02→03).

Ejemplos:
  python 04_correcciones.py --refresh-tipo-clean
  python 04_correcciones.py --migrar-recencia-a-fecha output/OFIM_02.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd

from listing_id import canonical_listing_url, is_listing_id_hex, tipo_from_urbania_listing_url
from urbania_recencia import recencia_dias_a_fecha_publicacion

CSV_SEP = "|"
DEFAULT_CLEAN_CSV = Path("output/OFIM_03.csv")
DEFAULT_OUTPUT_CSV = Path("output/OFIM_04.csv")
DEFAULT_EXCEL = Path("output/OFIM_04.xlsx")


def _normalize_link_for_output(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    if is_listing_id_hex(s):
        return s.lower()
    return canonical_listing_url(s)


def _series_effective_listing_url_for_tipo(df: pd.DataFrame) -> pd.Series:
    empty = pd.Series([""] * len(df), index=df.index, dtype=object)
    uc = df["url_canonical"] if "url_canonical" in df.columns else empty
    lk = df["link"] if "link" in df.columns else empty

    def is_http(s: object) -> bool:
        if s is None or (isinstance(s, float) and pd.isna(s)):
            return False
        return str(s).strip().startswith("http")

    out: list[str] = []
    for i in df.index:
        a, b = uc.get(i, ""), lk.get(i, "")
        out.append(str(a).strip() if is_http(a) else (str(b).strip() if is_http(b) else ""))
    return pd.Series(out, index=df.index, dtype=object)


def apply_tipo_from_urbania_urls(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    if "tipo" not in work.columns:
        work["tipo"] = ""
    urls = _series_effective_listing_url_for_tipo(work)
    mapped = urls.map(lambda u: tipo_from_urbania_listing_url(u) if u else "")
    use = mapped.astype(str).str.strip().ne("") & mapped.notna()
    work.loc[use, "tipo"] = mapped[use]
    return work


def aplicar_solo_link(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "link" in out.columns:
        out["link"] = out["link"].map(_normalize_link_for_output)
    return out


def refresh_tipo_on_clean_csv(
    csv_path: Path = DEFAULT_CLEAN_CSV,
    output_csv_path: Path = DEFAULT_OUTPUT_CSV,
    excel_path: Optional[Path] = DEFAULT_EXCEL,
    export_excel: bool = True,
) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)
    df = pd.read_csv(csv_path, encoding="utf-8-sig", sep=CSV_SEP)
    df = aplicar_solo_link(df)
    df = apply_tipo_from_urbania_urls(df)
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv_path, index=False, encoding="utf-8-sig", sep=CSV_SEP)
    n = int((df["tipo"].astype(str).str.strip() != "").sum()) if "tipo" in df.columns else 0
    print(f"Tipo reaplicado: {output_csv_path} ({len(df)} filas; {n} con tipo no vacío).")
    if export_excel and excel_path is not None:
        excel_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(excel_path, index=False)
        print(f"Excel: {excel_path}")
    return df


def migrar_recencia_a_fecha_en_csv(path: Path, sep: str = CSV_SEP) -> pd.DataFrame:
    """Convierte columna `recencia` (días) en `fecha_publicacion`; quita `recencia`."""
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(path, encoding="utf-8-sig", sep=sep)
    if "recencia" not in df.columns:
        print(f"{path}: no tiene columna 'recencia'; nada que migrar.")
        return df
    if "fecha_publicacion" not in df.columns:
        df["fecha_publicacion"] = ""
    mask = df["fecha_publicacion"].isna() | (
        df["fecha_publicacion"].astype(str).str.strip().isin(["", "nan"])
    )
    sub = df.loc[mask, "recencia"]
    df.loc[mask, "fecha_publicacion"] = sub.map(recencia_dias_a_fecha_publicacion)
    df = df.drop(columns=["recencia"], errors="ignore")
    df.to_csv(path, index=False, encoding="utf-8-sig", sep=sep)
    print(f"Migrado recencia → fecha_publicacion: {path}")
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description="Correcciones opcionales sobre CSV de avisos.")
    ap.add_argument(
        "--refresh-tipo-clean",
        action="store_true",
        help="Relee el CSV limpio y sobrescribe `tipo` desde la URL (listing_id).",
    )
    ap.add_argument(
        "--csv-clean",
        type=Path,
        default=DEFAULT_CLEAN_CSV,
        help="Ruta del CSV limpio de entrada para --refresh-tipo-clean.",
    )
    ap.add_argument(
        "--csv-output",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
        help="Ruta del CSV de salida para --refresh-tipo-clean.",
    )
    ap.add_argument(
        "--migrar-recencia-a-fecha",
        type=Path,
        nargs="?",
        const=Path("output/OFIM_02.csv"),
        default=None,
        metavar="CSV",
        help="Migrar recencia→fecha_publicacion (sin argumento: OFIM_02.csv).",
    )
    args = ap.parse_args()

    if args.refresh_tipo_clean:
        refresh_tipo_on_clean_csv(args.csv_clean, args.csv_output)
    if args.migrar_recencia_a_fecha is not None:
        migrar_recencia_a_fecha_en_csv(args.migrar_recencia_a_fecha)
    if not args.refresh_tipo_clean and args.migrar_recencia_a_fecha is None:
        ap.print_help()


if __name__ == "__main__":
    main()
