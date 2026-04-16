# Precio Depas - Pipeline OFIM

Pipeline de scraping y transformación de publicaciones inmobiliarias (Urbania), con salidas estandarizadas por etapas `OFIM_*`.

## Flujo del proceso

Flujo principal:

1. `01_precios_links.py`
2. `02_precio_data.py`
3. `03_clean_precio_data.py`

Flujo opcional de correcciones:

4. `04_correcciones.py`

## Que significa cada archivo OFIM

- `OFIM_01`: Links de publicaciones inmobiliarias.
- `OFIM_02`: Extracción de información.
- `OFIM_03`: Cleaning de `OFIM_02`.
- `OFIM_04` (opcional): Correcciones puntuales sobre datos limpios.

## Etapas en detalle

### 1) `01_precios_links.py` - Descarga de links

- Input: Urbania (web, páginas de búsqueda).
- Proceso: extrae links únicos de avisos (`href`).
- Output: `output/OFIM_01.csv`.

### 2) `02_precio_data.py` - Extracción de información

- Input: `output/OFIM_01.csv`.
- Proceso: visita cada aviso y extrae variables (precios, tipo, distrito, m2, dormitorios, baños, coordenadas, etc.).
- Output: `output/OFIM_02.csv`.

### 3) `03_clean_precio_data.py` - Limpieza

- Input: `output/OFIM_02.csv`.
- Proceso: limpia y normaliza campos (por ejemplo, `link`, `distrito`, `tipo`, `fecha_publicacion`) y elimina columnas auxiliares.
- Outputs:
  - `output/OFIM_03.csv`
  - `output/OFIM_03.xlsx`

### 4) `04_correcciones.py` - Correcciones opcionales

- Uso: ajustes puntuales sobre CSV ya generados.
- Caso comun: reaplicar `tipo` desde URL sobre `OFIM_03`.
- Outputs por defecto:
  - `output/OFIM_04.csv`
  - `output/OFIM_04.xlsx`
- Tambien permite migrar `recencia` -> `fecha_publicacion` en un CSV objetivo.

## Mapa input -> output (resumen rapido)

- `01_precios_links.py`: Urbania (web) -> `output/OFIM_01.csv`
- `02_precio_data.py`: `output/OFIM_01.csv` -> `output/OFIM_02.csv`
- `03_clean_precio_data.py`: `output/OFIM_02.csv` -> `output/OFIM_03.csv` + `output/OFIM_03.xlsx`
- `04_correcciones.py` (opcional): `output/OFIM_03.csv` -> `output/OFIM_04.csv` + `output/OFIM_04.xlsx`

