# =========================================================
# MAPA NACIONAL ESTABLECIMIENTOS POR DEPARTAMENTO
# =========================================================

from pathlib import Path
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from unidecode import unidecode
import numpy as np

# =========================================================
# 1. PATHS
# =========================================================

BASE = Path(
    r"C:\Users\agusr\Desktop\Instituto Argentina Grande\clusters_productivos\clusters_productivos"
)

SHAPE_PATH = (
    BASE /
    "departamento" /
    "departamentoPolygon.shp"
)

FEATURES_PATH = (
    BASE /
    "pipeline" /
    "output" /
    "marts" /
    "features_departamento.csv"
)

GEO_REF_PATH = (
    BASE /
    "pipeline" /
    "output" /
    "marts" /
    "geo_referencia_para_mapas.csv"
)

OUTPUT_DIR = (
    BASE /
    "pipeline" /
    "output" /
    "mapas"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)

# =========================================================
# 2. FUNCIONES
# =========================================================

def limpiar_texto(x):

    if pd.isna(x):
        return ""

    x = str(x)

    x = unidecode(x)

    x = x.upper()

    x = (
        x.replace(".", "")
         .replace(",", "")
         .replace("-", " ")
    )

    x = " ".join(x.split())

    return x

# =========================================================
# 3. CARGA SHAPE
# =========================================================

print("\n===================================")
print("CARGANDO SHAPE")
print("===================================\n")

gdf = gpd.read_file(SHAPE_PATH)

print(gdf.head())

print("\nColumnas shape:")
print(gdf.columns.tolist())

print("\nCRS:")
print(gdf.crs)

# =========================================================
# 4. LIMPIEZA SHAPE
# =========================================================

print("\n===================================")
print("LIMPIEZA SHAPE")
print("===================================\n")

gdf["departamento_shape"] = (
    gdf["nam"]
    .apply(limpiar_texto)
)

# codigo geografico oficial shape
gdf["codigo_geo"] = (
    gdf["in1"]
    .astype(str)
    .str.replace(".0", "", regex=False)
    .str.zfill(5)
)

gdf["provincia_id"] = (
    gdf["codigo_geo"]
    .str[:2]
)

print(
    gdf[
        [
            "nam",
            "departamento_shape",
            "codigo_geo"
        ]
    ].head()
)

# =========================================================
# 5. CARGA FEATURES
# =========================================================

print("\n===================================")
print("CARGANDO FEATURES")
print("===================================\n")

df = pd.read_csv(FEATURES_PATH)

print(df.head())

print("\nColumnas:")
print(df.columns.tolist())

# =========================================================
# 6. LIMPIEZA FEATURES
# =========================================================

print("\n===================================")
print("LIMPIEZA FEATURES")
print("===================================\n")

# provincia id
df["provincia_id"] = (
    df["provincia_id"]
    .astype(str)
    .str.zfill(2)
)

# codigo territorial
df["in_departamentos"] = (
    df["in_departamentos"]
    .astype(str)
    .str.replace(".0", "", regex=False)
    .str.zfill(5)
)

# nombres limpios
df["departamento_limpio"] = (
    df["departamento"]
    .apply(limpiar_texto)
)

print(
    df[
        [
            "provincia_id",
            "in_departamentos",
            "departamento"
        ]
    ].head()
)

# =========================================================
# 7. GEO REFERENCIA
# =========================================================

print("\n===================================")
print("GEO REFERENCIA")
print("===================================\n")

geo_ref = pd.read_csv(GEO_REF_PATH)

print(geo_ref.head())

print("\nColumnas geo_ref:")
print(geo_ref.columns.tolist())

# =========================================================
# 8. NORMALIZAR GEO_REF
# =========================================================

print("\n===================================")
print("NORMALIZANDO GEO_REF")
print("===================================\n")

geo_ref["provincia_id"] = (
    geo_ref["provincia_id"]
    .astype(str)
    .str.zfill(2)
)

geo_ref["in_departamentos"] = (
    geo_ref["in_departamentos"]
    .astype(str)
    .str.replace(".0", "", regex=False)
    .str.zfill(5)
)

geo_ref["codigo_geo"] = (
    geo_ref["in_departamentos"]
)

print(
    geo_ref[
        [
            "provincia_id",
            "in_departamentos",
            "codigo_geo"
        ]
    ].head()
)

# =========================================================
# 9. MERGE FEATURES + GEOREF
# =========================================================

print("\n===================================")
print("MERGE FEATURES + GEO")
print("===================================\n")

merge_cols = ["in_departamentos"]

print("Usando merge por:")
print(merge_cols)

print("\nTIPOS DF")
print(df[merge_cols].dtypes)

print("\nTIPOS GEO_REF")
print(geo_ref[merge_cols].dtypes)

df_geo = df.merge(
    geo_ref[
        [
            "in_departamentos",
            "codigo_geo"
        ]
    ].drop_duplicates(),
    on="in_departamentos",
    how="left"
)

print("\nMerge OK")

# =========================================================
# 10. CONTROLES
# =========================================================

print("\n===================================")
print("CONTROLES")
print("===================================\n")

print("Rows features:", len(df))
print("Rows merged:", len(df_geo))

faltantes = df_geo["codigo_geo"].isna().sum()

print("\nSin codigo geo:", faltantes)

if faltantes > 0:

    print("\nDepartamentos sin match:\n")

    print(
        df_geo.loc[
            df_geo["codigo_geo"].isna(),
            [
                "provincia",
                "departamento",
                "in_departamentos"
            ]
        ]
        .drop_duplicates()
        .head(50)
    )

# =========================================================
# 11. AGREGACION MAPA
# =========================================================

print("\n===================================")
print("AGREGACION")
print("===================================\n")

mapa_data = (
    df_geo
    .groupby(
        [
            "codigo_geo",
            "provincia",
            "departamento"
        ],
        as_index=False
    )
    .agg({
        "establecimientos": "sum",
        "empleo": "sum",
        "exportadoras": "sum"
    })
)

print(mapa_data.head())

# =========================================================
# 12. CHEQUEO CODIGOS
# =========================================================

print("\n===================================")
print("CHEQUEO CODIGOS")
print("===================================\n")

print("\nEJEMPLOS SHAPE")
print(
    gdf[
        [
            "codigo_geo",
            "nam"
        ]
    ].head(20)
)

print("\nEJEMPLOS DATA")
print(
    mapa_data[
        [
            "codigo_geo"
        ]
    ].head(20)
)

# =========================================================
# 13. MERGE FINAL GEO
# =========================================================

print("\n===================================")
print("MERGE FINAL")
print("===================================\n")

mapa = gdf.merge(
    mapa_data,
    on="codigo_geo",
    how="left"
)

print(mapa.head())

# =========================================================
# 14. REPROYECCION
# =========================================================

print("\n===================================")
print("REPROYECCION")
print("===================================\n")

mapa = mapa.to_crs(22185)

# =========================================================
# 15. VARIABLES MAPA
# =========================================================

mapa["establecimientos"] = (
    mapa["establecimientos"]
    .fillna(0)
)

mapa["empleo"] = (
    mapa["empleo"]
    .fillna(0)
)

# =========================================================
# 16. ESTADISTICAS
# =========================================================

print("\n===================================")
print("ESTADISTICAS")
print("===================================\n")

print(
    mapa["establecimientos"]
    .describe()
)

# =========================================================
# 17. MAPA
# =========================================================

print("\n===================================")
print("GENERANDO MAPA")
print("===================================\n")

fig, ax = plt.subplots(
    figsize=(18, 16)
)

mapa.plot(
    column="establecimientos",
    cmap="YlOrRd",
    linewidth=0.15,
    edgecolor="black",
    legend=True,
    scheme="naturalbreaks",
    k=7,
    ax=ax,
    missing_kwds={
        "color": "lightgrey",
        "label": "Sin datos"
    }
)

ax.set_title(
    "Argentina — Establecimientos por Departamento",
    fontsize=22,
    fontweight="bold",
    pad=20
)

ax.axis("off")

# =========================================================
# 18. EXPORT
# =========================================================

PNG_OUT = (
    OUTPUT_DIR /
    "mapa_establecimientos_argentina.png"
)

PDF_OUT = (
    OUTPUT_DIR /
    "mapa_establecimientos_argentina.pdf"
)

CSV_OUT = (
    OUTPUT_DIR /
    "mapa_establecimientos_argentina.csv"
)

plt.savefig(
    PNG_OUT,
    dpi=300,
    bbox_inches="tight"
)

plt.savefig(
    PDF_OUT,
    bbox_inches="tight"
)

mapa.to_csv(
    CSV_OUT,
    index=False
)

print("\n===================================")
print("EXPORTADO")
print("===================================\n")

print(PNG_OUT)
print(PDF_OUT)
print(CSV_OUT)

plt.show()

# =========================================================
# 19. CHEQUEOS GEOGRAFICOS
# =========================================================

print("\n===================================")
print("CHEQUEOS GEOGRAFICOS")
print("===================================\n")

shape_codes = set(
    gdf["codigo_geo"]
)

data_codes = set(
    mapa_data["codigo_geo"]
)

print(
    "\nCodigos solo en shape:",
    len(shape_codes - data_codes)
)

print(
    "\nCodigos solo en data:",
    len(data_codes - shape_codes)
)

print("\nEjemplo solo shape:")

print(
    list(shape_codes - data_codes)[:20]
)

print("\nEjemplo solo data:")

print(
    list(data_codes - shape_codes)[:20]
)