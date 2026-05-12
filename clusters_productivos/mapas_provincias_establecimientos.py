# =========================================================
# MAPAS PROVINCIALES — ESTABLECIMIENTOS POR DEPARTAMENTO
# VERSION FINAL — LABELS + RANKING LATERAL
# =========================================================

from pathlib import Path
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib import patheffects
from unidecode import unidecode
import numpy as np
import warnings

warnings.filterwarnings("ignore")

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
    "mapas_provinciales"
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


def nombre_archivo(x):

    x = limpiar_texto(x)

    x = (
        x.lower()
         .replace(" ", "_")
    )

    return x


# =========================================================
# 3. SHAPE
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

gdf["departamento_shape"] = (
    gdf["nam"]
    .apply(limpiar_texto)
)

# =========================================================
# 5. FEATURES
# =========================================================

print("\n===================================")
print("CARGANDO FEATURES")
print("===================================\n")

df = pd.read_csv(FEATURES_PATH)

print(df.head())

print("\nColumnas features:")
print(df.columns.tolist())

# =========================================================
# 6. LIMPIEZA FEATURES
# =========================================================

df["provincia_id"] = (
    df["provincia_id"]
    .astype(str)
    .str.zfill(2)
)

df["in_departamentos"] = (
    df["in_departamentos"]
    .astype(str)
    .str.replace(".0", "", regex=False)
    .str.zfill(5)
)

df["departamento_limpio"] = (
    df["departamento"]
    .apply(limpiar_texto)
)

# =========================================================
# 7. GEO REFERENCIA
# =========================================================

print("\n===================================")
print("CARGANDO GEO REF")
print("===================================\n")

geo_ref = pd.read_csv(GEO_REF_PATH)

print(geo_ref.head())

print("\nColumnas geo_ref:")
print(geo_ref.columns.tolist())

# =========================================================
# 8. LIMPIEZA GEO REF
# =========================================================

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

# =========================================================
# 9. MERGE FEATURES + GEO
# =========================================================

print("\n===================================")
print("MERGE")
print("===================================\n")

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

print("\nRows features:", len(df))
print("Rows merged:", len(df_geo))

faltantes = df_geo["codigo_geo"].isna().sum()

print("\nSin codigo geo:", faltantes)

# =========================================================
# 10. AGREGACION
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
# =========================================================
# CUANTILES POR PROVINCIA (ROBUSTO)
# =========================================================

mapa_data["cuantil_7"] = np.nan

for prov, dfp in mapa_data.groupby("provincia"):

    idx = dfp.index
    x = dfp["establecimientos"]

    # caso 1: muy pocos valores únicos
    if x.nunique() < 7:
        mapa_data.loc[idx, "cuantil_7"] = 1

    else:
        mapa_data.loc[idx, "cuantil_7"] = (
            pd.qcut(x, 7, labels=False, duplicates="drop") + 1
        )

# =========================================================
# 11. MERGE GEO
# =========================================================

print("\n===================================")
print("MERGE GEO")
print("===================================\n")

mapa = gdf.merge(
    mapa_data[[
        "codigo_geo",
        "provincia",
        "departamento",
        "establecimientos",
        "empleo",
        "exportadoras",
        "cuantil_7"
    ]],
    on="codigo_geo",
    how="left"
)

match_rate = (
    mapa["establecimientos"]
    .notna()
    .mean()
)

print(
    "\nMatch geografico:",
    round(match_rate * 100, 2),
    "%"
)

# =========================================================
# 12. PROYECCION
# =========================================================

print("\n===================================")
print("REPROYECCION")
print("===================================\n")

mapa = mapa.to_crs(22185)

print(mapa.crs)

# =========================================================
# 13. GEOMETRIAS
# =========================================================

invalidas = (~mapa.is_valid).sum()

print("\nGeometrias invalidas:", invalidas)

if invalidas > 0:

    print("\nCorrigiendo geometrias...")

    mapa["geometry"] = (
        mapa.buffer(0)
    )

# =========================================================
# 14. LIMPIEZA VARIABLES
# =========================================================

mapa["establecimientos"] = (
    mapa["establecimientos"]
    .fillna(0)
)

mapa["empleo"] = (
    mapa["empleo"]
    .fillna(0)
)

mapa["exportadoras"] = (
    mapa["exportadoras"]
    .fillna(0)
)

# =========================================================
# 15. WINSORIZACION
# =========================================================

print("\n===================================")
print("WINSORIZACION")
print("===================================\n")

top_cap = (
    mapa["establecimientos"]
    .quantile(0.99)
)

print("Cap 99%:", round(top_cap, 2))

mapa["establecimientos_plot"] = (
    mapa["establecimientos"]
    .clip(upper=top_cap)
)

# =========================================================
# 16.BIS AMBA
# =========================================================

print("\n===================================")
print("CONFIGURANDO AMBA")
print("===================================\n")

AMBA_40 = [
    "ALMIRANTE BROWN",
    "AVELLANEDA",
    "BERAZATEGUI",
    "BERISSO",
    "BRANDSEN",
    "CAMPANA",
    "CANUELAS",
    "ENSENADA",
    "ESCOBAR",
    "ESTEBAN ECHEVERRIA",
    "EXALTACION DE LA CRUZ",
    "EZEIZA",
    "FLORENCIO VARELA",
    "GENERAL LAS HERAS",
    "GENERAL RODRIGUEZ",
    "GENERAL SAN MARTIN",
    "HURLINGHAM",
    "ITUZAINGO",
    "JOSE C PAZ",
    "LA MATANZA",
    "LANUS",
    "LA PLATA",
    "LOMAS DE ZAMORA",
    "LUJAN",
    "MALVINAS ARGENTINAS",
    "MARCOS PAZ",
    "MERLO",
    "MORENO",
    "MORON",
    "PILAR",
    "PRESIDENTE PERON",
    "QUILMES",
    "SAN FERNANDO",
    "SAN ISIDRO",
    "SAN MIGUEL",
    "SAN VICENTE",
    "TIGRE",
    "TRES DE FEBRERO",
    "VICENTE LOPEZ",
    "ZARATE"
]

# =========================================================
# CABA
# =========================================================

comunas_caba = mapa[
    mapa["nam"].astype(str).str.contains(
        "Comuna",
        case=False,
        na=False
    )
].copy()

if len(comunas_caba) > 0:

    caba_union = gpd.GeoDataFrame(
        {
            "provincia": ["CABA"],
            "departamento": ["CABA"],
            "establecimientos": [
                comunas_caba["establecimientos"].sum()
            ],
            "empleo": [
                comunas_caba["empleo"].sum()
            ],
            "exportadoras": [
                comunas_caba["exportadoras"].sum()
            ]
        },
        geometry=[comunas_caba.unary_union],
        crs=mapa.crs
    )

else:

    caba_union = gpd.GeoDataFrame(
        columns=mapa.columns,
        crs=mapa.crs
    )

# =========================================================
# AMBA BUENOS AIRES
# =========================================================

amba_ba = mapa.loc[
    (
        mapa["provincia_id"] == "06"
    )
    &
    (
        mapa["departamento_shape"]
        .isin(AMBA_40)
    )
].copy()

# =========================================================
# UNION FINAL
# =========================================================

amba = pd.concat(
    [
        amba_ba,
        caba_union
    ],
    ignore_index=True
)

print("Departamentos AMBA:", len(amba))

# =========================================================
# LABEL POINTS
# =========================================================

amba["label_point"] = (
    amba.representative_point()
)

amba["x"] = (
    amba["label_point"]
    .x
)

amba["y"] = (
    amba["label_point"]
    .y
)

# =========================================================
# RANKING
# =========================================================

ranking = (
    amba[
        [
            "departamento",
            "establecimientos"
        ]
    ]
    .sort_values(
        "establecimientos",
        ascending=False
    )
    .reset_index(drop=True)
)

ranking["rank"] = (
    ranking.index + 1
)

# =========================================================
# DOS COLUMNAS
# =========================================================

mitad = int(
    np.ceil(len(ranking) / 2)
)

ranking_1 = ranking.iloc[:mitad]

ranking_2 = ranking.iloc[mitad:]

# =========================================================
# FIGURA
# =========================================================

fig = plt.figure(
    figsize=(30, 22)
)

ax_map = fig.add_axes(
    [0.01, 0.02, 0.67, 0.96]
)

ax_rank = fig.add_axes(
    [0.69, 0.02, 0.30, 0.96]
)

# =========================================================
# MAPA
# =========================================================

try:

    n_classes = min(
        7,
        amba["establecimientos_plot"].nunique()
    )

    amba.plot(
        column="establecimientos_plot",
        cmap="YlOrRd",
        linewidth=0.8,
        edgecolor="black",
        legend=True,
        scheme="naturalbreaks",
        k=max(n_classes, 2),
        ax=ax_map,
        missing_kwds={
            "color": "lightgrey",
            "label": "Sin datos"
        }
    )

except Exception as e:

    print("ERROR MAPA AMBA:", e)

    amba.plot(
        color="lightgrey",
        linewidth=0.8,
        edgecolor="black",
        ax=ax_map
    )

# =========================================================
# LABELS GIGANTES
# =========================================================

for idx, row in amba.iterrows():

    try:

        nombre_depto = str(
            row["departamento"]
        )

        valor = int(
            row["establecimientos"]
        )

        label = (
            f"{nombre_depto}\n"
            f"{valor:,}"
        )

        txt = ax_map.text(
            row["x"],
            row["y"],
            label,
            fontsize=22,
            ha="center",
            va="center",
            color="black",
            fontweight="bold",
            linespacing=1.3
        )

        txt.set_path_effects([
            patheffects.withStroke(
                linewidth=5,
                foreground="white"
            )
        ])

    except:
        pass

# =========================================================
# TITULO
# =========================================================

ax_map.set_title(
    "AMBA\n"
    "Establecimientos por Departamento\n"
    "(Natural Breaks — k=7)",
    fontsize=34,
    fontweight="bold",
    pad=25
)

ax_map.axis("off")

# =========================================================
# PANEL RANKING
# =========================================================

ax_rank.axis("off")

ax_rank.text(
    0.02,
    0.99,
    "Ranking AMBA\n"
    "por cantidad de establecimientos",
    fontsize=28,
    fontweight="bold",
    va="top"
)

# =========================================================
# COLUMNA 1
# =========================================================

y1 = 0.93

for idx, row in ranking_1.iterrows():

    texto = (
        f"{int(row['rank']):02d}. "
        f"{row['departamento']} — "
        f"{int(row['establecimientos']):,}"
    )

    ax_rank.text(
        0.02,
        y1,
        texto,
        fontsize=18,
        va="top"
    )

    y1 -= 0.038

# =========================================================
# COLUMNA 2
# =========================================================

y2 = 0.93

for idx, row in ranking_2.iterrows():

    texto = (
        f"{int(row['rank']):02d}. "
        f"{row['departamento']} — "
        f"{int(row['establecimientos']):,}"
    )

    ax_rank.text(
        0.52,
        y2,
        texto,
        fontsize=18,
        va="top"
    )

    y2 -= 0.038

# =========================================================
# 17. EXPORT TABLAS
# =========================================================

print("\n===================================")
print("EXPORTANDO TABLAS")
print("===================================\n")

PNG_AMBA = (
    OUTPUT_DIR /
    "amba_mapa_rank.png"
)

PDF_AMBA = (
    OUTPUT_DIR /
    "amba_mapa_rank.pdf"
)

plt.savefig(
    PNG_AMBA,
    dpi=300,
    bbox_inches="tight"
)

plt.savefig(
    PDF_AMBA,
    dpi=300,
    bbox_inches="tight"
)

plt.close()

print("\nAMBA EXPORTADO")
print(PNG_AMBA)
print(PDF_AMBA)

for prov in sorted(mapa["provincia"].dropna().unique()):

    print(prov)

    sub = mapa.loc[
        mapa["provincia"] == prov,
        [
            "provincia",
            "departamento",
            "codigo_geo",
            "establecimientos",
            "empleo",
            "exportadoras",
            "cuantil_7"
        ]
    ].copy()

    nombre = nombre_archivo(prov)

    csv_out = OUTPUT_DIR / f"{nombre}_departamentos.csv"
    xlsx_out = OUTPUT_DIR / f"{nombre}_departamentos.xlsx"

    sub.to_csv(csv_out, index=False)

    try:
        sub.to_excel(xlsx_out, index=False)
    except Exception as e:
        print(f"No se pudo exportar Excel en {prov}: {e}")

# =========================================================
# 18. MAPAS PROVINCIALES
# NATURAL BREAKS K=7 + LABELS GIGANTES
# + DOBLE COLUMNA RANKING
# =========================================================

print("\n===================================")
print("GENERANDO MAPAS")
print("===================================\n")

for prov in mapa["provincia"].dropna().unique():

    print(f"MAPA: {prov}")

    sub = mapa.loc[
        mapa["provincia"] == prov
    ].copy()

    if len(sub) == 0:
        continue

    # =====================================================
    # LABEL POINTS
    # =====================================================

    sub["label_point"] = (
        sub.representative_point()
    )

    sub["x"] = (
        sub["label_point"]
        .x
    )

    sub["y"] = (
        sub["label_point"]
        .y
    )

    # =====================================================
    # RANKING
    # =====================================================

    ranking = (
        sub[
            [
                "departamento",
                "establecimientos"
            ]
        ]
        .sort_values(
            "establecimientos",
            ascending=False
        )
        .reset_index(drop=True)
    )

    ranking["rank"] = (
        ranking.index + 1
    )

    # =====================================================
    # SPLIT DOS COLUMNAS
    # =====================================================

    mitad = int(
        np.ceil(len(ranking) / 2)
    )

    ranking_1 = ranking.iloc[:mitad]

    ranking_2 = ranking.iloc[mitad:]

    # =====================================================
    # FIGURA
    # =====================================================

    fig = plt.figure(
        figsize=(26, 18)
    )

    # MAPA
    ax_map = fig.add_axes(
        [0.01, 0.02, 0.68, 0.96]
    )

    # RANKING
    ax_rank = fig.add_axes(
        [0.70, 0.02, 0.29, 0.96]
    )

    # =====================================================
    # MAPA
    # =====================================================

    try:

        n_classes = min(
            7,
            sub["establecimientos_plot"].nunique()
        )

        sub.plot(
            column="establecimientos_plot",
            cmap="YlOrRd",
            linewidth=0.5,
            edgecolor="black",
            legend=True,
            scheme="naturalbreaks",
            k=max(n_classes, 2),
            ax=ax_map,
            missing_kwds={
                "color": "lightgrey",
                "label": "Sin datos"
            }
        )

    except Exception as e:

        print(f"ERROR MAPA {prov}: {e}")

        sub.plot(
            color="lightgrey",
            linewidth=0.5,
            edgecolor="black",
            ax=ax_map
        )

    # =====================================================
    # LABELS INTERNOS GIGANTES
    # =====================================================

    for idx, row in sub.iterrows():

        try:

            nombre_depto = str(
                row["departamento"]
            )

            valor = int(
                row["establecimientos"]
            )

            label = (
                f"{nombre_depto}\n"
                f"{valor:,}"
            )

            txt = ax_map.text(
                row["x"],
                row["y"],
                label,
                fontsize=60,
                ha="center",
                va="center",
                color="black",
                fontweight="bold",
                linespacing=1.4
            )

            txt.set_path_effects([
                patheffects.withStroke(
                    linewidth=12,
                    foreground="white"
                )
            ])

        except:
            pass

    # =====================================================
    # TITULO
    # =====================================================

    ax_map.set_title(
        f"{prov}\n"
        "Establecimientos por Departamento\n"
        "(Natural Breaks — k=7)",
        fontsize=34,
        fontweight="bold",
        pad=30
    )

    ax_map.axis("off")

    # =====================================================
    # PANEL RANKING
    # =====================================================

    ax_rank.axis("off")

    titulo_rank = (
        "Ranking departamental\n"
        "por cantidad de establecimientos\n"
    )

    ax_rank.text(
        0.02,
        0.99,
        titulo_rank,
        fontsize=24,
        fontweight="bold",
        va="top"
    )

    # =====================================================
    # COLUMNA 1
    # =====================================================

    y1 = 0.93

    for idx, row in ranking_1.iterrows():

        rank = row["rank"]

        depto = row["departamento"]

        valor = int(
            row["establecimientos"]
        )

        texto = (
            f"{rank:02d}. "
            f"{depto} — "
            f"{valor:,}"
        )

        ax_rank.text(
            0.02,
            y1,
            texto,
            fontsize=18,
            va="top"
        )

        y1 -= 0.035

    # =====================================================
    # COLUMNA 2
    # =====================================================

    y2 = 0.93

    for idx, row in ranking_2.iterrows():

        rank = row["rank"]

        depto = row["departamento"]

        valor = int(
            row["establecimientos"]
        )

        texto = (
            f"{rank:02d}. "
            f"{depto} — "
            f"{valor:,}"
        )

        ax_rank.text(
            0.52,
            y2,
            texto,
            fontsize=18,
            va="top"
        )

        y2 -= 0.035

    # =====================================================
    # EXPORT
    # =====================================================

    nombre = nombre_archivo(prov)

    png_out = (
        OUTPUT_DIR /
        f"{nombre}_mapa_rank.png"
    )

    pdf_out = (
        OUTPUT_DIR /
        f"{nombre}_mapa_rank.pdf"
    )

    plt.savefig(
        png_out,
        dpi=300,
        bbox_inches="tight"
    )

    plt.savefig(
        pdf_out,
        dpi=300,
        bbox_inches="tight"
    )

    plt.close()


# =========================================================
# 20. CHEQUEOS GEOGRAFICOS
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

# =========================================================
# 21. FINAL
# =========================================================

print("\n===================================")
print("FINALIZADO")
print("===================================\n")

print("Output:")
print(OUTPUT_DIR)