# =========================================================
# RENOMBRE EXPLICITO COLUMNAS REDATAM
# sindesca_dpto.csv
# =========================================================

import pandas as pd

# =========================================================
# 1. PATH
# =========================================================

CSV_PATH = r"sindesca_dpto.csv"

# =========================================================
# 2. CARGA
# =========================================================

df = pd.read_csv(CSV_PATH)

print("\nCOLUMNAS ORIGINALES:\n")
print(df.columns.tolist())

# =========================================================
# 3. RENOMBRE EXPLICITO
# =========================================================

rename_dict = {

    # ids
    "REDCODE": "codigo_departamento",
    "Departamento": "departamento",

    # -----------------------------------------------------
    # SERVICIO DOMESTICO (catocup = 1)
    # -----------------------------------------------------

    "numerv1_1": "varones_sd",
    "numerm1_2": "mujeres_sd",
    "denomi1_3": "ocupados_sd",
    "pctvaron1_4": "pct_varones_sd",
    "pctmujer1_5": "pct_mujeres_sd",

    # -----------------------------------------------------
    # ASALARIADOS (catocup = 2)
    # -----------------------------------------------------

    "numerv2_6": "varones_asalariados",
    "numerm2_7": "mujeres_asalariadas",
    "denomi2_8": "ocupados_asalariados",
    "pctvaron2_9": "pct_varones_asalariados",
    "pctmujer2_10": "pct_mujeres_asalariadas",

    # -----------------------------------------------------
    # CUENTA PROPIA (catocup = 3)
    # -----------------------------------------------------

    "numerv3_11": "varones_cp",
    "numerm3_12": "mujeres_cp",
    "denomi3_13": "ocupados_cp",
    "pctvaron3_14": "pct_varones_cp",
    "pctmujer3_15": "pct_mujeres_cp",

    # -----------------------------------------------------
    # PATRONES (catocup = 4)
    # -----------------------------------------------------

    "numerv4_16": "varones_patrones",
    "numerm4_17": "mujeres_patrones",
    "denomi4_18": "ocupados_patrones",
    "pctvaron4_19": "pct_varones_patrones",
    "pctmujer4_20": "pct_mujeres_patrones",

    # -----------------------------------------------------
    # BRECHAS
    # -----------------------------------------------------

    "brechapp1_21": "brecha_pp_sd",
    "brechapct1_22": "brecha_ratio_sd",

    "brechapp2_23": "brecha_pp_asalariados",
    "brechapct2_24": "brecha_ratio_asalariados",

    "brechapp3_25": "brecha_pp_cp",
    "brechapct3_26": "brecha_ratio_cp",

    "brechapp4_27": "brecha_pp_patrones",
    "brechapct4_28": "brecha_ratio_patrones"
}

# =========================================================
# 4. APLICAR RENOMBRE
# =========================================================

df = df.rename(columns=rename_dict)

# =========================================================
# 5. NORMALIZAR CODIGO
# =========================================================

df["codigo_departamento"] = (
    df["codigo_departamento"]
    .astype(str)
    .str.replace(".0", "", regex=False)
    .str.zfill(5)
)

# =========================================================
# 6. CHEQUEO
# =========================================================

print("\nCOLUMNAS RENOMBRADAS:\n")
print(df.columns.tolist())

print("\nHEAD:\n")
print(df.head())

# =========================================================
# 7. EXPORT LIMPIO
# =========================================================

OUT = "sindesca_dpto_limpio.csv"

df.to_csv(
    OUT,
    index=False
)

print("\nEXPORTADO:")
print(OUT)


# =========================================================
# MAPAS AMBA Y RESTO PBA
# SINDESCAPOR POR CATEGORIA OCUPACIONAL
# NATURAL BREAKS K=7 + RANKING LATERAL
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

CSV_PATH = (
    BASE /
    "pipeline" /
    "output" /
    "marts" /
    "renombra_sindesc.csv"
)

OUTPUT_DIR = (
    BASE /
    "pipeline" /
    "output" /
    "mapas_sindesc_amba_pba"
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
# 3. CARGA SHAPE
# =========================================================

print("\n===================================")
print("CARGANDO SHAPE")
print("===================================\n")

gdf = gpd.read_file(SHAPE_PATH)

print(gdf.head())

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
# 5. CARGA CSV SINDESC
# =========================================================

print("\n===================================")
print("CARGANDO CSV")
print("===================================\n")

df = pd.read_csv(CSV_PATH)

print(df.head())

print("\nColumnas:")
print(df.columns.tolist())

# =========================================================
# 6. LIMPIEZA CSV
# =========================================================

df["codigo_departamento"] = (
    df["codigo_departamento"]
    .astype(str)
    .str.replace(".0", "", regex=False)
    .str.zfill(5)
)

df["departamento_limpio"] = (
    df["departamento"]
    .apply(limpiar_texto)
)

# =========================================================
# 7. VARIABLES TOTALES
# =========================================================
#
# EL DENOMINADOR YA ES:
# ocupados_sd
# ocupados_asalariados
# ocupados_cp
# ocupados_patrones
#
# REPRESENTA TOTAL SIN DESCAPOR
# SIN DISTINGUIR SEXO
#
# =========================================================

VARIABLES = {

    "ocupados_sd": {
        "titulo": "Servicio doméstico",
        "archivo": "servicio_domestico"
    },

    "ocupados_asalariados": {
        "titulo": "Asalariados",
        "archivo": "asalariados"
    },

    "ocupados_cp": {
        "titulo": "Cuenta propia",
        "archivo": "cuenta_propia"
    },

    "ocupados_patrones": {
        "titulo": "Patrones",
        "archivo": "patrones"
    }

}

# =========================================================
# 8. MERGE GEO
# =========================================================

print("\n===================================")
print("MERGE")
print("===================================\n")

mapa = gdf.merge(
    df,
    left_on="codigo_geo",
    right_on="codigo_departamento",
    how="left"
)

print("\nRows mapa:", len(mapa))

# =========================================================
# 9. PROYECCION
# =========================================================

print("\n===================================")
print("REPROYECCION")
print("===================================\n")

mapa = mapa.to_crs(22185)

# =========================================================
# 10. GEOMETRIAS
# =========================================================

invalidas = (~mapa.is_valid).sum()

print("\nGeometrias invalidas:", invalidas)

if invalidas > 0:

    print("Corrigiendo geometrias...")

    mapa["geometry"] = (
        mapa.buffer(0)
    )

# =========================================================
# 11. CONFIGURACION AMBA
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
# 12. CABA
# =========================================================

comunas_caba = mapa[
    mapa["nam"].astype(str).str.contains(
        "Comuna",
        case=False,
        na=False
    )
].copy()

# =========================================================
# UNION CABA
# =========================================================

caba_union = gpd.GeoDataFrame(
    {
        "provincia": ["CABA"],
        "departamento": ["CABA"]
    },
    geometry=[comunas_caba.unary_union],
    crs=mapa.crs
)

# sumar variables
for var in VARIABLES.keys():

    caba_union[var] = (
        comunas_caba[var]
        .fillna(0)
        .sum()
    )

# =========================================================
# 13. AMBA PBA
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
# 14. UNION FINAL AMBA
# =========================================================

amba = pd.concat(
    [
        amba_ba,
        caba_union
    ],
    ignore_index=True
)

# =========================================================
# 15. RESTO PBA
# =========================================================

resto_pba = mapa.loc[
    (
        mapa["provincia_id"] == "06"
    )
    &
    (
        ~mapa["departamento_shape"]
        .isin(AMBA_40)
    )
].copy()

# sacar comunas
resto_pba = resto_pba.loc[
    ~resto_pba["nam"].astype(str).str.contains(
        "Comuna",
        case=False,
        na=False
    )
]

# =========================================================
# 16. GENERADOR MAPAS
# =========================================================

def generar_mapa(sub, variable, titulo_variable, region):

    print("\n===================================")
    print(region, variable)
    print("===================================\n")

    sub = sub.copy()

    sub[variable] = (
        pd.to_numeric(
            sub[variable],
            errors="coerce"
        )
        .fillna(0)
    )

    # =====================================================
    # WINSORIZACION
    # =====================================================

    cap = (
        sub[variable]
        .quantile(0.99)
    )

    sub["plot_var"] = (
        sub[variable]
        .clip(upper=cap)
    )

    # =====================================================
    # LABELS
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
                variable
            ]
        ]
        .sort_values(
            variable,
            ascending=False
        )
        .reset_index(drop=True)
    )

    ranking["rank"] = (
        ranking.index + 1
    )

    mitad = int(
        np.ceil(len(ranking) / 2)
    )

    ranking_1 = ranking.iloc[:mitad]

    ranking_2 = ranking.iloc[mitad:]

    # =====================================================
    # FIGURA
    # =====================================================

    fig = plt.figure(
        figsize=(30, 22)
    )

    ax_map = fig.add_axes(
        [0.01, 0.02, 0.67, 0.96]
    )

    ax_rank = fig.add_axes(
        [0.69, 0.02, 0.30, 0.96]
    )

    # =====================================================
    # MAPA
    # =====================================================

    try:

        n_classes = min(
            7,
            sub["plot_var"].nunique()
        )

        sub.plot(
            column="plot_var",
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

        print("ERROR:", e)

        sub.plot(
            color="lightgrey",
            linewidth=0.8,
            edgecolor="black",
            ax=ax_map
        )

    # =====================================================
    # LABELS
    # =====================================================

    for idx, row in sub.iterrows():

        try:

            depto = str(
                row["departamento"]
            )

            valor = int(
                row[variable]
            )

            label = (
                f"{depto}\n"
                f"{valor:,}"
            )

            txt = ax_map.text(
                row["x"],
                row["y"],
                label,
                fontsize=16,
                ha="center",
                va="center",
                color="black",
                fontweight="bold"
            )

            txt.set_path_effects([
                patheffects.withStroke(
                    linewidth=4,
                    foreground="white"
                )
            ])

        except:
            pass

    # =====================================================
    # TITULO
    # =====================================================

    ax_map.set_title(
        f"{region}\n"
        f"{titulo_variable}\n"
        "Personas sin descapor\n"
        "(Natural Breaks — k=7)",
        fontsize=30,
        fontweight="bold",
        pad=25
    )

    ax_map.axis("off")

    # =====================================================
    # RANKING
    # =====================================================

    ax_rank.axis("off")

    ax_rank.text(
        0.02,
        0.99,
        "Ranking departamental",
        fontsize=28,
        fontweight="bold",
        va="top"
    )

    # =====================================================
    # COLUMNA 1
    # =====================================================

    y1 = 0.93

    for idx, row in ranking_1.iterrows():

        texto = (
            f"{int(row['rank']):02d}. "
            f"{row['departamento']} — "
            f"{int(row[variable]):,}"
        )

        ax_rank.text(
            0.02,
            y1,
            texto,
            fontsize=16,
            va="top"
        )

        y1 -= 0.035

    # =====================================================
    # COLUMNA 2
    # =====================================================

    y2 = 0.93

    for idx, row in ranking_2.iterrows():

        texto = (
            f"{int(row['rank']):02d}. "
            f"{row['departamento']} — "
            f"{int(row[variable]):,}"
        )

        ax_rank.text(
            0.52,
            y2,
            texto,
            fontsize=16,
            va="top"
        )

        y2 -= 0.035

    # =====================================================
    # EXPORT
    # =====================================================

    nombre_region = nombre_archivo(region)

    nombre_var = nombre_archivo(
        titulo_variable
    )

    png_out = (
        OUTPUT_DIR /
        f"{nombre_region}_{nombre_var}.png"
    )

    pdf_out = (
        OUTPUT_DIR /
        f"{nombre_region}_{nombre_var}.pdf"
    )

    csv_out = (
        OUTPUT_DIR /
        f"{nombre_region}_{nombre_var}.csv"
    )

    plt.savefig(
        png_out,
        dpi=600,
        bbox_inches="tight"
    )

    plt.savefig(
        pdf_out,
        dpi=600,
        bbox_inches="tight"
    )

    ranking.to_csv(
        csv_out,
        index=False
    )

    plt.close()

    print("EXPORTADO:")
    print(png_out)

# =========================================================
# 17. LOOP VARIABLES
# =========================================================

for variable, meta in VARIABLES.items():

    titulo = meta["titulo"]

    # =====================================================
    # AMBA
    # =====================================================

    generar_mapa(
        amba,
        variable,
        titulo,
        "AMBA"
    )

    # =====================================================
    # RESTO PBA
    # =====================================================

    generar_mapa(
        resto_pba,
        variable,
        titulo,
        "RESTO_PBA"
    )

# =========================================================
# 18. FINAL
# =========================================================

print("\n===================================")
print("FINALIZADO")
print("===================================\n")

print(OUTPUT_DIR)