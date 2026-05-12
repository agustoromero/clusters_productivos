# =========================================================
# RENOMBRE + MAPAS
# SINDESCAPOR POR CATEGORIA OCUPACIONAL
# VERSION ESTABLE Y OPTIMIZADA
# =========================================================
import gc
from pathlib import Path
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib import patheffects
from unidecode import unidecode
import numpy as np
import warnings
import gc

warnings.filterwarnings("ignore")

# =========================================================
# 1. PATHS
# =========================================================

BASE = Path(
    r"C:\Users\agusr\Desktop\Instituto Argentina Grande\clusters_productivos\clusters_productivos"
)

CSV_ORIGINAL = (
    BASE /
    "sindesc_dpto.csv"
)

SHAPE_PATH = (
    BASE /
    "departamento" /
    "departamentoPolygon.shp"
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
# 2. EXISTENCIA ARCHIVOS
# =========================================================

if not CSV_ORIGINAL.exists():

    raise FileNotFoundError(
        f"\nNO SE ENCONTRO EL CSV ORIGINAL\n"
        f"Buscar en:\n{CSV_ORIGINAL}"
    )

if not SHAPE_PATH.exists():

    raise FileNotFoundError(
        f"\nNO SE ENCONTRO EL SHAPEFILE\n"
        f"Buscar en:\n{SHAPE_PATH}"
    )

# =========================================================
# 3. CARGA CSV
# =========================================================

print("\n===================================")
print("CARGANDO CSV ORIGINAL")
print("===================================\n")

df = pd.read_csv(CSV_ORIGINAL)

print(df.head())

print("\nCOLUMNAS:")
print(df.columns.tolist())

# =========================================================
# 4. LIMPIEZA COLUMNAS
# =========================================================

df = df.loc[
    :,
    ~df.columns.str.contains("^Unnamed")
]

# =========================================================
# 5. RENOMBRE EXPLICITO
# =========================================================

rename_dict = {

    "REDCODE": "codigo_departamento",
    "Departamento": "departamento",

    # servicio domestico
    "total_sd_1": "total_sd",
    "sindesc_sd_2": "sindesc_sd",
    "varones_sd_3": "varones_sd",
    "mujeres_sd_4": "mujeres_sd",
    "tasa_sd_5": "tasa_sd",
    "pct_varones_sd_6": "pct_varones_sd",
    "pct_mujeres_sd_7": "pct_mujeres_sd",

    # asalariados
    "total_asal_8": "total_asalariados",
    "sindesc_asal_9": "sindesc_asalariados",
    "varones_asal_10": "varones_asalariados",
    "mujeres_asal_11": "mujeres_asalariadas",
    "tasa_asal_12": "tasa_asalariados",
    "pct_varones_asal_13": "pct_varones_asalariados",
    "pct_mujeres_asal_14": "pct_mujeres_asalariadas",

    # cuenta propia
    "total_cp_15": "total_cp",
    "sindesc_cp_16": "sindesc_cp",
    "varones_cp_17": "varones_cp",
    "mujeres_cp_18": "mujeres_cp",
    "tasa_cp_19": "tasa_cp",
    "pct_varones_cp_20": "pct_varones_cp",
    "pct_mujeres_cp_21": "pct_mujeres_cp",

    # patrones
    "total_pat_22": "total_patrones",
    "sindesc_pat_23": "sindesc_patrones",
    "varones_pat_24": "varones_patrones",
    "mujeres_pat_25": "mujeres_patrones",
    "tasa_pat_26": "tasa_patrones",
    "pct_varones_pat_27": "pct_varones_patrones",
    "pct_mujeres_pat_28": "pct_mujeres_patrones",

    # brechas
    "brecha_pp_sd_29": "brecha_pp_sd",
    "brecha_ratio_sd_30": "brecha_ratio_sd",

    "brecha_pp_asal_31": "brecha_pp_asalariados",
    "brecha_ratio_asal_32": "brecha_ratio_asalariados",

    "brecha_pp_cp_33": "brecha_pp_cp",
    "brecha_ratio_cp_34": "brecha_ratio_cp",

    "brecha_pp_pat_35": "brecha_pp_patrones",
    "brecha_ratio_pat_36": "brecha_ratio_patrones"
}

df = df.rename(columns=rename_dict)

# =========================================================
# 6. NORMALIZAR CODIGOS
# =========================================================

df["codigo_departamento"] = (
    df["codigo_departamento"]
    .astype(str)
    .str.replace(".0", "", regex=False)
    .str.zfill(5)
)

# =========================================================
# 7. VARIABLES NUMERICAS
# =========================================================

for col in df.columns:

    if col not in [
        "codigo_departamento",
        "departamento"
    ]:

        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        )

# =========================================================
# 8. FUNCIONES
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

    return (
        x.lower()
         .replace(" ", "_")
    )

# =========================================================
# 9. CARGA SHAPE
# =========================================================

print("\n===================================")
print("CARGANDO SHAPE")
print("===================================\n")

gdf = gpd.read_file(SHAPE_PATH)

# =========================================================
# 10. LIMPIEZA SHAPE
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
# 11. LIMPIEZA CSV
# =========================================================

df["departamento_limpio"] = (
    df["departamento"]
    .apply(limpiar_texto)
)

# =========================================================
# 12. MERGE
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

print("Rows:", len(mapa))

# =========================================================
# 13. PROYECCION
# =========================================================

mapa = mapa.to_crs(22185)

# =========================================================
# 14. GEOMETRIAS INVALIDAS
# =========================================================

invalidas = (~mapa.is_valid).sum()

print("\nGeometrias invalidas:", invalidas)

if invalidas > 0:

    mapa["geometry"] = mapa.buffer(0)

# =========================================================
# 15. AMBA
# =========================================================

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
# 16. CABA
# =========================================================

comunas_caba = mapa[
    mapa["nam"].astype(str).str.contains(
        "Comuna",
        case=False,
        na=False
    )
].copy()

caba_union = gpd.GeoDataFrame(
    {
        "departamento": ["CABA"]
    },
    geometry=[comunas_caba.unary_union],
    crs=mapa.crs
)

# sumar variables
columnas_sumar = [

    "sindesc_sd",
    "sindesc_asalariados",
    "sindesc_cp",
    "sindesc_patrones",

    "total_sd",
    "total_asalariados",
    "total_cp",
    "total_patrones"
]

for col in columnas_sumar:

    caba_union[col] = (
        comunas_caba[col]
        .fillna(0)
        .sum()
    )

# recalcular tasas
caba_union["tasa_sd"] = (
    caba_union["sindesc_sd"] /
    caba_union["total_sd"]
) * 100

caba_union["tasa_asalariados"] = (
    caba_union["sindesc_asalariados"] /
    caba_union["total_asalariados"]
) * 100

caba_union["tasa_cp"] = (
    caba_union["sindesc_cp"] /
    caba_union["total_cp"]
) * 100

caba_union["tasa_patrones"] = (
    caba_union["sindesc_patrones"] /
    caba_union["total_patrones"]
) * 100

# =========================================================
# 17. AMBA BA
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
# 18. UNION FINAL AMBA
# =========================================================

amba = gpd.GeoDataFrame(
    pd.concat(
        [
            amba_ba,
            caba_union
        ],
        ignore_index=True
    ),
    crs=mapa.crs
)

# =========================================================
# 19. RESTO PBA
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

resto_pba = resto_pba.loc[
    ~resto_pba["nam"].astype(str).str.contains(
        "Comuna",
        case=False,
        na=False
    )
]

# =========================================================
# 20. VARIABLES
# =========================================================

VARIABLES = {

    "sindesc_sd": {
        "titulo": "Servicio doméstico",
        "tasa": "tasa_sd"
    },

    "sindesc_asalariados": {
        "titulo": "Asalariados",
        "tasa": "tasa_asalariados"
    },

    "sindesc_cp": {
        "titulo": "Cuenta propia",
        "tasa": "tasa_cp"
    },

    "sindesc_patrones": {
        "titulo": "Patrones",
        "tasa": "tasa_patrones"
    }
}

# =========================================================
# 21. GENERADOR MAPAS
# =========================================================
def generar_mapa(
    sub,
    variable,
    titulo_variable,
    tasa_col,
    region
):

    print("\n===================================")
    print(region, variable)
    print("===================================\n")

    sub = sub.copy()

    # =====================================================
    # NUMERICAS
    # =====================================================

    sub[variable] = pd.to_numeric(
        sub[variable],
        errors="coerce"
    ).fillna(0)

    sub[tasa_col] = pd.to_numeric(
        sub[tasa_col],
        errors="coerce"
    ).fillna(0)

    # =====================================================
    # FILTRAR
    # =====================================================

    sub = sub.loc[
        sub[variable] > 0
    ].copy()

    if len(sub) == 0:

        print("SIN DATOS")
        return

    # =====================================================
    # WINSORIZACION
    # =====================================================

    cap = sub[variable].quantile(0.99)

    sub["plot_var"] = (
        sub[variable]
        .clip(upper=cap)
    )

    # =====================================================
    # LABEL POINTS
    # =====================================================

    sub["label_point"] = (
        sub.representative_point()
    )

    sub["x"] = sub["label_point"].x
    sub["y"] = sub["label_point"].y

    # =====================================================
    # RANKING
    # =====================================================

    ranking = (
        sub[
            [
                "departamento",
                variable,
                tasa_col
            ]
        ]
        .sort_values(
            tasa_col,
            ascending=False
        )
        .reset_index(drop=True)
    )

    ranking = ranking.rename(
        columns={
            tasa_col: "tasa"
        }
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
        figsize=(18, 12)
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
            linewidth=0.3,
            edgecolor="black",
            legend=True,
            scheme="naturalbreaks",
            k=max(n_classes, 2),
            ax=ax_map
        )

    except Exception as e:

        print("ERROR:", e)

        sub.plot(
            color="lightgrey",
            linewidth=0.3,
            edgecolor="black",
            ax=ax_map
        )

    # =====================================================
    # LABELS
    # =====================================================

    for _, row in sub.iterrows():

        try:

            valor = int(row[variable])

            valor_txt = (
                f"{valor:,}"
                .replace(",", ".")
            )

            tasa = row[tasa_col]

            label = (
                f"{row['departamento']}\n"
                f"{valor_txt}\n"
                f"{tasa:.1f}%"
            )

            txt = ax_map.text(
                row["x"],
                row["y"],
                label,
                fontsize=7,
                ha="center",
                va="center",
                color="black",
                fontweight="bold"
            )

            txt.set_path_effects([
                patheffects.withStroke(
                    linewidth=1.5,
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
        f"Personas sin descuento jubilatorio",
        fontsize=20,
        fontweight="bold",
        pad=15
    )

    ax_map.axis("off")

    # =====================================================
    # RANKING
    # =====================================================

    ax_rank.axis("off")

    ax_rank.text(
        0.02,
        0.99,
        "Ranking por informalidad",
        fontsize=18,
        fontweight="bold",
        va="top"
    )

    y1 = 0.93

    for _, row in ranking_1.iterrows():

        texto = (
            f"{int(row['rank']):02d}. "
            f"{row['departamento']} — "
            f"{row['tasa']:.1f}%"
        )

        ax_rank.text(
            0.02,
            y1,
            texto,
            fontsize=9,
            va="top"
        )

        y1 -= 0.030

    y2 = 0.93

    for _, row in ranking_2.iterrows():

        texto = (
            f"{int(row['rank']):02d}. "
            f"{row['departamento']} — "
            f"{row['tasa']:.1f}%"
        )

        ax_rank.text(
            0.52,
            y2,
            texto,
            fontsize=9,
            va="top"
        )

        y2 -= 0.030

    # =====================================================
    # EXPORT
    # =====================================================
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

    # =====================================================
    # OPTIMIZACION MEMORIA
    # =====================================================

    plt.subplots_adjust(
        left=0.01,
        right=0.99,
        top=0.95,
        bottom=0.02
    )

    # PNG liviano
    fig.savefig(
        png_out,
        dpi=140,
        bbox_inches="tight",
        facecolor="white"
    )

    # PDF vectorial
    fig.savefig(
        pdf_out,
        bbox_inches="tight",
        facecolor="white"
    )

    ranking.to_csv(
        csv_out,
        index=False
    )

    # cerrar figura explicitamente
    plt.close(fig)

    # liberar memoria
    gc.collect()

    print("\nEXPORTADO:")
    print(png_out)

# =========================================================
# 22. LOOP MAPAS
# =========================================================

for variable, meta in VARIABLES.items():

    titulo = meta["titulo"]

    tasa_col = meta["tasa"]

    # =====================================================
    # AMBA
    # =====================================================

    generar_mapa(
        amba,
        variable,
        titulo,
        tasa_col,
        "AMBA"
    )

    # =====================================================
    # RESTO PBA
    # =====================================================

    generar_mapa(
        resto_pba,
        variable,
        titulo,
        tasa_col,
        "RESTO_PBA"
    )

# =========================================================
# 23. FINAL
# =========================================================

print("\n===================================")
print("FINALIZADO")
print("===================================\n")

print(OUTPUT_DIR)