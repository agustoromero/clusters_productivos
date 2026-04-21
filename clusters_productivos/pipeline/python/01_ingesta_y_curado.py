#!/usr/bin/env python3
import csv
import json
import math
import os
import pandas as pd
from collections import Counter, defaultdict
from datetime import datetime

CONFIG_PATH = "pipeline/config/params.json"


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def read_csv(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def normalize_text(value):
    if value is None:
        return ""
    return str(value).strip()


def to_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def sanitize_for_json(obj):
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def ensure_dirs(cfg):
    for key in ["curated_dir", "quality_dir"]:
        os.makedirs(cfg["paths"][key], exist_ok=True)


def load_ipc_data(ipc_path):
    """Load IPC data for deflation (base 100 = Dec 2016)"""
    try:
        df = pd.read_excel(ipc_path)
        # Columns: t, Fecha, Año, Mes, IPC, INF
        df['periodo'] = df['Año'] * 100 + df['Mes']
        df['fecha'] = pd.to_datetime(df['periodo'], format='%Y%m')
        return df.set_index('fecha')[['IPC', 'INF']].rename(columns={'IPC': 'ipc', 'INF': 'inf'}).to_dict('index')
    except Exception as e:
        print(f"Error loading IPC: {e}")
        return {}


def create_sector_mapping(sector_dim_path):
    """Create mapping from sector descriptions to CLAE letters (most aggregated level)"""
    # Manual mapping based on standard sector classifications
    manual_mapping = {
        'agricultura, ganaderia y pesca': 'A',
        'comercio': 'G',  # Comercio al por mayor y menor
        'construccion': 'F',  # Construcción
        'electricidad, gas y agua': 'D',  # Suministro de electricidad y gas
        'explotacion de minas y canteras': 'B',  # Actividades de extracción
        'industria manufacturera': 'C',  # Industria manufacturera
        'servicios': 'N',  # Actividades administrativas y servicios de apoyo
        'sin rama': None
    }
    
    return manual_mapping


def match_sector_to_clae2(sector_desc, sector_mapping):
    """Match sector description to CLAE 2-digit code"""
    desc_lower = normalize_text(sector_desc).lower()
    return sector_mapping.get(desc_lower)


def load_and_aggregate_salarios(series_path, ipc_data, sector_mapping, years):
    """Load series data, deflate salaries, and aggregate by province-sector-year with employment-weighted average"""
    aggregated = defaultdict(lambda: {'salarios_ponderados': 0.0, 'empleo_total': 0.0, 'meses_completos': 0})
    
    try:
        # Read CSV with semicolon delimiter and latin-1 encoding
        df = pd.read_csv(series_path, sep=';', encoding='latin-1')
        print(f"Loaded {len(df)} rows from series data")
        print(f"Sector mapping has {len(sector_mapping)} entries")
        
        processed = 0
        matched = 0
        
        for _, row in df.iterrows():
            periodo_str = str(row.get('Periodo', '')).strip()
            if len(periodo_str) != 6:
                continue
                
            anio = int(periodo_str[:4])
            mes = int(periodo_str[4:])
            
            if anio not in years:
                continue
                
            id_prov = to_int(row.get('id_prov'))
            sector_desc = normalize_text(row.get('Sector', ''))
            empleo = to_float(row.get('Empleo'))
            salario = to_float(row.get('Salario'))
            
            processed += 1
            
            if not all([id_prov, sector_desc, salario > 0]):
                continue
            
            # Match to CLAE letter
            letra = match_sector_to_clae2(sector_desc, sector_mapping)
            if not letra:
                continue
            
            matched += 1
            
            # Deflate salary using IPC
            periodo_key = f"{anio:04d}{mes:02d}"
            fecha = pd.to_datetime(periodo_key, format='%Y%m')
            ipc_value = ipc_data.get(fecha, {}).get('ipc', 100.0)  # Default to 100 if not found
            
            salario_deflactado = salario * (100.0 / ipc_value)
            
            # Aggregate by province-sector-year with employment weighting
            key = (id_prov, letra, anio)
            aggregated[key]['salarios_ponderados'] += salario_deflactado * empleo
            aggregated[key]['empleo_total'] += empleo
            aggregated[key]['meses_completos'] += 1
        
        print(f"Processed {processed} rows, matched {matched} sectors")
    
    except Exception as e:
        print(f"Error processing series data: {e}")
    
    # Calculate employment-weighted annual averages (only if >=6 months and employment > 0)
    final_data = {}
    for key, vals in aggregated.items():
        if vals['meses_completos'] >= 6 and vals['empleo_total'] > 0:
            salario_promedio_ponderado = vals['salarios_ponderados'] / vals['empleo_total']
            final_data[key] = {
                'salario_promedio_anual_deflactado': salario_promedio_ponderado,
                'meses_completos': vals['meses_completos']
            }
    
    print(f"Final aggregated data: {len(final_data)} valid records")
    return final_data


def make_activity_index(rows):
    idx = {}
    for r in rows:
        key = (normalize_text(r.get("clae6")), normalize_text(r.get("clae2")), normalize_text(r.get("letra")))
        idx[key] = {
            "clae6_desc": normalize_text(r.get("clae6_desc")),
            "clae2_desc": normalize_text(r.get("clae2_desc")),
            "letra_desc": normalize_text(r.get("letra_desc"))
        }
    return idx


def curate_main(rows, years, act_idx, salarios_agg):
    out = []
    dup_counter = Counter()
    for r in rows:
        anio = to_int(r.get("anio"))
        if anio not in years:
            continue

        row = {
            "anio": anio,
            "in_departamentos": to_int(r.get("in_departamentos")),
            "departamento": normalize_text(r.get("departamento")),
            "provincia_id": to_int(r.get("provincia_id")),
            "provincia": normalize_text(r.get("provincia")),
            "clae6": normalize_text(r.get("clae6")),
            "clae2": normalize_text(r.get("clae2")),
            "letra": normalize_text(r.get("letra")),
            "empleo": to_float(r.get("Empleo")),
            "establecimientos": to_float(r.get("Establecimientos")),
            "empresas_exportadoras": to_float(r.get("empresas_exportadoras"))
        }

        # Add aggregated salary data if available
        prov_id = row["provincia_id"]
        letra = row["letra"]
        sal_key = (prov_id, letra, anio)
        if sal_key in salarios_agg:
            row.update(salarios_agg[sal_key])
        else:
            row["salario_promedio_anual_deflactado"] = 0.0
            row["meses_completos"] = 0

        desc = act_idx.get((row["clae6"], row["clae2"], row["letra"]), {})
        row.update(desc)
        out.append(row)

        dup_key = (row["anio"], row["in_departamentos"], row["clae6"])
        dup_counter[dup_key] += 1

    duplicate_keys = sum(1 for _, c in dup_counter.items() if c > 1)
    return out, duplicate_keys


def curate_gender(rows, years, act_idx, salarios_agg):
    out = []
    dup_counter = Counter()
    for r in rows:
        anio = to_int(r.get("anio"))
        if anio not in years:
            continue

        row = {
            "anio": anio,
            "in_departamentos": to_int(r.get("in_departamentos")),
            "departamento": normalize_text(r.get("departamento")),
            "provincia_id": to_int(r.get("provincia_id")),
            "provincia": normalize_text(r.get("provincia")),
            "clae6": normalize_text(r.get("clae6")),
            "clae2": normalize_text(r.get("clae2")),
            "letra": normalize_text(r.get("letra")),
            "genero": normalize_text(r.get("genero")),
            "empleo": to_float(r.get("Empleo")),
            "establecimientos": to_float(r.get("Establecimientos")),
            "empresas_exportadoras": to_float(r.get("empresas_exportadoras"))
        }
        
        # Add aggregated salary data if available
        prov_id = row["provincia_id"]
        letra = row["letra"]
        sal_key = (prov_id, letra, anio)
        if sal_key in salarios_agg:
            row.update(salarios_agg[sal_key])
        else:
            row["salario_promedio_anual_deflactado"] = 0.0
            row["meses_completos"] = 0
        
        desc = act_idx.get((row["clae6"], row["clae2"], row["letra"]), {})
        row.update(desc)
        out.append(row)

        dup_key = (row["anio"], row["in_departamentos"], row["clae6"], row["genero"])
        dup_counter[dup_key] += 1

    duplicate_keys = sum(1 for _, c in dup_counter.items() if c > 1)
    return out, duplicate_keys


def write_csv(path, rows):
    if not rows:
        raise RuntimeError(f"Sin filas para escribir en {path}")
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def null_share(rows, col):
    if not rows:
        return 0.0
    n = len(rows)
    nulls = 0
    for r in rows:
        v = r.get(col)
        if v in (None, ""):
            nulls += 1
    return round(nulls / n, 6)


def main():
    cfg = load_config(CONFIG_PATH)
    ensure_dirs(cfg)

    years = set(cfg["run"]["years"])
    estab_raw = read_csv(cfg["paths"]["raw_estab"])
    gender_raw = read_csv(cfg["paths"]["raw_gender"])
    act_raw = read_csv(cfg["paths"]["dim_activity"])

    # Load new data sources
    ipc_data = load_ipc_data(cfg["paths"]["raw_ipc"])
    sector_mapping = create_sector_mapping(cfg["paths"]["dim_sector"])
    salarios_agg = load_and_aggregate_salarios(cfg["paths"]["raw_series"], ipc_data, sector_mapping, years)

    act_idx = make_activity_index(act_raw)
    estab_cur, estab_dups = curate_main(estab_raw, years, act_idx, salarios_agg)
    gender_cur, gender_dups = curate_gender(gender_raw, years, act_idx, salarios_agg)

    estab_out = os.path.join(cfg["paths"]["curated_dir"], "estab_curado.csv")
    gender_out = os.path.join(cfg["paths"]["curated_dir"], "gender_curado.csv")
    write_csv(estab_out, estab_cur)
    write_csv(gender_out, gender_cur)

    quality = {
        "run_timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "years": sorted(list(years)),
        "counts": {
            "estab_raw": len(estab_raw),
            "estab_curado": len(estab_cur),
            "gender_raw": len(gender_raw),
            "gender_curado": len(gender_cur),
            "series_raw": len(pd.read_csv(cfg["paths"]["raw_series"], sep=';', encoding='latin-1')) if os.path.exists(cfg["paths"]["raw_series"]) else 0,
            "salarios_agregados": len(salarios_agg)
        },
        "duplicates": {
            "estab_grain_anio_depto_clae6": estab_dups,
            "gender_grain_anio_depto_clae6_genero": gender_dups
        },
        "null_share_estab": {
            "provincia_id": null_share(estab_cur, "provincia_id"),
            "in_departamentos": null_share(estab_cur, "in_departamentos"),
            "clae2": null_share(estab_cur, "clae2"),
            "salario_promedio_anual_deflactado": null_share(estab_cur, "salario_promedio_anual_deflactado")
        },
        "salarios_coverage": {
            "provincias_con_salarios": len(set(k[0] for k in salarios_agg.keys())),
            "sectores_con_salarios": len(set(k[1] for k in salarios_agg.keys())),
            "años_con_salarios": len(set(k[2] for k in salarios_agg.keys())),
            "total_provincia_sector_año": len(salarios_agg),
            "ipc_coverage_years": sorted(list(set(fecha.year for fecha in ipc_data.keys()))) if ipc_data else [],
            "deflactacion_stats": {
                "ipc_min": min(ipc_data.values(), key=lambda x: x['ipc'])['ipc'] if ipc_data else None,
                "ipc_max": max(ipc_data.values(), key=lambda x: x['ipc'])['ipc'] if ipc_data else None,
                "ipc_mean": (
                    sum(v["ipc"] for v in ipc_data.values() if not pd.isna(v["ipc"])) /
                    len([v for v in ipc_data.values() if not pd.isna(v["ipc"])])
                ) if ipc_data and any(not pd.isna(v["ipc"]) for v in ipc_data.values()) else None,
                "salarios_deflactados_count": len([v for v in salarios_agg.values() if v['salario_promedio_anual_deflactado'] > 0]),
                "salarios_no_deflactados": len([v for v in salarios_agg.values() if v['salario_promedio_anual_deflactado'] == 0])
            }
        }
    }

    qpath = os.path.join(cfg["paths"]["quality_dir"], "quality_ingesta.json")
    with open(qpath, "w", encoding="utf-8") as f:
        json.dump(sanitize_for_json(quality), f, ensure_ascii=False, indent=2, allow_nan=False)

    print(f"Curado OK: {estab_out}, {gender_out}")
    print(f"Reporte calidad: {qpath}")
    print(f"Salarios agregados: {len(salarios_agg)} registros válidos")


if __name__ == "__main__":
    main()
