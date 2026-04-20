#!/usr/bin/env python3
import csv
import json
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
    """Create mapping from sector descriptions to CLAE 2-digit codes"""
    try:
        df = pd.read_excel(sector_dim_path)
        # Columns: 'ClaNAE 2010', 'DESCRIPCIÓN DE LA ACTIVIDAD', 'CIIU-4'
        mapping = defaultdict(list)
        for _, row in df.iterrows():
            desc = normalize_text(row.get('DESCRIPCIÓN DE LA ACTIVIDAD', '')).lower()
            clae_full = normalize_text(row.get('ClaNAE 2010', ''))
            
            # Extract 2-digit CLAE code
            if len(clae_full) >= 2 and clae_full[:2].isdigit():
                clae2 = clae_full[:2]
                if desc:
                    mapping[desc].append(clae2)
        
        return mapping
    except Exception as e:
        print(f"Error loading sector dimension: {e}")
        return {}


def match_sector_to_clae2(sector_desc, sector_mapping):
    """Match sector description to CLAE 2-digit code using frequency"""
    desc_lower = normalize_text(sector_desc).lower()
    candidates = sector_mapping.get(desc_lower, [])
    if not candidates:
        # Try partial matching
        for key in sector_mapping.keys():
            if desc_lower in key or key in desc_lower:
                candidates = sector_mapping[key]
                break
    
    if not candidates:
        return None
    
    # Return most frequent CLAE2 code
    return Counter(candidates).most_common(1)[0][0]


def load_and_aggregate_salarios(series_path, ipc_data, sector_mapping, years):
    """Load series data, deflate salaries, and aggregate by province-sector-year"""
    aggregated = defaultdict(lambda: {'salarios_deflactados': [], 'meses_completos': 0})
    
    try:
        # Read CSV with semicolon delimiter and latin-1 encoding
        df = pd.read_csv(series_path, sep=';', encoding='latin-1')
        print(f"Loaded {len(df)} rows from series data")
        print(f"Sector mapping has {len(sector_mapping)} entries")
        print("Sample sector keys from mapping:", list(sector_mapping.keys())[:5])
        
        processed = 0
        matched = 0
        sample_sectors = set()
        
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
            sample_sectors.add(sector_desc)
            
            processed += 1
            
            if not all([id_prov, sector_desc, salario > 0]):
                continue
            
            # Match to CLAE2
            clae2 = match_sector_to_clae2(sector_desc, sector_mapping)
            if not clae2:
                continue
            
            matched += 1
            
            # Deflate salary using IPC
            periodo_key = f"{anio:04d}{mes:02d}"
            fecha = pd.to_datetime(periodo_key, format='%Y%m')
            ipc_value = ipc_data.get(fecha, {}).get('ipc', 100.0)  # Default to 100 if not found
            
            salario_deflactado = salario * (100.0 / ipc_value)
            
            # Aggregate by province-sector-year
            key = (id_prov, clae2, anio)
            aggregated[key]['salarios_deflactados'].append(salario_deflactado)
            aggregated[key]['meses_completos'] += 1
        
        print(f"Processed {processed} rows, matched {matched} sectors")
        print("Sample sectors from data:", list(sample_sectors)[:10])
    
    except Exception as e:
        print(f"Error processing series data: {e}")
    
    # Calculate annual averages (only if >=6 months)
    final_data = {}
    for key, vals in aggregated.items():
        if vals['meses_completos'] >= 6:
            salarios = vals['salarios_deflactados']
            final_data[key] = {
                'salario_promedio_anual_deflactado': sum(salarios) / len(salarios),
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
        clae2 = row["clae2"]
        sal_key = (prov_id, clae2, anio)
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
        clae2 = row["clae2"]
        sal_key = (prov_id, clae2, anio)
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
            "series_raw": len(pd.read_csv(cfg["paths"]["raw_series"], sep=';', encoding='latin-1')) if os.path.exists(cfg["paths"]["raw_series"]) else 0
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
        }
    }

    qpath = os.path.join(cfg["paths"]["quality_dir"], "quality_ingesta.json")
    with open(qpath, "w", encoding="utf-8") as f:
        json.dump(quality, f, ensure_ascii=False, indent=2)

    print(f"Curado OK: {estab_out}, {gender_out}")
    print(f"Reporte calidad: {qpath}")
    print(f"Salarios agregados: {len(salarios_agg)} registros válidos")


if __name__ == "__main__":
    main()
