#!/usr/bin/env python3
import csv
import json
import os
from collections import Counter
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


def curate_main(rows, years, act_idx):
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

        desc = act_idx.get((row["clae6"], row["clae2"], row["letra"]), {})
        row.update(desc)
        out.append(row)

        dup_key = (row["anio"], row["in_departamentos"], row["clae6"])
        dup_counter[dup_key] += 1

    duplicate_keys = sum(1 for _, c in dup_counter.items() if c > 1)
    return out, duplicate_keys


def curate_gender(rows, years, act_idx):
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

    act_idx = make_activity_index(act_raw)
    estab_cur, estab_dups = curate_main(estab_raw, years, act_idx)
    gender_cur, gender_dups = curate_gender(gender_raw, years, act_idx)

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
            "gender_curado": len(gender_cur)
        },
        "duplicates": {
            "estab_grain_anio_depto_clae6": estab_dups,
            "gender_grain_anio_depto_clae6_genero": gender_dups
        },
        "null_share_estab": {
            "provincia_id": null_share(estab_cur, "provincia_id"),
            "in_departamentos": null_share(estab_cur, "in_departamentos"),
            "clae2": null_share(estab_cur, "clae2")
        }
    }

    qpath = os.path.join(cfg["paths"]["quality_dir"], "quality_ingesta.json")
    with open(qpath, "w", encoding="utf-8") as f:
        json.dump(quality, f, ensure_ascii=False, indent=2)

    print(f"Curado OK: {estab_out}, {gender_out}")
    print(f"Reporte calidad: {qpath}")


if __name__ == "__main__":
    main()
