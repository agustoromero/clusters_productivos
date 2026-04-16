#!/usr/bin/env python3
import csv
import json
import os
from collections import defaultdict

CONFIG_PATH = "pipeline/config/params.json"


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def read_csv(path):
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows):
    if not rows:
        raise RuntimeError(f"Sin filas para escribir en {path}")
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def to_int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def geo_key(row, level_geo):
    if level_geo == "provincia":
        return (to_int(row["provincia_id"]), row["provincia"])
    return (
        to_int(row["provincia_id"]),
        row["provincia"],
        to_int(row["in_departamentos"]),
        row["departamento"],
    )


def build_features(estab_rows, level_geo, level_activity):
    by_cell = defaultdict(lambda: {"empleo": 0.0, "establecimientos": 0.0, "exportadoras": 0.0})
    totals = defaultdict(lambda: {"empleo": 0.0, "establecimientos": 0.0, "exportadoras": 0.0})

    for r in estab_rows:
        g = geo_key(r, level_geo)
        k = (to_int(r["anio"]),) + g + (str(r[level_activity]),)
        by_cell[k]["empleo"] += to_float(r["empleo"])
        by_cell[k]["establecimientos"] += to_float(r["establecimientos"])
        by_cell[k]["exportadoras"] += to_float(r["empresas_exportadoras"])

        tk = (to_int(r["anio"]),) + g
        totals[tk]["empleo"] += to_float(r["empleo"])
        totals[tk]["establecimientos"] += to_float(r["establecimientos"])
        totals[tk]["exportadoras"] += to_float(r["empresas_exportadoras"])

    out = []
    for k, vals in by_cell.items():
        anio = k[0]
        activity = k[-1]
        g = k[1:-1]
        tk = (anio,) + g
        total = totals[tk]

        empleo_total = total["empleo"]
        estab_total = total["establecimientos"]

        row = {
            "anio": anio,
            level_activity: activity,
            "empleo": round(vals["empleo"], 4),
            "establecimientos": round(vals["establecimientos"], 4),
            "exportadoras": round(vals["exportadoras"], 4),
            "empleo_total_geo": round(empleo_total, 4),
            "establecimientos_total_geo": round(estab_total, 4),
            "share_empleo": round(vals["empleo"] / empleo_total, 8) if empleo_total else 0.0,
            "densidad_estab": round(vals["establecimientos"] / vals["empleo"], 8) if vals["empleo"] else 0.0,
            "intensidad_export": round(vals["exportadoras"] / vals["establecimientos"], 8) if vals["establecimientos"] else 0.0
        }

        if level_geo == "provincia":
            row.update({"provincia_id": g[0], "provincia": g[1]})
        else:
            row.update({"provincia_id": g[0], "provincia": g[1], "in_departamentos": g[2], "departamento": g[3]})

        out.append(row)

    return out


def build_gender_features(gender_rows, level_geo, level_activity):
    gsum = defaultdict(lambda: {"mujeres": 0.0, "varones": 0.0, "otro": 0.0})

    for r in gender_rows:
        g = geo_key(r, level_geo)
        k = (to_int(r["anio"]),) + g + (str(r[level_activity]),)
        genero = str(r.get("genero", "")).strip().lower()
        empleo = to_float(r.get("empleo"))

        if genero.startswith("muj"):
            gsum[k]["mujeres"] += empleo
        elif genero.startswith("var"):
            gsum[k]["varones"] += empleo
        else:
            gsum[k]["otro"] += empleo

    out = []
    for k, vals in gsum.items():
        anio = k[0]
        activity = k[-1]
        g = k[1:-1]
        total_bin = vals["mujeres"] + vals["varones"]
        row = {
            "anio": anio,
            level_activity: activity,
            "empleo_mujeres": round(vals["mujeres"], 4),
            "empleo_varones": round(vals["varones"], 4),
            "empleo_otro": round(vals["otro"], 4),
            "ratio_fem": round(vals["mujeres"] / total_bin, 8) if total_bin else 0.0,
            "brecha_genero_abs": round(abs(vals["mujeres"] - vals["varones"]), 4)
        }
        if level_geo == "provincia":
            row.update({"provincia_id": g[0], "provincia": g[1]})
        else:
            row.update({"provincia_id": g[0], "provincia": g[1], "in_departamentos": g[2], "departamento": g[3]})
        out.append(row)
    return out


def export_geo_reference(estab_rows, out_path):
    uniq = {}
    for r in estab_rows:
        key = (r["provincia_id"], r["in_departamentos"])
        uniq[key] = {
            "provincia_id": r["provincia_id"],
            "provincia": r["provincia"],
            "in_departamentos": r["in_departamentos"],
            "departamento": r["departamento"]
        }
    write_csv(out_path, list(uniq.values()))


def main():
    cfg = load_config(CONFIG_PATH)
    os.makedirs(cfg["paths"]["marts_dir"], exist_ok=True)

    estab = read_csv(os.path.join(cfg["paths"]["curated_dir"], "estab_curado.csv"))
    gender = read_csv(os.path.join(cfg["paths"]["curated_dir"], "gender_curado.csv"))

    level_activity = cfg["run"]["level_activity"]

    for level_geo in cfg["run"]["level_geo"]:
        feats = build_features(estab, level_geo, level_activity)
        fpath = os.path.join(cfg["paths"]["marts_dir"], f"features_{level_geo}.csv")
        write_csv(fpath, feats)

        if cfg["run"].get("include_gender_module", False):
            gfeats = build_gender_features(gender, level_geo, level_activity)
            gfpath = os.path.join(cfg["paths"]["marts_dir"], f"features_genero_{level_geo}.csv")
            write_csv(gfpath, gfeats)

    geo_path = os.path.join(cfg["paths"]["marts_dir"], "geo_referencia_para_mapas.csv")
    export_geo_reference(estab, geo_path)
    print("Features OK")


if __name__ == "__main__":
    main()
