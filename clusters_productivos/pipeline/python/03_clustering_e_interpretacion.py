#!/usr/bin/env python3
import csv
import json
import math
import os
import random
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


def euclidean(a, b):
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def median(values):
    x = sorted(values)
    n = len(x)
    if n == 0:
        return 0.0
    m = n // 2
    if n % 2 == 1:
        return x[m]
    return (x[m - 1] + x[m]) / 2


def mad(values, med):
    return median([abs(v - med) for v in values])


def scale_matrix(rows, vars_list, method):
    cols = {v: [to_float(r[v]) for r in rows] for v in vars_list}
    scaled_cols = {}

    for v in vars_list:
        values = cols[v]
        if method == "robust":
            med = median(values)
            m = mad(values, med)
            den = m if m > 1e-9 else 1.0
            scaled_cols[v] = [(x - med) / den for x in values]
        else:
            mean = sum(values) / len(values) if values else 0.0
            var = sum((x - mean) ** 2 for x in values) / len(values) if values else 0.0
            std = math.sqrt(var) if var > 1e-12 else 1.0
            scaled_cols[v] = [(x - mean) / std for x in values]

    matrix = []
    for i in range(len(rows)):
        matrix.append([scaled_cols[v][i] for v in vars_list])
    return matrix


def kmeans(matrix, k, seed=123, max_iter=100):
    rnd = random.Random(seed)
    n = len(matrix)
    if k > n:
        raise ValueError("k no puede ser mayor al número de observaciones")

    centers = [matrix[i][:] for i in rnd.sample(range(n), k)]
    labels = [0] * n

    for _ in range(max_iter):
        changed = False
        for i, point in enumerate(matrix):
            best = min(range(k), key=lambda c: euclidean(point, centers[c]))
            if labels[i] != best:
                labels[i] = best
                changed = True

        new_centers = [[0.0] * len(matrix[0]) for _ in range(k)]
        counts = [0] * k
        for lbl, point in zip(labels, matrix):
            counts[lbl] += 1
            for j, val in enumerate(point):
                new_centers[lbl][j] += val

        for c in range(k):
            if counts[c] == 0:
                new_centers[c] = matrix[rnd.randrange(n)][:]
            else:
                new_centers[c] = [v / counts[c] for v in new_centers[c]]

        centers = new_centers
        if not changed:
            break

    return labels


def silhouette_score(matrix, labels):
    n = len(matrix)
    if n < 3:
        return -1.0

    clusters = defaultdict(list)
    for idx, lbl in enumerate(labels):
        clusters[lbl].append(idx)

    if len(clusters) < 2:
        return -1.0

    svals = []
    for i in range(n):
        own = labels[i]
        own_idx = clusters[own]

        if len(own_idx) <= 1:
            a = 0.0
        else:
            a = sum(euclidean(matrix[i], matrix[j]) for j in own_idx if j != i) / (len(own_idx) - 1)

        b = float("inf")
        for cl, idxs in clusters.items():
            if cl == own:
                continue
            d = sum(euclidean(matrix[i], matrix[j]) for j in idxs) / len(idxs)
            if d < b:
                b = d

        denom = max(a, b)
        s = (b - a) / denom if denom > 0 else 0.0
        svals.append(s)

    return sum(svals) / len(svals)


def aggregate_for_clustering(feature_rows, level_geo):
    geo_stats = defaultdict(lambda: {
        "empleo_total": 0.0,
        "establecimientos_total": 0.0,
        "exportadoras_total": 0.0,
        "hhi": 0.0,
        "diversidad": 0,
        "max_share": 0.0,
        "intensidad_export_sum": 0.0,
        "densidad_estab_sum": 0.0,
        "salario_ponderado_sum": 0.0,
        "empleo_con_salario": 0.0,
        "n_actividades": 0,
        "top_clae2": None,
        "top_share": -1.0
    })

    for r in feature_rows:
        if level_geo == "provincia":
            g = (to_int(r["provincia_id"]), r["provincia"])
        else:
            g = (to_int(r["provincia_id"]), r["provincia"], to_int(r["in_departamentos"]), r["departamento"])

        share = to_float(r["share_empleo"])
        empleo = to_float(r["empleo"])
        estab = to_float(r["establecimientos"])
        expo = to_float(r["exportadoras"])
        intensidad = to_float(r["intensidad_export"])
        densidad = to_float(r["densidad_estab"])
        salario = to_float(r["salario_promedio_anual_deflactado"])
        clae2 = str(r["clae2"])

        s = geo_stats[g]
        s["empleo_total"] += empleo
        s["establecimientos_total"] += estab
        s["exportadoras_total"] += expo
        s["hhi"] += share * share
        s["max_share"] = max(s["max_share"], share)
        s["intensidad_export_sum"] += intensidad
        s["densidad_estab_sum"] += densidad
        s["n_actividades"] += 1
        if empleo > 0:
            s["diversidad"] += 1
        if share > s["top_share"]:
            s["top_share"] = share
            s["top_clae2"] = clae2
        
        # Agregar salario ponderado por empleo
        if salario > 0 and empleo > 0:
            s["salario_ponderado_sum"] += salario * empleo
            s["empleo_con_salario"] += empleo

    out = []
    for g, s in geo_stats.items():
        n = s["n_actividades"] if s["n_actividades"] else 1
        salario_promedio_ponderado = 0.0
        if s["empleo_con_salario"] > 0:
            salario_promedio_ponderado = s["salario_ponderado_sum"] / s["empleo_con_salario"]
        
        row = {
            "empleo_total": round(s["empleo_total"], 4),
            "establecimientos_total": round(s["establecimientos_total"], 4),
            "exportadoras_total": round(s["exportadoras_total"], 4),
            "hhi": round(s["hhi"], 8),
            "diversidad_sectorial": s["diversidad"],
            "max_share_sector": round(s["max_share"], 8),
            "intensidad_export_prom": round(s["intensidad_export_sum"] / n, 8),
            "densidad_estab_prom": round(s["densidad_estab_sum"] / n, 8),
            "salario_promedio_ponderado": round(salario_promedio_ponderado, 2),
            "top_clae2": s["top_clae2"]
        }
        if level_geo == "provincia":
            row.update({"provincia_id": g[0], "provincia": g[1]})
        else:
            row.update({"provincia_id": g[0], "provincia": g[1], "in_departamentos": g[2], "departamento": g[3]})
        out.append(row)

    return out


def profile_clusters(rows, feature_vars):
    acc = defaultdict(lambda: {"count": 0, **{v: 0.0 for v in feature_vars}})
    for r in rows:
        c = int(r["cluster"])
        acc[c]["count"] += 1
        for v in feature_vars:
            acc[c][v] += to_float(r[v])

    out = []
    for c, stats in sorted(acc.items()):
        n = stats["count"]
        row = {"cluster": c, "n_territorios": n}
        for v in feature_vars:
            row[v + "_prom"] = round(stats[v] / n, 6) if n else 0.0
        out.append(row)
    return out


def main():
    cfg = load_config(CONFIG_PATH)
    os.makedirs(cfg["paths"]["clusters_dir"], exist_ok=True)

    feature_vars = [
        "empleo_total",
        "establecimientos_total",
        "exportadoras_total",
        "hhi",
        "diversidad_sectorial",
        "max_share_sector",
        "intensidad_export_prom",
        "densidad_estab_prom",
        "salario_promedio_ponderado"
    ]

    for level_geo in cfg["run"]["level_geo"]:
        fpath = os.path.join(cfg["paths"]["marts_dir"], f"features_{level_geo}.csv")
        feature_rows = read_csv(fpath)
        agg_rows = aggregate_for_clustering(feature_rows, level_geo)

        matrix = scale_matrix(agg_rows, feature_vars, cfg["run"]["scale_method"])
        kmin = cfg["run"]["k_min"]
        kmax = min(cfg["run"]["k_max"], len(agg_rows) - 1)

        best = {"k": None, "silhouette": -2.0, "labels": None}
        for k in range(kmin, kmax + 1):
            labels = kmeans(matrix, k, seed=cfg["run"]["random_seed"])
            score = silhouette_score(matrix, labels)
            if score > best["silhouette"]:
                best = {"k": k, "silhouette": score, "labels": labels}

        if best["labels"] is None:
            raise RuntimeError(f"No fue posible clusterizar nivel {level_geo}")

        for i, r in enumerate(agg_rows):
            r["cluster"] = int(best["labels"][i]) + 1
            r["cluster_method"] = "kmeans"
            r["k_optimo"] = int(best["k"])
            r["silhouette"] = round(best["silhouette"], 6)

        cluster_out = os.path.join(cfg["paths"]["clusters_dir"], f"clusters_{level_geo}.csv")
        write_csv(cluster_out, agg_rows)

        profile = profile_clusters(agg_rows, feature_vars)
        profile_out = os.path.join(cfg["paths"]["clusters_dir"], f"perfiles_{level_geo}.csv")
        write_csv(profile_out, profile)

        meta = {
            "level_geo": level_geo,
            "k_optimo": best["k"],
            "silhouette": round(best["silhouette"], 6),
            "n_territorios": len(agg_rows),
            "features": feature_vars,
            "run_years": cfg["run"]["years"]
        }
        meta_out = os.path.join(cfg["paths"]["clusters_dir"], f"metricas_modelo_{level_geo}.json")
        with open(meta_out, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print("Clustering OK")


if __name__ == "__main__":
    main()
