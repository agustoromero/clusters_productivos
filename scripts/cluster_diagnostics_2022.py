#!/usr/bin/env python3
"""Genera diagnósticos de k y estabilidad para clustering (2022)."""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


FEATURE_VARS = [
    "empleo_total",
    "establecimientos_total",
    "exportadoras_total",
    "hhi",
    "diversidad_sectorial",
    "max_share_sector",
    "intensidad_export_prom",
    "densidad_estab_prom",
    "salario_promedio_ponderado",
]


def _import_clustering_module(module_path: str):
    spec = importlib.util.spec_from_file_location("cluster03", module_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


def _inertia(matrix, labels):
    k = max(labels) + 1
    d = len(matrix[0])
    centers = [[0.0] * d for _ in range(k)]
    counts = [0] * k
    for lbl, row in zip(labels, matrix):
        counts[lbl] += 1
        for j, val in enumerate(row):
            centers[lbl][j] += val
    for c in range(k):
        if counts[c] > 0:
            centers[c] = [v / counts[c] for v in centers[c]]
    sse = 0.0
    for lbl, row in zip(labels, matrix):
        sse += sum((x - y) ** 2 for x, y in zip(row, centers[lbl]))
    return sse


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Diagnóstico elbow/silhouette/estabilidad")
    p.add_argument("--features-dir", default="clusters_productivos/pipeline/output/marts")
    p.add_argument("--outdir", default="clusters_productivos/pipeline/output/quality")
    p.add_argument("--k-min", type=int, default=2)
    p.add_argument("--k-max", type=int, default=8)
    p.add_argument("--seeds", type=int, default=40)
    p.add_argument(
        "--cluster-module",
        default="clusters_productivos/pipeline/python/03_clustering_e_interpretacion.py",
    )
    return p


def main() -> None:
    args = _parser().parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    mod = _import_clustering_module(args.cluster_module)

    all_rows = []
    summary_rows = []

    for level in ["provincia", "departamento"]:
        fpath = Path(args.features_dir) / f"features_{level}.csv"
        feature_rows = list(csv.DictReader(fpath.open("r", encoding="utf-8", newline="")))
        agg_rows = mod.aggregate_for_clustering(feature_rows, level, "letra")
        matrix = mod.scale_matrix(agg_rows, FEATURE_VARS, method="zscore")

        k_max_eff = min(args.k_max, len(agg_rows) - 1)
        if k_max_eff < args.k_min:
            continue

        for k in range(args.k_min, k_max_eff + 1):
            sils = []
            sses = []
            for seed in range(1, args.seeds + 1):
                labels = mod.kmeans(matrix, k, seed=seed)
                sils.append(mod.silhouette_score(matrix, labels))
                sses.append(_inertia(matrix, labels))
                all_rows.append(
                    {
                        "level": level,
                        "k": k,
                        "seed": seed,
                        "silhouette": sils[-1],
                        "inertia": sses[-1],
                    }
                )

            summary_rows.append(
                {
                    "level": level,
                    "k": k,
                    "silhouette_mean": sum(sils) / len(sils),
                    "silhouette_min": min(sils),
                    "silhouette_max": max(sils),
                    "inertia_mean": sum(sses) / len(sses),
                }
            )

    detail = pd.DataFrame(all_rows)
    summary = pd.DataFrame(summary_rows)
    detail.to_csv(outdir / "k_diagnostics_detail.csv", index=False)
    summary.to_csv(outdir / "k_diagnostics_summary.csv", index=False)

    for level in summary["level"].unique():
        df = summary[summary["level"] == level].sort_values("k")
        fig, ax1 = plt.subplots(figsize=(8, 5))
        ax1.plot(df["k"], df["inertia_mean"], marker="o", color="#1f77b4")
        ax1.set_xlabel("k")
        ax1.set_ylabel("Inercia media (SSE)", color="#1f77b4")

        ax2 = ax1.twinx()
        ax2.plot(df["k"], df["silhouette_mean"], marker="s", color="#d62728")
        ax2.fill_between(df["k"], df["silhouette_min"], df["silhouette_max"], color="#d62728", alpha=0.15)
        ax2.set_ylabel("Silhouette media (rango min-max por seed)", color="#d62728")

        plt.title(f"Diagnóstico de k ({level}) - {args.seeds} seeds")
        fig.tight_layout()
        fig.savefig(outdir / f"k_diagnostics_{level}.png", dpi=170)
        plt.close(fig)

    consensus = []
    for level in summary["level"].unique():
        df = summary[summary["level"] == level].copy().sort_values("k")
        best = df.loc[df["silhouette_mean"].idxmax()]
        consensus.append(
            {
                "level": level,
                "k_recomendado_por_silhouette": int(best["k"]),
                "silhouette_mean": float(best["silhouette_mean"]),
                "nota": "Revisar junto con elbow e interpretabilidad económica.",
            }
        )

    (outdir / "k_diagnostics_consenso.json").write_text(json.dumps(consensus, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Diagnósticos generados en", outdir)


if __name__ == "__main__":
    main()
