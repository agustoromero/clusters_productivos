#!/usr/bin/env python3
"""Perfilado de género por cluster (sin incluir género en clustering)."""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generar perfiles de género por cluster")
    p.add_argument("--clusters-dir", default="clusters_productivos/pipeline/output/clusters")
    p.add_argument("--marts-dir", default="clusters_productivos/pipeline/output/marts")
    p.add_argument("--outdir", default="clusters_productivos/pipeline/output/quality")
    return p


def _keys(level: str) -> list[str]:
    if level == "provincia":
        return ["provincia_id", "provincia"]
    return ["provincia_id", "provincia", "in_departamentos", "departamento"]


def main() -> None:
    args = _parser().parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    for level in ["provincia", "departamento"]:
        clusters = pd.read_csv(Path(args.clusters_dir) / f"clusters_{level}.csv")
        gender = pd.read_csv(Path(args.marts_dir) / f"features_genero_{level}.csv")

        k = _keys(level)
        merged = clusters[k + ["cluster"]].merge(gender, on=k, how="left")
        merged["brecha_abs"] = (merged["empleo_varones"] - merged["empleo_mujeres"]).abs()

        profile = (
            merged.groupby("cluster", as_index=False)
            .agg(
                n_territorios=(k[0], "count"),
                ratio_fem_prom=("ratio_fem", "mean"),
                empleo_mujeres_prom=("empleo_mujeres", "mean"),
                empleo_varones_prom=("empleo_varones", "mean"),
                brecha_abs_prom=("brecha_abs", "mean"),
            )
            .sort_values("cluster")
        )
        profile.to_csv(outdir / f"gender_profile_by_cluster_{level}.csv", index=False)

        fig, ax = plt.subplots(figsize=(8, 5))
        ax.bar(profile["cluster"].astype(str), profile["ratio_fem_prom"], color="#7570b3")
        ax.set_title(f"Ratio femenino promedio por cluster ({level})")
        ax.set_xlabel("Cluster")
        ax.set_ylabel("Ratio femenino")
        fig.tight_layout()
        fig.savefig(outdir / f"gender_ratio_cluster_{level}.png", dpi=170)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(8, 5))
        ax.bar(profile["cluster"].astype(str), profile["brecha_abs_prom"], color="#1b9e77")
        ax.set_title(f"Brecha absoluta de empleo por cluster ({level})")
        ax.set_xlabel("Cluster")
        ax.set_ylabel("Brecha abs (empleo)")
        fig.tight_layout()
        fig.savefig(outdir / f"gender_gap_cluster_{level}.png", dpi=170)
        plt.close(fig)

    print("Perfiles de género guardados en", outdir)


if __name__ == "__main__":
    main()
