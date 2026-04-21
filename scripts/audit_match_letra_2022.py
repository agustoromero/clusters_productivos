#!/usr/bin/env python3
"""Audita el match de la fuente de salarios 2022 contra letras CLAE.

Genera tablas y gráficos para provincia y departamento.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

SECTOR_TO_LETRA = {
    "agricultura, ganaderia y pesca": "A",
    "comercio": "G",
    "construccion": "F",
    "electricidad, gas y agua": "D",
    "explotacion de minas y canteras": "B",
    "industria manufacturera": "C",
    "servicios": "N",
}


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Auditar match salarios 2022 -> letra CLAE")
    p.add_argument("--salarios", default="clusters_productivos/departamento_series_empleo_y_salarios_mensual_sector2_0.csv")
    p.add_argument("--clae", default="clusters_productivos/actividades_establecimientos.csv")
    p.add_argument("--outdir", default="clusters_productivos/pipeline/output/quality")
    p.add_argument("--anio", type=int, default=2022)
    return p


def _plot_top_nomatch(df_nomatch: pd.DataFrame, id_cols: list[str], outpath: Path, title: str) -> None:
    if df_nomatch.empty:
        fig, ax = plt.subplots(figsize=(8, 3.5))
        ax.text(0.5, 0.5, "Sin sectores sin match", ha="center", va="center")
        ax.set_axis_off()
        fig.suptitle(title)
        fig.tight_layout()
        fig.savefig(outpath, dpi=160)
        plt.close(fig)
        return

    top = (
        df_nomatch.groupby("sector_norm", as_index=False)["registros"].sum()
        .sort_values("registros", ascending=False)
        .head(20)
    )
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.barh(top["sector_norm"], top["registros"], color="#d95f02")
    ax.invert_yaxis()
    ax.set_xlabel("Cantidad de registros 2022")
    ax.set_ylabel("Sector (fuente salarios)")
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(outpath, dpi=160)
    plt.close(fig)


def main() -> None:
    args = _build_parser().parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    salarios = pd.read_csv(args.salarios, sep=";", encoding="latin-1")
    salarios["Periodo"] = salarios["Periodo"].astype(str)
    salarios["anio"] = salarios["Periodo"].str[:4].astype(int)
    salarios = salarios[salarios["anio"] == args.anio].copy()

    salarios["sector_norm"] = salarios["Sector"].astype(str).str.strip().str.lower()
    salarios["letra_mapeada"] = salarios["sector_norm"].map(SECTOR_TO_LETRA)

    clae = pd.read_csv(args.clae, encoding="utf-8")
    letras_validas = set(clae["letra"].dropna().astype(str).str.strip().unique())
    salarios["match_letra_clae"] = salarios["letra_mapeada"].isin(letras_validas)

    prov = (
        salarios.groupby(["id_prov", "sector_norm"], dropna=False)
        .agg(
            registros=("Sector", "size"),
            letra_mapeada=("letra_mapeada", lambda s: s.dropna().iloc[0] if len(s.dropna()) else None),
            match_letra_clae=("match_letra_clae", "max"),
        )
        .reset_index()
    )

    depto = (
        salarios.groupby(["id_prov", "id_depto", "sector_norm"], dropna=False)
        .agg(
            registros=("Sector", "size"),
            letra_mapeada=("letra_mapeada", lambda s: s.dropna().iloc[0] if len(s.dropna()) else None),
            match_letra_clae=("match_letra_clae", "max"),
        )
        .reset_index()
    )

    prov_nomatch = prov[~prov["match_letra_clae"]].copy()
    depto_nomatch = depto[~depto["match_letra_clae"]].copy()

    prov.to_csv(outdir / f"audit_match_letra_provincia_{args.anio}.csv", index=False)
    depto.to_csv(outdir / f"audit_match_letra_departamento_{args.anio}.csv", index=False)
    prov_nomatch.to_csv(outdir / f"audit_nomatch_letra_provincia_{args.anio}.csv", index=False)
    depto_nomatch.to_csv(outdir / f"audit_nomatch_letra_departamento_{args.anio}.csv", index=False)

    _plot_top_nomatch(
        prov_nomatch,
        ["id_prov"],
        outdir / f"audit_nomatch_letra_provincia_{args.anio}.png",
        f"Top sectores sin match (provincia) - {args.anio}",
    )
    _plot_top_nomatch(
        depto_nomatch,
        ["id_prov", "id_depto"],
        outdir / f"audit_nomatch_letra_departamento_{args.anio}.png",
        f"Top sectores sin match (departamento) - {args.anio}",
    )

    resumen = {
        "anio": args.anio,
        "source_rows_anio": int(len(salarios)),
        "provincias_sector_total": int(len(prov)),
        "provincias_sector_nomatch": int(len(prov_nomatch)),
        "departamentos_sector_total": int(len(depto)),
        "departamentos_sector_nomatch": int(len(depto_nomatch)),
    }
    (outdir / f"audit_match_resumen_{args.anio}.json").write_text(
        json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(resumen, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
