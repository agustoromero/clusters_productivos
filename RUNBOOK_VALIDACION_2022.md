# Runbook validación 2022 (match salarios, k y perfiles por cluster)

## Objetivo
Ejecutar el flujo completo para 2022 y dejar evidencia reproducible para merge a `main`.

## 1) Correr pipeline base
```bash
bash clusters_productivos/pipeline/python/run_pipeline.sh
```

## 2) Auditar match de salarios a letra CLAE (provincia/departamento)
```bash
python scripts/audit_match_letra_2022.py --anio 2022
```

## 3) Diagnóstico de k con 40 seeds (elbow + silhouette)
```bash
python scripts/cluster_diagnostics_2022.py --seeds 40 --k-min 2 --k-max 8
```

## 4) Perfiles de género post-clustering (sin incluir género en clustering)
```bash
python scripts/postcluster_gender_profiles.py
```

## 5) Outputs esperados en `clusters_productivos/pipeline/output/quality`
- `audit_match_letra_provincia_2022.csv`
- `audit_match_letra_departamento_2022.csv`
- `audit_nomatch_letra_provincia_2022.csv`
- `audit_nomatch_letra_departamento_2022.csv`
- `audit_nomatch_letra_provincia_2022.png`
- `audit_nomatch_letra_departamento_2022.png`
- `audit_match_resumen_2022.json`
- `k_diagnostics_detail.csv`
- `k_diagnostics_summary.csv`
- `k_diagnostics_provincia.png`
- `k_diagnostics_departamento.png`
- `k_diagnostics_consenso.json`
- `gender_profile_by_cluster_provincia.csv`
- `gender_profile_by_cluster_departamento.csv`
- `gender_ratio_cluster_provincia.png`
- `gender_ratio_cluster_departamento.png`
- `gender_gap_cluster_provincia.png`
- `gender_gap_cluster_departamento.png`

## 6) Commit sugerido
```bash
git add scripts/audit_match_letra_2022.py \
        scripts/cluster_diagnostics_2022.py \
        scripts/postcluster_gender_profiles.py \
        RUNBOOK_VALIDACION_2022.md \
        clusters_productivos/pipeline/output/quality

git commit -m "feat: agregar flujo de validación 2022 con match salarios, diagnóstico de k y perfiles de género"
```
