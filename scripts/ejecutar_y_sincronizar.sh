#!/usr/bin/env bash
set -euo pipefail

SALARIOS_PATH="${1:-data/raw/salarios/departamento_series_empleo_y_salarios_mensual_sector2_0.csv}"
IPC_PATH="${2:-data/raw/ipc/ipc_indec_base2016.xlsx}"
OUTDIR="${3:-outputs}"
ANIO="${4:-}"

if [[ ! -f "$SALARIOS_PATH" ]]; then
  echo "ERROR: no existe archivo de salarios: $SALARIOS_PATH" >&2
  exit 2
fi

if [[ ! -f "$IPC_PATH" ]]; then
  echo "ERROR: no existe archivo IPC: $IPC_PATH" >&2
  exit 3
fi

echo "[1/4] Ejecutando pipeline E2E..."
if [[ -n "$ANIO" ]]; then
  python scripts/execute_salarios_e2e.py --salarios "$SALARIOS_PATH" --ipc "$IPC_PATH" --outdir "$OUTDIR" --anio "$ANIO"
else
  python scripts/execute_salarios_e2e.py --salarios "$SALARIOS_PATH" --ipc "$IPC_PATH" --outdir "$OUTDIR"
fi

echo "[2/4] Resumen de calidad..."
python - <<'PY'
import json
from pathlib import Path
q = json.loads(Path('outputs/quality_ingesta.json').read_text(encoding='utf-8'))['salarios']
print('pct_ceros_salario_real:', q['pct_ceros_salario_real'], q['pct_ceros_status'])
print('pct_nan_salario_real_pre_fill:', q['pct_nan_salario_real_pre_fill'])
print('pct_nan_salario_nominal_post_merge:', q['pct_nan_salario_nominal_post_merge'])
print('pct_grupos_empleo_sum_cero:', q['pct_grupos_empleo_sum_cero'])
print('sectores_cubiertos:', q['sectores_cubiertos'])
print('anios_cubiertos:', q['anios_cubiertos'])
PY

echo "[3/4] Versionando outputs..."
git add "$OUTDIR"/salarios_mensual_clean.csv "$OUTDIR"/salarios_anual_prov_actividad2d.csv "$OUTDIR"/quality_ingesta.json
if git diff --cached --quiet; then
  echo "Sin cambios en outputs para commitear."
else
  git commit -m "data: actualizar outputs e2e salarios"
fi

echo "[4/4] Sincronizando con remoto..."
if [[ -z "$(git remote)" ]]; then
  echo "WARNING: no hay remoto configurado. Ejecutá: git remote add origin <url>" >&2
  exit 4
fi

git push

echo "OK: ejecución y sincronización finalizada."
