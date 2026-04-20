# Runbook: ejecutar, resumir resultados y sincronizar con GitHub

## Comandos mínimos (VSCode terminal)

> Ejecutá estos **3 comandos** en orden:

```bash
mkdir -p data/raw/salarios data/raw/ipc outputs
```

```bash
python scripts/execute_salarios_e2e.py --salarios data/raw/salarios/departamento_series_empleo_y_salarios_mensual_sector2_0.csv --ipc data/raw/ipc/ipc_indec_base2016.xlsx --outdir outputs --anio 2022
```

```bash
git add PLAN_INTEGRACION_SALARIOS.md RUNBOOK_EJECUCION_Y_SYNC.md scripts/execute_salarios_e2e.py scripts/ejecutar_y_sincronizar.sh outputs && git commit -m "run: ejecutar e2e salarios y sincronizar" && git push -u origin $(git branch --show-current)
```

## 1) Ejecutar pipeline E2E

```bash
python scripts/execute_salarios_e2e.py \
  --salarios data/raw/salarios/departamento_series_empleo_y_salarios_mensual_sector2_0.csv \
  --ipc data/raw/ipc/ipc_indec_base2016.xlsx \
  --outdir outputs \
  --anio 2022
```

## 2) Outputs generados

- `outputs/salarios_mensual_clean.csv`
- `outputs/salarios_anual_prov_actividad2d.csv`
- `outputs/quality_ingesta.json`

- `outputs/quality_sector_anio.csv`
=======


## 3) Resumen rápido

```bash
python - <<'PY'
import json
from pathlib import Path
q = json.loads(Path('outputs/quality_ingesta.json').read_text())['salarios']
print('pct_ceros_salario_real:', q['pct_ceros_salario_real'], q['pct_ceros_status'])
print('pct_nan_salario_real_pre_fill:', q['pct_nan_salario_real_pre_fill'])
print('pct_nan_salario_nominal_post_merge:', q['pct_nan_salario_nominal_post_merge'])
print('pct_grupos_empleo_sum_cero:', q['pct_grupos_empleo_sum_cero'])
print('sectores_cubiertos:', q['sectores_cubiertos'])
print('anios_cubiertos:', q['anios_cubiertos'])

print('sectores_presentes_anio_objetivo:', q.get('sectores_presentes_anio_objetivo'))
print('sectores_faltantes_anio_objetivo:', q.get('sectores_faltantes_anio_objetivo'))
print('sectores_con_salario_anual_cero_anio_objetivo:', q.get('sectores_con_salario_anual_cero_anio_objetivo'))
=======

PY
```

## 4) Sincronizar con GitHub

```bash
git add scripts/execute_salarios_e2e.py RUNBOOK_EJECUCION_Y_SYNC.md PLAN_INTEGRACION_SALARIOS.md
git commit -m "feat: ejecutar e2e salarios con resumen de calidad y runbook de sync"
git push origin <tu-rama>
```



## 5) One-shot (ejecutar + resumir + commit + push)

```bash
bash scripts/ejecutar_y_sincronizar.sh \
  data/raw/salarios/departamento_series_empleo_y_salarios_mensual_sector2_0.csv \
  data/raw/ipc/ipc_indec_base2016.xlsx \
  outputs \
  2022
```

Si no tenés remoto configurado, primero:

```bash
git remote add origin <url-del-repo>
git push -u origin <tu-rama>
```
