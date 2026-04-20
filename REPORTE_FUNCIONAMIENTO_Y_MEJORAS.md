# Reporte completo: funcionamiento del repositorio, mejoras y diagnóstico de merge IPC-salarios

Fecha: 2026-04-20

---

## 1) Cómo funciona hoy el repositorio

El repositorio está organizado alrededor de un flujo operativo para integrar salarios al pipeline de clusters:

1. **Especificación funcional/técnica**
   - `PLAN_INTEGRACION_SALARIOS.md` define reglas de negocio, contrato de datos, diseño por etapas 01/02/03 y controles de calidad.
2. **Ejecución E2E de salarios**
   - `scripts/execute_salarios_e2e.py` ejecuta la transformación: ingesta de salarios e IPC, parseo temporal, deflactación, agregado anual y métricas de calidad.
3. **Operación y sincronización**
   - `RUNBOOK_EJECUCION_Y_SYNC.md` documenta comandos de ejecución y push.
   - `scripts/ejecutar_y_sincronizar.sh` envuelve ejecución + resumen + commit + push.

### Flujo concreto del script `execute_salarios_e2e.py`

1. **Inputs esperados**
   - Salarios CSV: `periodo,id_prov,sector,salarios[,empleo]`.
   - IPC CSV/XLSX: `periodo,IPC[,INF]`.
2. **Normalización**
   - `periodo` se limpia a dígitos y se fuerza a 6 posiciones (`aaaamm`).
   - de `periodo` se derivan `anio` y `mes`; se filtran meses inválidos fuera de `1..12`.
3. **Merge salarial-IPC**
   - join por `periodo` mensual.
4. **Deflactación**
   - fórmula: `salario_const = salario_nominal * (100 / IPC)` (base dic-2016=100).
5. **Agregado anual**
   - agrupación por `id_prov + actividad_2d + anio`.
   - regla de validez: `n_meses_observados >= 6`.
6. **Salida y calidad**
   - outputs CSV mensual/anual + `quality_ingesta.json`.
   - métricas: `% ceros`, `% NaN`, cobertura IPC, cobertura sector/año.

---

## 2) Conversión de salario corriente a constante (paso a paso y formatos de fecha)

## 2.1 Formatos de fecha involucrados

- **Año**: `aaaa` (ej. `2022`).
- **Mes**: `m` o `mm` de origen (ej. `1` o `01`), debe quedar `mm`.
- **Periodo salarios**: `aaaamm` (ej. `202201`).

## 2.2 Regla de estandarización temporal recomendada

1. Si recibís columnas separadas `anio` y `mes`:
   - `mes` debe padearse a 2 dígitos: `01..12`.
   - construir `periodo = anio || mes`.
2. Si recibís `periodo` en otro formato:
   - eliminar separadores/no dígitos.
   - `zfill(6)`.
3. Validar:
   - longitud `6`,
   - año numérico,
   - mes entre `01` y `12`.

## 2.3 Deflactación correcta

Para cada fila mensual:

- `salario_nominal` = salario corriente del mes.
- `IPC_mes` = índice mensual base dic-2016=100.
- `salario_constante_mes = salario_nominal * (100 / IPC_mes)`.

Luego:

- promediar mensual a anual por `id_prov + actividad_2d + anio`,
- mantener solo grupos con al menos 6 meses válidos.

---

## 3) Caracterización del problema de merge IPC ↔ salarios

El problema más crítico es la pérdida de match temporal que deriva en `IPC` nulo y por ende `salario_const` nulo.

### 3.1 Causas típicas

1. **Clave temporal inconsistente**
   - salarios en `aaaamm`, IPC en fecha tipo `aaaa-mm-01` o texto.
2. **Tipos incompatibles**
   - un lado string, otro numérico.
3. **Cobertura temporal incompleta del IPC**
   - faltan meses para el año objetivo (2022).
4. **Duplicados en IPC por período**
   - join 1:N accidental.
5. **Mes mal parseado**
   - `20221` en lugar de `202201`.

### 3.2 Síntomas de que el merge está mal

- `ipc_nulos` alto,
- `pct_nan_salario_real_pre_fill` alto,
- demasiados `salario_const = 0` o `NaN`,
- caída abrupta de `filas_anuales_validas`.

### 3.3 Chequeos obligatorios para detectar rápido

1. Unicidad IPC por `periodo`.
2. Intersección de períodos salarios vs IPC (esperada ~100% para 2022).
3. `IPC > 0` y sin nulos.
4. No aplicar `.fillna(0)` antes de medir calidad.
5. Tasa de nulos post-merge por período y provincia.

---

## 4) Asegurar funcionamiento considerando solo 2022 (completitud)

## 4.1 Filtro recomendado

- filtrar salarios a `anio == 2022` (o `periodo` entre `202201` y `202212`).
- filtrar IPC a esos mismos 12 meses.
- en el script actual, usar `--anio 2022` para forzar salida solo 2022.

## 4.2 Reglas de completitud 2022

Por cada `id_prov + actividad_2d`:

1. `n_meses_observados` entre 0 y 12.
2. **válido para agregado anual** si `n_meses_observados >= 6`.
3. bandera alta calidad opcional si `n_meses_observados == 12`.
4. reportar distribución de cobertura (`6-8`, `9-11`, `12`).

## 4.3 Tablero mínimo de control 2022

- `% combinaciones con 12/12 meses`.
- `% combinaciones con >= 6 meses`.
- `% combinaciones excluidas (< 6)`.
- `ipc_nulos` total en 2022.
- `pct_ceros_salario_real` en 2022.

---

## 5) Cómo mejorar el repositorio (prioridad alta)

1. **Separar lógica en módulos**
   - parser temporal, merge, deflactación, agregación, quality.
2. **Tests automáticos**
   - unit tests para parseo (`20221` -> `202201`),
   - test de merge con cobertura completa 2022,
   - test de denominador 0 en ponderado.
3. **Data contracts formales**
   - esquema esperado de columnas + tipos + rangos.
4. **Métricas versionadas**
   - persistir `quality_ingesta.json` por corrida con timestamp.
5. **Modo estricto 2022**
   - flag CLI `--anio 2022` para recorte controlado.
6. **Mapping sectorial robusto**
   - reemplazar heurística actual por diccionario explícito `sector -> actividad_2d`.

---

## 6) Ejecución de clusters con salarios + flujo de PR

## 6.1 Ejecución local recomendada

1. Ubicar archivos fuente:
   - `data/raw/salarios/departamento_series_empleo_y_salarios_mensual_sector2_0.csv`
   - `data/raw/ipc/ipc_indec_base2016.xlsx` (o CSV equivalente)
2. Ejecutar e2e salarios:

```bash
python scripts/execute_salarios_e2e.py \
  --salarios data/raw/salarios/departamento_series_empleo_y_salarios_mensual_sector2_0.csv \
  --ipc data/raw/ipc/ipc_indec_base2016.xlsx \
  --outdir outputs
```

3. Verificar `outputs/quality_ingesta.json`.
4. Integrar `outputs/salarios_anual_prov_actividad2d.csv` al paso de features del pipeline de clusters (script 02/03 de tu proyecto principal).

## 6.2 One-shot para ejecutar + sync

```bash
bash scripts/ejecutar_y_sincronizar.sh \
  data/raw/salarios/departamento_series_empleo_y_salarios_mensual_sector2_0.csv \
  data/raw/ipc/ipc_indec_base2016.xlsx \
  outputs
```

## 6.3 Flujo PR recomendado

1. `git checkout -b feat/salarios-2022-e2e`
2. ejecutar pipeline + validar calidad 2022
3. `git add ...` (scripts/docs/outputs necesarios)
4. `git commit -m "feat: integrar salarios constantes 2022 al pipeline de clusters"`
5. `git push -u origin feat/salarios-2022-e2e`
6. abrir PR con:
   - resumen de calidad (`pct_ceros`, `%>=6 meses`, `ipc_nulos`),
   - impacto en clusters (comparación baseline vs con salarios),
   - riesgos conocidos y mitigaciones.

---

## 7) Criterio de “todo funciona” para 2022

Se considera aprobado si:

1. `ipc_nulos == 0` para `202201..202212`.
2. `IPC > 0` en todos los meses.
3. `% merge exitoso` salarios↔IPC cercano al 100%.
4. `pct_nan_salario_real_pre_fill` bajo umbral acordado.
5. Salida anual generada con regla `>=6` meses y sin duplicados de llave.
