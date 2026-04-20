# Plan paso a paso: integración de `departamento_series_empleo_y_salarios_mensual_sector2_0`

Este documento describe un flujo **end-to-end** para incorporar `salarios` al desarrollo de clusters productivos (scripts R/Python 01, 02 y 03), incorporando:

- agregación temporal anual desde `periodo` (`aaaamm`),
- agregación geográfica por `id_prov`,
- agregación de actividad a **2 dígitos**,
- deflactación con IPC (base 100 = diciembre 2016),
- regla de faltantes: promedio anual válido con **mínimo 6 meses**.

---

## 0) Definición operativa cerrada (con tus aclaraciones)

1. **Fuente salarial**: `departamento_series_empleo_y_salarios_mensual_sector2_0`.
2. **Variable target**: `salarios`.
3. **Tiempo**:
   - `periodo` en formato `aaaamm`.
   - se calcula promedio anual por año calendario.
4. **Geografía**: provincia (`id_prov`).
5. **Actividad**:
   - nivel final a **2 dígitos**,
   - usar columna `sector` descriptiva de la fuente y mapearla al diccionario `dicc_clanae2010_ciiu4` con la metodología vigente.
6. **Faltantes**: se permite promedio anual si hay **al menos 6 meses** con dato válido.
7. **Precios constantes**:
   - usar Excel IPC cargado por vos,
   - `IPC` = índice (base 100 dic-2016),
   - `INF` = variación mensual,
   - deflactar salarios mensuales antes del promedio anual.

---

## 1) Sincronizar el Excel IPC a GitHub (paso previo obligatorio)

> Como el Excel está en tu PC local, primero debe quedar versionado en este repo para que los scripts lo consuman de forma reproducible.

### 1.1 Ubicación sugerida

Guardar en una ruta estable, por ejemplo:

- `data/raw/ipc/ipc_indec_base2016.xlsx`

### 1.2 Pasos para sincronización (en tu entorno local)

1. Copiar el archivo Excel a la ruta acordada.
2. Ejecutar:

```bash
git add data/raw/ipc/ipc_indec_base2016.xlsx
git commit -m "data: add IPC base dic-2016 for salary deflation"
git push
```

3. Verificar que los scripts lean desde esa ruta (sin rutas absolutas de tu PC).

### 1.3 Validaciones del IPC

- unicidad por `periodo` mensual,
- `IPC > 0`,
- coherencia `INF` vs variación de `IPC` (control cruzado),
- cobertura mensual completa para el rango de años a procesar.

---

## 2) Contrato de datos (actualizado)

### 2.1 Tabla de salarios

Campos mínimos esperados:

- `periodo` (`aaaamm`),
- `id_prov`,
- `sector` (descriptivo),
- `salarios`.

### 2.2 Diccionario de actividad

- fuente de referencia: `dicc_clanae2010_ciiu4`,
- salida obligatoria: código/agrupación de actividad a **2 dígitos**,
- incluir campo de trazabilidad: `match_confianza` o `metodo_match`.

### 2.3 Reglas

- `salarios >= 0`,
- mes válido (01..12),
- no duplicados en llave mensual final: `id_prov + actividad_2d + periodo`.

---

## 3) Implementación por script (01/02/03)

## 3.1 Script 01 (R/Python): ingesta y estandarización mensual

1. Leer salarios mensuales.
2. Estandarizar columnas a `snake_case`.
3. Parsear `periodo` a `anio` y `mes`.
4. Normalizar `id_prov`.
5. Resolver actividad a 2 dígitos:
   - limpiar texto de `sector`,
   - hacer match contra descripciones de `dicc_clanae2010_ciiu4`,
   - aplicar metodología definida para ambigüedades,
   - guardar `actividad_2d` + `metodo_match`.
6. Cargar IPC mensual desde Excel versionado.
7. Deflactar salario mensual a precios constantes.
8. Guardar tabla mensual limpia (`salarios_mensual_clean`).

**Controles (01):**
- `% nulos` en `salarios`, `id_prov`, `actividad_2d`, `periodo`,
- tasa de match de actividad 2d,
- cobertura IPC por mes.

## 3.2 Script 02 (R/Python): agregado anual

1. Partir de `salarios_mensual_clean`.
2. Crear agregados anuales por:
   - `id_prov`, `actividad_2d`, `anio`.
3. Métricas:
   - `n_meses_observados`,
   - `salario_promedio_anual_const` (promedio de salarios deflactados).
4. Regla de validez:
   - `n_meses_observados >= 6`.
5. Generar flags:
   - `flag_cobertura_mensual_baja = n_meses_observados < 6`,
   - `flag_ipc_faltante`,
   - `flag_match_actividad_bajo`.
6. Integrar al panel maestro de clusters por llave:
   - `id_prov + actividad_2d + anio`.

**Controles (02):**
- tasa de match del join al panel,
- distribución anual de salario constante,
- outliers por provincia/actividad.

## 3.3 Script 03 (R/Python): features, modelos y salidas

1. Incluir `salario_promedio_anual_const` en features finales.
2. Definir tratamiento de faltantes post-join.
3. Reentrenar/recalcular outputs del pipeline.
4. Comparar resultados contra baseline sin salarios.
5. Publicar tablas y reportes finales.

**Controles (03):**
- estabilidad de clusters,
- sensibilidad del score por incorporar salarios constantes,
- auditoría de provincias/actividades sin cobertura suficiente.

---

## 4) Pseudocódigo base actualizado

### Python

```python
# Parse periodo
df["periodo"] = df["periodo"].astype(str).str.zfill(6)
df["anio"] = df["periodo"].str[:4].astype(int)
df["mes"] = df["periodo"].str[4:6].astype(int)
df = df[df["mes"].between(1, 12)]

# Match actividad a 2 dígitos (placeholder de función)
df["actividad_2d"] = map_sector_to_2d(df["sector"], dicc_clanae2010_ciiu4)

# Deflactar a precios constantes base dic-2016 = 100
df = df.merge(ipc_df[["periodo", "IPC"]], on="periodo", how="left")
df["salario_const"] = df["salarios"] * (100.0 / df["IPC"])

# Agregado anual con regla >= 6 meses
agg = (df.groupby(["id_prov", "actividad_2d", "anio"], as_index=False)
         .agg(n_meses_observados=("salario_const", lambda s: s.notna().sum()),
              salario_promedio_anual_const=("salario_const", "mean")))
agg["flag_cobertura_mensual_baja"] = agg["n_meses_observados"] < 6
agg = agg[agg["n_meses_observados"] >= 6]
```

### R

```r
# Parse periodo
df <- df |>
  dplyr::mutate(
    periodo = stringr::str_pad(as.character(periodo), width = 6, pad = "0"),
    anio = as.integer(substr(periodo, 1, 4)),
    mes = as.integer(substr(periodo, 5, 6))
  ) |>
  dplyr::filter(mes >= 1, mes <= 12)

# Match actividad a 2 dígitos (placeholder)
df <- df |>
  dplyr::mutate(actividad_2d = map_sector_to_2d(sector, dicc_clanae2010_ciiu4))

# Deflactar salario mensual
df <- df |>
  dplyr::left_join(ipc_df |> dplyr::select(periodo, IPC), by = "periodo") |>
  dplyr::mutate(salario_const = salarios * (100 / IPC))

# Agregado anual con mínimo 6 meses
agg <- df |>
  dplyr::group_by(id_prov, actividad_2d, anio) |>
  dplyr::summarise(
    n_meses_observados = sum(!is.na(salario_const)),
    salario_promedio_anual_const = mean(salario_const, na.rm = TRUE),
    .groups = "drop"
  ) |>
  dplyr::mutate(flag_cobertura_mensual_baja = n_meses_observados < 6) |>
  dplyr::filter(n_meses_observados >= 6)
```

---

## 5) Pruebas automáticas mínimas

1. `periodo` -> (`anio`, `mes`) correcto.
2. Match de actividad descriptiva a 2 dígitos reproducible.
3. Deflactación correcta con IPC (casos controlados).
4. Regla de faltantes: 5 meses excluye, 6 meses incluye.
5. Integración 01→02→03 sin duplicados en llave final.
6. Test de promedio ponderado: cuando `sum(empleo)=0`, el resultado debe quedar `NaN` + flag, nunca forzado a 0.
7. Test de no-regresión: `.fillna(0)` no puede aplicarse antes de validaciones de merge/IPC.

---

## 6) Preguntas críticas pendientes (solo las que quedan abiertas)

1. **Año base de reporte final**: para presentar resultados, ¿querés expresar todo en moneda constante de 2016 (coherente con IPC base dic-2016=100) o reescalar a otro año de referencia (por ejemplo 2022)?
2. **Cobertura de años productivos**: confirmame desde qué año mensual el IPC y salarios tienen calidad suficiente para correr producción completa.
3. **Empates en el match descriptivo-sectorial**: cuando una descripción de `sector` pueda mapear a más de un 2d, ¿preferimos regla automática conservadora o tabla manual de excepciones curada?

---

## 7) Entregables

1. `salarios_mensual_clean` con `actividad_2d` y `salario_const`.
2. `salarios_anual_prov_actividad2d` con regla de `>=6` meses.
3. panel final de clusters enriquecido con salario anual constante.
4. pruebas de calidad y trazabilidad del match sectorial + IPC.


---

## 8) Diagnóstico de calidad: salario deflactado en 0 (bloque obligatorio)

Tener **muchas filas con `salario_deflactado = 0` no es normal** en un pipeline sano.

### 8.1 Casos válidos (esperados pero minoritarios)

- no hay salario para esa combinación `id_prov + actividad_2d + anio`,
- sector inexistente en ese territorio/período,
- en promedio ponderado: `sum(empleo)=0` (debe tratarse como `NaN`, no 0 silencioso).

### 8.2 Casos problemáticos más frecuentes

1. **Fallo de merge** salarios ↔ empleo (keys incompletas o mal tipadas).
2. **Deflactor IPC mal aplicado** (`IPC` nulo/0 o fuera de cobertura temporal).
3. **Salario nominal mal parseado** (strings convertidos a 0, filtros erróneos).
4. **Promedio ponderado mal definido** con denominador 0 resuelto incorrectamente.
5. **`.fillna(0)` temprano** que enmascara errores estructurales.

### 8.3 Chequeos obligatorios en script 02 (quality checks)

1. `pct_ceros_salario_real = mean(salario_real == 0)`
   - `< 5%`: aceptable
   - `5%-20%`: warning
   - `> 20%`: error estructural
2. `pct_nan_salario_real_pre_fill` (antes de cualquier imputación).
3. `pct_nan_salario_nominal_post_merge` (cobertura de join).
4. `IPC`: validar `min(IPC) > 0`, sin nulos, cobertura completa de meses/años usados.
5. `pct_grupos_empleo_sum_cero` en agregación ponderada.

### 8.4 Reglas de implementación para evitar sesgos

- **Prohibido** hacer `.fillna(0)` antes de correr checks de calidad.
- Mantener `NaN` y crear flags:
  - `salario_missing`,
  - `ipc_missing`,
  - `empleo_sum_cero`,
  - `merge_salario_fallido`.
- Para clustering:
  - preferir filtrar filas con salario faltante, o
  - imputar explícitamente (mediana sectorial/provincial) con flag de imputación.

### 8.5 Match sectorial: riesgo puntual de servicios

Mapear todo “Servicios” a una sola letra (por ejemplo `N`) es insuficiente y genera huecos de cobertura.

- ampliar reglas con metodología CLAE/CIIU para cubrir servicios en múltiples letras (ej. `H, I, J, K, M, N, O, P, Q, R, S` según alcance analítico),
- monitorear `sectores_cubiertos` por año y provincia.

### 8.6 Logging recomendado (`quality_ingesta.json`)

```json
{
  "salarios": {
    "pct_ceros_salario_real": 0.0,
    "pct_nan_salario_real_pre_fill": 0.0,
    "pct_nan_salario_nominal_post_merge": 0.0,
    "pct_grupos_empleo_sum_cero": 0.0,
    "sectores_cubiertos": 0,
    "anios_cubiertos": []
  }
}
```

