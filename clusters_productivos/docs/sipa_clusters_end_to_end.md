# Diseño end-to-end para clusters productivos con SIPA

## 1) Qué se puede armar con tu base actual

Con los archivos disponibles se puede construir **un pipeline parametrizado y reproducible** con dos módulos:

1. **Módulo provincial** (unidad de análisis: provincia).
2. **Módulo jurisdicción/departamento** (unidad de análisis: `in_departamentos`).

Los dos módulos pueden compartir la misma metodología de TP (datos → variables → clustering → perfiles → interpretación), cambiando solo el nivel geográfico y ciertos umbrales mínimos.

## 2) Fuentes incluidas y rol en el modelo

- `Establecimientos_por_departamento_y_actividad.csv`: base principal de actividad/empleo/establecimientos/exportadoras (sin género).
- `Datos_por_departamento_actividad_y_sexo.csv`: mismo cubo con apertura por sexo para módulo de brechas.
- `codigo_departamento_provincia.csv`: dimensión geográfica para normalización de claves.
- `actividades_establecimientos.csv` (o diccionario CLAE): dimensión de actividad para descripciones y agrupaciones.
- `Metodologia_establecimiento_productivos.pdf`: marco metodológico para documentar supuestos y límites.

## 3) Arquitectura recomendada de datos

### Capas

1. **Raw**: CSV/XLS originales sin modificar.
2. **Curated**: tablas tipadas, limpias y con joins de dimensiones.
3. **Marts analíticos**: features para clustering por nivel geo.
4. **Output**: clusters, perfiles y anexos metodológicos.

### Entregables mínimos por corrida

- `estab_curado.parquet`
- `gender_curado.parquet`
- `features_provincia.parquet`
- `features_departamento.parquet`
- `clusters_provincia.csv`
- `clusters_departamento.csv`
- reporte de calidad y diccionario de variables final

## 4) Estructura de datasets final (para clustering)

### 4.1 Tabla base curada (`estab_curado`)

**Granularidad**: `anio + in_departamentos + clae6`.

Variables clave:
- IDs: `anio`, `provincia_id`, `in_departamentos`, `clae6`, `clae2`, `letra`
- Medidas: `empleo`, `establecimientos`, `empresas_exportadoras`
- Descripciones: `clae6_desc`, `clae2_desc`, `letra_desc`

### 4.2 Tabla de género (`gender_curado`)

**Granularidad**: `anio + in_departamentos + clae6 + genero`.

Variables derivadas recomendadas:
- `ratio_fem` = mujeres / (mujeres + varones)
- `brecha_genero_abs` = |mujeres - varones|

### 4.3 Feature mart (`features_<nivel_geo>`)

**Granularidad**:
- provincial: `anio + provincia_id + clae2` (o nivel elegido)
- departamento: `anio + provincia_id + in_departamentos + clae2`

Features sugeridas:
- `share_empleo` = empleo sectorial / empleo total del territorio
- `densidad_estab` = establecimientos / empleo
- `intensidad_export` = exportadoras / establecimientos
- opcionales: concentración (HHI), diversidad sectorial, crecimiento interanual

### 4.4 Dataset final de clustering (`clusters_<nivel_geo>.csv`)

**Granularidad final de cluster**: territorio (provincia o departamento), con features agregadas temporales.

Campos:
- identificadores geográficos
- variables escaladas usadas en clustering
- `cluster`
- metadatos del modelo: método, k, semilla, fecha corrida

## 5) Paso a paso (metodología reproducible)

1. **Parametrizar corrida** (`pipeline/config/params.yml`): años, nivel geo, nivel de actividad, método de cluster.
2. **Ingesta y tipado** (`01_ingesta_y_curado.R`): estandarizar nombres/formatos, validar claves y guardar parquet.
3. **Controles de calidad**: nulos, duplicados por clave, totales anuales, consistencia de joins.
4. **Construcción de features** (`02_features_para_clusters.R`): variables estructurales por territorio/actividad.
5. **Módulo de género**: generar indicadores de brecha y composición, anexos comparables.
6. **Escalado**: z-score o robust scaling según outliers.
7. **Clustering** (`03_clustering_e_interpretacion.R`): k-means o hclust Ward.
8. **Selección de k**: silhouette + criterio sustantivo (interpretabilidad).
9. **Perfilado de clusters**: tablas resumen por cluster y variable.
10. **Interpretación para TP**: narrativa por perfil productivo + heterogeneidad territorial.
11. **Versionado y trazabilidad**: guardar parámetros, hash de insumos y fecha de corrida.

## 6) Cuellos de botella probables y solución

1. **Volumen y memoria** (clae6 × depto × año × género).
   - Solución: parquet + agregaciones tempranas a `clae2`/`letra`; procesamiento incremental por año.
2. **Sparsity (muchos ceros)** en departamentos pequeños.
   - Solución: reglas de mínimo (`min_obs_per_geo`, `min_obs_per_activity`) y winsorización.
3. **Cambios metodológicos en el tiempo**.
   - Solución: bloquear ventanas homogéneas (ej. 2018–2024) y documentar cortes.
4. **Comparabilidad provincia vs departamento**.
   - Solución: mismo set de features base + umbrales distintos de estabilidad.
5. **Sensibilidad del cluster a escala/outliers**.
   - Solución: comparar z-score vs robust, y reportar estabilidad del cluster.

## 7) Implicancias de diseño y desarrollo

- **Diseño modular**: separar ingesta, features y modelado evita rehacer todo en cada cambio.
- **Parámetros centralizados**: permite reproducir y auditar corridas.
- **Datos grandes**: conviene leer/escribir en parquet y no en CSV para capas intermedias.
- **Interpretabilidad**: no sobredimensionar k; preferir perfiles robustos y explicables para el TP.
- **Escalabilidad**: dejar posibilidad de sumar nuevas métricas (formalidad, salarios, dinámica temporal).

## 8) Preguntas críticas antes de desarrollar

1. ¿Ventana temporal exacta del TP (años) y cómo tratar quiebres?
2. ¿Nivel de actividad final: `clae6`, `clae2` o `letra`?
3. ¿Objetivo analítico principal: especialización, complejidad, exportación, empleo o mixto?
4. ¿Cluster transversal (promedio período) o panel anual?
5. ¿Cuántos clusters son útiles para política pública y narrativa del informe?
6. ¿Qué criterio mínimo define una jurisdicción “analizable”?
7. ¿Cómo se va a validar estabilidad de perfiles en el tiempo?
8. ¿Qué indicadores son “core” y cuáles “complementarios” (género, exportación)?
9. ¿Se necesita geovisualización final (mapas) como salida obligatoria?
10. ¿Qué tan comparable debe ser con trabajos previos (misma metodología exacta o adaptación)?

## 9) Orden de implementación sugerido

1. Cerrar definiciones metodológicas (preguntas críticas).
2. Implementar pipeline base en provincia.
3. Extender a departamento con umbrales de robustez.
4. Integrar módulo de género.
5. Ajustar cluster y validación de estabilidad.
6. Automatizar reporte final para TP (tablas + interpretación).


## 10) Definiciones metodológicas cerradas para esta implementación

Para la versión implementada en este repositorio se fijaron estas decisiones:

- Año de análisis: **2022**.
- Nivel de actividad: **CLAE2**.
- Objetivo: reproducir metodología del TP para perfiles y clusters de estructura productiva.
- Tipo de cluster: corte transversal (solo 2022).
- Jurisdicciones: todas las que tengan información disponible.
- Indicadores: core + complementarios de género y exportación.
- Georreferencia: se deja tabla de apoyo para mapas (`geo_referencia_para_mapas.csv`).

## 11) Notas sobre validación 2021-2022 (opcional)

Aunque la corrida objetivo quedó en 2022, con los mismos scripts se puede activar `run.years: [2021, 2022]` para análisis de estabilidad temporal básico (comparación de asignación de cluster entre años), sin modificar la arquitectura.
