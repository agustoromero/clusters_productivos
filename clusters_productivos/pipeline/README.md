# Pipeline SIPA clusters productivos (implementación ejecutable)

Este pipeline implementa el desarrollo pedido para **2022**, con actividad a **CLAE2**, módulos **provincia** y **departamento**, y selección automática de `k` con **silhouette**.

## Requisitos

- Python 3.10+ (sin dependencias externas).

## Ejecución end-to-end

```bash
bash pipeline/python/run_pipeline.sh
```

También se puede correr por etapas:

```bash
python pipeline/python/01_ingesta_y_curado.py
python pipeline/python/02_features_para_clusters.py
python pipeline/python/03_clustering_e_interpretacion.py
```

## Parámetros

Configurar en `pipeline/config/params.json`:
- años (`run.years`)
- nivel geográfico (`run.level_geo`)
- nivel de actividad (`run.level_activity`)
- método de escalado (`run.scale_method`)
- rango de búsqueda de `k` (`run.k_min`, `run.k_max`)

## Salidas

### Curated
- `pipeline/output/curated/estab_curado.csv`
- `pipeline/output/curated/gender_curado.csv`

### Marts
- `pipeline/output/marts/features_provincia.csv`
- `pipeline/output/marts/features_departamento.csv`
- `pipeline/output/marts/features_genero_provincia.csv`
- `pipeline/output/marts/features_genero_departamento.csv`
- `pipeline/output/marts/geo_referencia_para_mapas.csv`

### Clusters
- `pipeline/output/clusters/clusters_provincia.csv`
- `pipeline/output/clusters/clusters_departamento.csv`
- `pipeline/output/clusters/perfiles_provincia.csv`
- `pipeline/output/clusters/perfiles_departamento.csv`
- `pipeline/output/clusters/metricas_modelo_provincia.json`
- `pipeline/output/clusters/metricas_modelo_departamento.json`

### Calidad
- `pipeline/output/quality/quality_ingesta.json`
