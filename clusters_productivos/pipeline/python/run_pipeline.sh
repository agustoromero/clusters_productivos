#!/usr/bin/env bash
set -euo pipefail

python pipeline/python/01_ingesta_y_curado.py
python pipeline/python/02_features_para_clusters.py
python pipeline/python/03_clustering_e_interpretacion.py

echo "Pipeline finalizado"
