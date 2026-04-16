#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(tidyverse)
  library(yaml)
  library(arrow)
  library(cluster)
})

cfg <- yaml::read_yaml("pipeline/config/params.yml")
dir.create(cfg$paths$clusters_dir, recursive = TRUE, showWarnings = FALSE)

scale_features <- function(df, vars, method = "zscore") {
  x <- as.matrix(df[, vars])
  if (method == "robust") {
    med <- apply(x, 2, median, na.rm = TRUE)
    madv <- apply(x, 2, mad, na.rm = TRUE)
    x <- sweep(x, 2, med, "-")
    x <- sweep(x, 2, pmax(madv, 1e-8), "/")
  } else {
    x <- scale(x)
  }
  x
}

for (lg in cfg$run$level_geo) {
  feats <- arrow::read_parquet(file.path(cfg$paths$marts_dir, paste0("features_", lg, ".parquet")))

  agg_geo <- feats |>
    group_by(across(any_of(c("provincia_id", "provincia", "in_departamentos", "departamento")))) |>
    summarise(
      empleo = mean(empleo, na.rm = TRUE),
      establecimientos = mean(establecimientos, na.rm = TRUE),
      exportadoras = mean(exportadoras, na.rm = TRUE),
      share_empleo = mean(share_empleo, na.rm = TRUE),
      densidad_estab = mean(densidad_estab, na.rm = TRUE),
      intensidad_export = mean(intensidad_export, na.rm = TRUE),
      .groups = "drop"
    )

  vars <- c("empleo", "establecimientos", "exportadoras", "share_empleo", "densidad_estab", "intensidad_export")
  x <- scale_features(agg_geo, vars, cfg$run$scale_method)

  if (cfg$run$cluster_method == "kmeans") {
    best <- list(k = NA, score = -Inf, model = NULL)
    for (k in cfg$run$k_min:cfg$run$k_max) {
      set.seed(cfg$run$random_seed)
      km <- kmeans(x, centers = k, nstart = 25)
      sil <- cluster::silhouette(km$cluster, dist(x))
      score <- mean(sil[, 3])
      if (score > best$score) best <- list(k = k, score = score, model = km)
    }
    agg_geo$cluster <- best$model$cluster
    agg_geo$cluster_method <- paste0("kmeans_k", best$k)
  } else {
    hc <- hclust(dist(x), method = "ward.D2")
    k <- cfg$run$k_min
    agg_geo$cluster <- cutree(hc, k = k)
    agg_geo$cluster_method <- paste0("hclust_ward_k", k)
  }

  out <- file.path(cfg$paths$clusters_dir, paste0("clusters_", lg, ".csv"))
  readr::write_csv(agg_geo, out)
}

message("Clusters generados en: ", cfg$paths$clusters_dir)
