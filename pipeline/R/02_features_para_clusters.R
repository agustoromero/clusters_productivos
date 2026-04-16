#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(tidyverse)
  library(yaml)
  library(arrow)
})

cfg <- yaml::read_yaml("pipeline/config/params.yml")
dir.create(cfg$paths$marts_dir, recursive = TRUE, showWarnings = FALSE)

estab <- arrow::read_parquet(file.path(cfg$paths$curated_dir, "estab_curado.parquet"))
gender <- arrow::read_parquet(file.path(cfg$paths$curated_dir, "gender_curado.parquet"))

nvl <- function(x, default = 0) {
  if (is.null(x)) return(default)
  x
}

build_features <- function(df, level_geo = c("provincia", "departamento"), level_activity = "clae2") {
  level_geo <- match.arg(level_geo)

  geo_cols <- if (level_geo == "provincia") c("provincia_id", "provincia") else c("provincia_id", "provincia", "in_departamentos", "departamento")

  agg <- df |>
    group_by(across(all_of(c("anio", geo_cols, level_activity)))) |>
    summarise(
      empleo = sum(empleo, na.rm = TRUE),
      establecimientos = sum(establecimientos, na.rm = TRUE),
      exportadoras = sum(empresas_exportadoras, na.rm = TRUE),
      .groups = "drop"
    )

  tot <- agg |>
    group_by(across(all_of(c("anio", geo_cols)))) |>
    summarise(
      empleo_total = sum(empleo, na.rm = TRUE),
      estab_total = sum(establecimientos, na.rm = TRUE),
      exp_total = sum(exportadoras, na.rm = TRUE),
      .groups = "drop"
    )

  agg |>
    left_join(tot, by = c("anio", geo_cols)) |>
    mutate(
      share_empleo = if_else(empleo_total > 0, empleo / empleo_total, 0),
      densidad_estab = if_else(empleo > 0, establecimientos / empleo, 0),
      intensidad_export = if_else(establecimientos > 0, exportadoras / establecimientos, 0)
    )
}

for (lg in cfg$run$level_geo) {
  feats <- build_features(estab, level_geo = lg, level_activity = cfg$run$level_activity)
  arrow::write_parquet(feats, file.path(cfg$paths$marts_dir, paste0("features_", lg, ".parquet")))

  if (isTRUE(cfg$run$include_gender_module)) {
    gmodule <- gender |>
      group_by(anio, across(any_of(c("provincia_id", "provincia", "in_departamentos", "departamento", cfg$run$level_activity))), genero) |>
      summarise(empleo = sum(empleo, na.rm = TRUE), .groups = "drop") |>
      tidyr::pivot_wider(names_from = genero, values_from = empleo, values_fill = 0)

    mujeres <- if ("Mujeres" %in% names(gmodule)) gmodule$Mujeres else 0
    varones <- if ("Varones" %in% names(gmodule)) gmodule$Varones else 0

    gmodule <- gmodule |>
      mutate(
        brecha_genero_abs = abs(nvl(mujeres) - nvl(varones)),
        ratio_fem = nvl(mujeres) / pmax(nvl(mujeres) + nvl(varones), 1)
      )

    arrow::write_parquet(gmodule, file.path(cfg$paths$marts_dir, paste0("features_genero_", lg, ".parquet")))
  }
}

message("Features listos en: ", cfg$paths$marts_dir)
