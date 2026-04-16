#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(tidyverse)
  library(yaml)
  library(janitor)
  library(arrow)
})

cfg <- yaml::read_yaml("pipeline/config/params.yml")

dir.create(cfg$paths$curated_dir, recursive = TRUE, showWarnings = FALSE)

read_csv_typed <- function(path) {
  readr::read_csv(path, show_col_types = FALSE) |>
    janitor::clean_names()
}

estab <- read_csv_typed(cfg$paths$raw_estab) |>
  mutate(
    anio = as.integer(anio),
    in_departamentos = as.integer(in_departamentos),
    provincia_id = as.integer(provincia_id),
    clae6 = as.character(clae6),
    clae2 = as.character(clae2),
    empleo = as.numeric(empleo),
    establecimientos = as.numeric(establecimientos),
    empresas_exportadoras = as.numeric(empresas_exportadoras)
  ) |>
  filter(anio >= cfg$run$year_min, anio <= cfg$run$year_max)

gender <- read_csv_typed(cfg$paths$raw_gender) |>
  mutate(
    anio = as.integer(anio),
    in_departamentos = as.integer(in_departamentos),
    provincia_id = as.integer(provincia_id),
    clae6 = as.character(clae6),
    clae2 = as.character(clae2),
    empleo = as.numeric(empleo),
    establecimientos = as.numeric(establecimientos),
    empresas_exportadoras = as.numeric(empresas_exportadoras)
  ) |>
  filter(anio >= cfg$run$year_min, anio <= cfg$run$year_max)

geo <- read_csv_typed(cfg$paths$dim_geo) |>
  mutate(
    in_departamentos = as.integer(in_departamentos),
    provincia_id = as.integer(provincia_id)
  )

act <- read_csv_typed(cfg$paths$dim_activity) |>
  mutate(
    clae6 = as.character(clae6),
    clae2 = as.character(clae2)
  )

estab_cur <- estab |>
  left_join(geo, by = c("provincia_id", "in_departamentos", "departamento", "provincia")) |>
  left_join(act, by = c("clae6", "clae2", "letra"))

gender_cur <- gender |>
  left_join(geo, by = c("provincia_id", "in_departamentos", "departamento", "provincia")) |>
  left_join(act, by = c("clae6", "clae2", "letra"))

arrow::write_parquet(estab_cur, file.path(cfg$paths$curated_dir, "estab_curado.parquet"))
arrow::write_parquet(gender_cur, file.path(cfg$paths$curated_dir, "gender_curado.parquet"))

message("Curado listo en: ", cfg$paths$curated_dir)
