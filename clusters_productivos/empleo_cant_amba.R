library(tidyverse)
library(readxl)
library(sf)
library(scales)
library(magick)
library(grid)
library(ggplot2)
library(patchwork)
library(lubridate)

# =====================================================
# 1. BASE
# =====================================================

var_empleo_amba <- read_excel(
  "SIPA por provincia y sector.xlsx",
  sheet = "var_empleo_amba",
  .name_repair = "minimal"
)

names(var_empleo_amba) <- make.names(
  names(var_empleo_amba),
  unique = TRUE
)

# =====================================================
# 2. SHAPE
# =====================================================

departamento_polygon <- st_read(
  "departamento/departamentoPolygon.shp",
  quiet = TRUE
)

# =====================================================
# 3. CORREGIR ENCODING
# =====================================================

departamento_polygon$nam <- iconv(
  departamento_polygon$nam,
  from = "latin1",
  to = "UTF-8"
)

# =====================================================
# 4. FUNCIÓN LIMPIEZA
# =====================================================

limpiar_texto <- function(x) {
  
  x %>%
    iconv(from = "UTF-8", to = "ASCII//TRANSLIT") %>%
    str_to_upper() %>%
    str_replace_all("[^A-Z0-9 ]", "") %>%
    str_squish()
  
}

# =====================================================
# 5. DEFINICIÓN AMBA
# =====================================================

amba_41 <- c(
  "ALMIRANTE BROWN",
  "AVELLANEDA",
  "BERAZATEGUI",
  "BERISSO",
  "BRANDSEN",
  "CAMPANA",
  "CANUELAS",
  "ENSENADA",
  "ESCOBAR",
  "ESTEBAN ECHEVERRIA",
  "EXALTACION DE LA CRUZ",
  "EZEIZA",
  "FLORENCIO VARELA",
  "GENERAL LAS HERAS",
  "GENERAL RODRIGUEZ",
  "GENERAL SAN MARTIN",
  "HURLINGHAM",
  "ITUZAINGO",
  "JOSE C PAZ",
  "LA MATANZA",
  "LANUS",
  "LA PLATA",
  "LOMAS DE ZAMORA",
  "LUJAN",
  "MALVINAS ARGENTINAS",
  "MARCOS PAZ",
  "MERLO",
  "MORENO",
  "MORON",
  "PILAR",
  "PRESIDENTE PERON",
  "QUILMES",
  "SAN FERNANDO",
  "SAN ISIDRO",
  "SAN MIGUEL",
  "SAN VICENTE",
  "TIGRE",
  "TRES DE FEBRERO",
  "VICENTE LOPEZ",
  "ZARATE"
)

# =====================================================
# 6. LIMPIAR SHAPE
# =====================================================

departamento_polygon <- departamento_polygon %>%
  mutate(
    nam_limpio = limpiar_texto(nam)
  )

# =====================================================
# 7. CABA UNIFICADA
# =====================================================

caba_union <- departamento_polygon %>%
  filter(str_detect(nam, "Comuna")) %>%
  summarise() %>%
  mutate(
    nam = "CABA",
    nam_limpio = "CABA",
    in1 = "02000",
    codigo_merge = "2000"
  )

# =====================================================
# 8. AMBA BUENOS AIRES
# =====================================================

amba_ba <- departamento_polygon %>%
  mutate(
    provincia = str_sub(
      as.character(in1),
      1,
      2
    ),
    
    codigo_merge = str_sub(
      as.character(in1),
      -4
    )
  ) %>%
  
  filter(
    provincia == "06"
  ) %>%
  
  filter(
    nam_limpio %in% amba_41
  ) %>%
  
  distinct(
    codigo_merge,
    .keep_all = TRUE
  )

# =====================================================
# 9. UNIR AMBA + CABA
# =====================================================

amba_shape <- bind_rows(
  amba_ba,
  caba_union
)
# =====================================================
# 10. BASE EMPLEO
# =====================================================

var_empleo_amba <- var_empleo_amba %>%
  
  mutate(
    
    codigo_merge = str_pad(
      as.character(Código5.),
      width = 4,
      pad = "0"
    )
    
  ) %>%
  
  distinct(
    codigo_merge,
    .keep_all = TRUE
  )

# =====================================================
# 10.1 VER COLUMNAS
# =====================================================

names(var_empleo_amba)

# =====================================================
# 10.2 IDENTIFICAR COLUMNAS FECHA
# =====================================================

columnas_fecha <- names(var_empleo_amba) %>%
  
  str_subset("^X[0-9]+\\.?[0-9]*$")

columnas_fecha

# =====================================================
# 10.3 EXTRAER SERIALES
# =====================================================

seriales_excel <- columnas_fecha %>%
  
  str_remove("^X") %>%
  str_remove("\\.0$") %>%
  
  as.numeric()

seriales_excel

# =====================================================
# 10.4 CONVERTIR A FECHA
# =====================================================

fechas_reales <- as.Date(
  seriales_excel,
  origin = "1899-12-30"
)

fechas_reales

# =====================================================
# 10.5 CREAR NOMBRES YYYY_MM
# =====================================================

nuevos_nombres <- format(
  fechas_reales,
  "%Y_%m"
)

nuevos_nombres
# =====================================================
# 10.6 RENOMBRAR
# =====================================================

posiciones <- match(
  columnas_fecha,
  names(var_empleo_amba)
)

names(var_empleo_amba)[posiciones] <- nuevos_nombres

# =====================================================
# 10.7 LIMPIAR NOMBRES
# =====================================================

names(var_empleo_amba) <- make.names(
  names(var_empleo_amba),
  unique = TRUE
)

names(var_empleo_amba) <- names(var_empleo_amba) %>%
  
  str_replace(
    "^X(?=20)",
    ""
  )

# =====================================================
# 10.8 CHEQUEO FINAL
# =====================================================

names(var_empleo_amba)

# =====================================================
# 10.9 CREAR VARIABLES
# =====================================================

mapa_base <- var_empleo_amba %>%
  
  mutate(
    
    empleo_dic2023 = `2023_12`,
    
    empleo_sep2025 = `2025_09`,
    
    variacion_abs = (
      empleo_sep2025 -
        empleo_dic2023
    ),
    
    variacion_pct = (
      variacion_abs /
        empleo_dic2023
    ) * 100
    
  )

# =====================================================
# 10.10 CONTROL
# =====================================================

mapa_base %>%
  
  select(
    Departamento,
    empleo_dic2023,
    empleo_sep2025,
    variacion_abs,
    variacion_pct
  ) %>%
  
  print(n = 10)
# =====================================================
# 11. CONTROLES
# =====================================================

cat(
  "Departamentos shape:",
  nrow(amba_shape),
  "\n"
)

cat(
  "Departamentos base:",
  n_distinct(mapa_base$codigo_merge),
  "\n"
)

cat(
  "Faltan en shape:\n"
)

setdiff(
  mapa_base$codigo_merge,
  amba_shape$codigo_merge
)

cat(
  "Sobran en shape:\n"
)

setdiff(
  amba_shape$codigo_merge,
  mapa_base$codigo_merge
)

# =====================================================
# 12. MERGE
# =====================================================

mapa_amba <- amba_shape %>%
  
  left_join(
    mapa_base,
    by = "codigo_merge"
  )

# =====================================================
# 13. CONTROL FINAL
# =====================================================

cat(
  "Departamentos matcheados:",
  sum(!is.na(mapa_amba$Departamento)),
  "\n"
)

# =====================================================
# 14. VARIABLES DISPONIBLES
# =====================================================

names(mapa_amba)

# =====================================================
# 15. TRAMOS
# =====================================================

niveles_tramo <- c(
  "≤ -4 mil",
  "-4 mil a -2 mil",
  "-2 mil a -1 mil",
  "-1 mil a 0",
  "0 a 1 mil",
  "> 1 mil"
)

colores_tramos <- c(
  "≤ -4 mil"        = "#67000D",
  "-4 mil a -2 mil" = "#CB181D",
  "-2 mil a -1 mil" = "#FB6A4A",
  "-1 mil a 0"      = "#FCAE91",
  "0 a 1 mil"       = "#A1D99B",
  "> 1 mil"         = "#238B45"
)

mapa_amba <- mapa_amba %>%
  
  mutate(
    
    tramo = cut(
      variacion_abs,
      
      breaks = c(
        -Inf,
        -4000,
        -2000,
        -1000,
        0,
        1000,
        Inf
      ),
      
      labels = niveles_tramo,
      include.lowest = TRUE
    ),
    
    tramo = factor(
      tramo,
      levels = niveles_tramo
    )
    
  )

# =====================================================
# 16. LOGO
# =====================================================
# 
# logo <- image_read("logon.png") %>%
#   image_trim()
# 
# logo_grob <- rasterGrob(
#   as.raster(logo),
#   interpolate = TRUE
# )

# =====================================================
# 16.1 REPROYECTAR
# =====================================================

mapa_amba <- st_transform(
  mapa_amba,
  22185
)

# =====================================================
# 16.2 TABLA LATERAL
# =====================================================

tabla_perdida <- mapa_amba %>%
  
  st_drop_geometry() %>%
  
  arrange(variacion_abs) %>%
  
  mutate(
    
    ranking = row_number(),
    
    texto = paste0(
      ranking,
      ". ",
      Departamento,
      "  ",
      ifelse(
        variacion_abs > 0,
        "+",
        ""
      ),
      comma(
        variacion_abs,
        accuracy = 1,
        big.mark = ".",
        decimal.mark = ","
      )
    )
    
  )

grafico_tabla <- ggplot(
  tabla_perdida,
  aes(
    y = reorder(texto, -ranking),
    x = 1
  )
) +
  
  geom_text(
    aes(label = texto),
    hjust = 0,
    size = 3.4,
    family = "sans",
    color = "#1a1a1a"
  ) +
  
  labs(
    title = "Departamentos\nordenados por variación"
  ) +
  
  theme_void() +
  
  theme(
    
    plot.title = element_text(
      size = 14,
      face = "bold",
      hjust = 0
    ),
    
    plot.margin = margin(
      15,
      10,
      15,
      10
    )
    
  ) +
  
  xlim(1, 2.6)

# =====================================================
# 17. MAPA
# =====================================================

mapa_base_plot <- ggplot(mapa_amba) +
  
  geom_sf(
    aes(fill = tramo),
    color = "white",
    size = 0.2
  ) +
  
  geom_sf_text(
    aes(label = Departamento),
    size = 2.3,
    fontface = "bold",
    color = "#001f47",
    check_overlap = TRUE
  ) +
  
  scale_fill_manual(
    values = colores_tramos,
    drop = FALSE,
    name = "Variación empleo"
  ) +
  
  coord_sf(expand = FALSE) +
  
  labs(
    title = "Variación del empleo registrado",
    subtitle = "AMBA por departamento"
  ) +
  
  theme_void() +
  
  theme(
    
    plot.background = element_rect(
      fill = "white",
      color = NA
    ),
    
    panel.background = element_rect(
      fill = "white",
      color = NA
    ),
    
    plot.title = element_text(
      size = 22,
      face = "bold",
      hjust = 0.5
    ),
    
    plot.subtitle = element_text(
      size = 15,
      hjust = 0.5
    ),
    
    legend.title = element_text(size = 14),
    
    legend.text = element_text(size = 12),
    
    legend.position = "bottom"
  )
  # ) +
  # 
  # inset_element(
  #   logo_grob,
  #   left = 0.72,
  #   bottom = 0.02,
  #   right = 0.98,
  #   top = 0.18,
  #   align_to = "full"
  # )

# =====================================================
# 17.1 COMBINAR MAPA + TABLA
# =====================================================

mapa_empleo_final <- mapa_base_plot +
  
  grafico_tabla +
  
  plot_layout(
    widths = c(2.3, 1)
  )

# =====================================================
# 18. EXPORTAR
# =====================================================

ggsave(
  "mapa_amba_variacion_abs.png",
  mapa_empleo_final,
  width = 14,
  height = 10,
  dpi = 300,
  bg = "white"
)

ggsave(
  "mapa_amba_variacion_abs.svg",
  mapa_empleo_final,
  width = 14,
  height = 10,
  bg = "white"
)