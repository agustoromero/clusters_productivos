# Plan de Integración: Incorporación de Salarios a Clusters Productivos

## Resumen Ejecutivo
Este documento detalla el plan end-to-end para incorporar el archivo `departamento_series_empleo_y_salarios_mensual_sector2_0.csv` al pipeline de clusters productivos, incluyendo:
- Agregación temporal (promedio anual)
- Agregación geográfica (por provincia)
- Agregación sectorial (a 2 dígitos con match contra dicc_clanae2010_ciiu4)
- Deflactación con IPC para precios constantes
- Regla de completitud (>=6 meses)
- Actualización de scripts Python y R (01, 02, 03)

## Respuestas a Preguntas Críticas
1. **Base de reporte final**: No reescalar, mantener posición relativa en promedio anual para caracterizar clusters mediante niveles salariales relativos inter e intrasectoriales en 2022.
2. **Cobertura temporal productiva**: Datos productivos de interés son anuales para 2022.
3. **Resolución de empates en match sectorial**: Mayor frecuencia (elegir el sector 2d más frecuente en coincidencias).

## Paso a Paso de Implementación

### 1. Preparación de Datos
#### 1.1 Agregar archivo IPC a GitHub
```bash
# Desde el directorio raíz del proyecto
git add ipc.xlsx
git commit -m "data: agregar archivo IPC para deflactación de salarios"
git push origin main
```

#### 1.2 Actualizar params.json
Agregar rutas para los nuevos archivos:
```json
{
  "paths": {
    "raw_series": "departamento_series_empleo_y_salarios_mensual_sector2_0.csv",
    "raw_ipc": "ipc.xlsx",
    "dim_sector": "dicc_clanae2010_ciiu4.xls"
  }
}
```

### 2. Actualización de Scripts

#### 2.1 Script 01: Ingesta y Curado (Python y R)
**Cambios principales:**
- Cargar archivo series empleo-salarios
- Cargar IPC para deflactación
- Agregar match sectorial a 2 dígitos
- Calcular promedio anual deflactado (>=6 meses)

**Pseudocódigo Python:**
```python
def load_ipc_data(ipc_path):
    # Leer IPC con base 100 dic-2016
    ipc_df = pd.read_excel(ipc_path)
    ipc_df['fecha'] = pd.to_datetime(ipc_df['periodo'], format='%Y%m')
    return ipc_df.set_index('fecha')['ipc']

def deflactar_salario(salario_mensual, periodo, ipc_base):
    # Deflactar a precios constantes base dic-2016
    ipc_periodo = ipc_base.loc[periodo]
    return salario_mensual * (100 / ipc_periodo)

def aggregate_salarios_by_sector_provincia_anio(series_data, ipc_data, sector_mapping):
    # Agrupar por id_prov, sector_2d, anio
    # Calcular promedio anual si >=6 meses
    # Aplicar deflactación mensual antes del promedio
    pass
```

#### 2.2 Script 02: Features para Clusters (Python y R)
**Cambios principales:**
- Incluir features de salarios en los marts
- Agregar métricas derivadas (ratio salario/empleo, etc.)

#### 2.3 Script 03: Clustering e Interpretación (Python y R)
**Cambios principales:**
- Incluir variable salario en el modelado
- Actualizar interpretaciones de clusters

### 3. Validaciones y Testing
- Verificar match sectorial (cobertura >=90%)
- Validar deflactación (comparar con índices conocidos)
- Probar regla de completitud (>=6 meses)
- QA de agregaciones geográficas

### 4. Checklist Operativo
- [ ] Archivos subidos a GitHub
- [ ] Scripts actualizados y probados
- [ ] Pipeline ejecutado exitosamente
- [ ] Resultados validados
- [ ] Documentación actualizada

### 5. Riesgos y Mitigaciones
- **Match sectorial imperfecto**: Implementar validación manual y métricas de confianza
- **Datos faltantes**: Regla clara de >=6 meses, imputación opcional
- **Cambios en IPC**: Versionar archivo y documentar base

## Entregables
- Scripts actualizados (Python y R)
- Archivo IPC en repositorio
- Reporte de calidad de match sectorial
- Resultados de clusters con nueva variable
- Documentación técnica actualizada