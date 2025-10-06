# 🦠 COVID-19 Data Pipeline

Pipeline de datos para monitoreo y análisis de la evolución pandémica del COVID-19.

## 📋 Descripción del Proyecto

Sistema ETL que ingesta, valida, transforma y almacena datos COVID-19 desde múltiples fuentes para alimentar dashboards analíticos.

## 🏗️ Arquitectura

### Diagrama de Flujo


### Componentes
1. **Extracción**: APIs en tiempo real + dataset histórico
2. **Validación**: Completitud, precisión, frescura
3. **Transformación**: Clasificación, agregaciones, métricas
4. **Almacenamiento**: SQLite con estructura normalizada
5. **Orquestación**: Airflow con ejecución diaria

## 📊 Fuentes de Datos

### Fuente 1: API COVID-19 (Tiempo Real)
- **Endpoint**: `https://disease.sh/v3/covid-19/`
- **Datos**: Casos globales, por países, históricos
- **Frecuencia**: Actualización continua

### Fuente 2: Our World in Data (Histórico)
- **URL**: `https://covid.ourworldindata.org/data/owid-covid-data.csv`
- **Datos**: Series temporales completas desde 2020
- **Frecuencia**: Actualización diaria

## 🎯 Métricas de Calidad

| Métrica | Umbral | Propósito |
|---------|--------|-----------|
| Completitud | ≥95% | Datos sin valores nulos críticos |
| Precisión | ≥98% | Valores consistentes y lógicos |
| Frescura | ≤24h | Datos actualizados |

## 🚀 Instalación y Uso

### Requisitos
- Python 3.8+
- Apache Airflow
- SQLite3

### Ejecución
```bash
# Instalar dependencias
pip install -r requirements.txt

# Inicializar base de datos
python database/init_db.py

# Ejecutar pipeline manualmente
python scripts/run_pipeline.py