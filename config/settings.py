# config/settings.py
import os

print("✅ config/settings.py cargado exitosamente")

# Configuración de fuentes de datos
DATA_SOURCES = {
    'covid_api': {
        'base_url': 'https://disease.sh/v3/covid-19',
        'endpoints': {
            'global': '/all',
            'countries': '/countries',
            'historical': '/historical'
        }
    },
    'historical_csv': {
        'url': 'https://covid.ourworldindata.org/data/owid-covid-data.csv',
        'local_path': '/opt/airflow/data/owid_covid_data.csv'
    }
}

# Configuración de base de datos
DATABASE_CONFIG = {
    'db_type': 'sqlite',
    'db_path': '/opt/airflow/data/covid_dashboard.db',
    'tables': {
        'current_global': 'current_global_stats',
        'current_countries': 'current_countries_stats',
        'historical_data': 'historical_covid_data',
        'quality_metrics': 'data_quality_metrics'
    }
}

# Parámetros de calidad de datos
QUALITY_THRESHOLDS = {
    'completeness': 0.95,
    'accuracy': 0.98,
    'freshness_hours': 24
}

print("✅ Configuración inicializada:")
print(f"   - API Base: {DATA_SOURCES['covid_api']['base_url']}")
print(f"   - DB Path: {DATABASE_CONFIG['db_path']}")