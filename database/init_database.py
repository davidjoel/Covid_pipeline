# database/init_database.py
import sqlite3
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_database():
    """Inicializa la base de datos SQLite con todas las tablas necesarias"""
    
    db_path = '/opt/airflow/data/covid_dashboard.db'
    
    # Crear directorio si no existe
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    logger.info("🗃️ Inicializando base de datos COVID-19...")
    
    try:
        # 1. Tabla de datos globales actuales
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS current_global_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                updated INTEGER,
                cases INTEGER,
                deaths INTEGER,
                recovered INTEGER,
                active INTEGER,
                critical INTEGER,
                cases_per_million REAL,
                deaths_per_million REAL,
                tests_per_million REAL,
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        logger.info("✅ Tabla 'current_global_stats' creada/verificada")
        
        # 2. Tabla de datos por países
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS current_countries_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country TEXT,
                country_code TEXT,
                cases INTEGER,
                deaths INTEGER,
                recovered INTEGER,
                active INTEGER,
                critical INTEGER,
                tests INTEGER,
                population INTEGER,
                continent TEXT,
                income_level TEXT,
                region TEXT,
                outbreak_severity TEXT,
                cases_per_million REAL,
                deaths_per_million REAL,
                fatality_rate REAL,
                infection_rate REAL,
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        logger.info("✅ Tabla 'current_countries_stats' creada/verificada")
        
        # 3. Tabla de datos históricos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS historical_covid_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE,
                location TEXT,
                new_cases INTEGER,
                new_deaths INTEGER,
                total_cases INTEGER,
                total_deaths INTEGER,
                population INTEGER,
                aggregation_level TEXT,
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        logger.info("✅ Tabla 'historical_covid_data' creada/verificada")
        
        # 4. Tabla de métricas de calidad
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_quality_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_date DATE,
                quality_score REAL,
                total_datasets INTEGER,
                passed_checks INTEGER,
                total_checks INTEGER,
                detailed_report TEXT,
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        logger.info("✅ Tabla 'data_quality_metrics' creada/verificada")
        
        conn.commit()
        logger.info("🎯 Base de datos inicializada exitosamente")
        
        # Verificar las tablas creadas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        logger.info(f"📊 Tablas en la base de datos: {[table[0] for table in tables]}")
        
    except Exception as e:
        logger.error(f"❌ Error inicializando base de datos: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def check_database_status():
    """Verifica el estado de la base de datos"""
    db_path = '/opt/airflow/data/covid_dashboard.db'
    
    if not os.path.exists(db_path):
        return {'exists': False, 'tables': []}
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in cursor.fetchall()]
    
    conn.close()
    
    return {'exists': True, 'tables': tables}

if __name__ == "__main__":
    print("=== INICIALIZADOR DE BASE DE DATOS COVID-19 ===")
    initialize_database()
    
    status = check_database_status()
    print(f"📁 Base de datos existe: {status['exists']}")
    print(f"🗃️ Tablas creadas: {status['tables']}")