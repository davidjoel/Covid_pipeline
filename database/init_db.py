import sqlite3
import pandas as pd
import json
from datetime import datetime
import logging
import sys
import os
sys.path.append('/opt/airflow/scripts')
sys.path.append('/opt/airflow/config')
sys.path.append('/opt/airflow/database')

from config.settings import DATABASE_CONFIG

from config.settings import DATABASE_CONFIG

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self):
        self.db_path = DATABASE_CONFIG['db_path']
        self.tables = DATABASE_CONFIG['tables']
        self._init_database()
    
    def _init_database(self):
        """Inicializa la base de datos con las tablas necesarias"""
        conn = sqlite3.connect(self.db_path)
        
        # Tabla de datos globales actuales
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.tables['current_global']} (
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
        """)
        
        # Tabla de datos por países
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.tables['current_countries']} (
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
        """)
        
        # Tabla de datos históricos
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.tables['historical_data']} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE,
                location TEXT,
                new_cases INTEGER,
                new_deaths INTEGER,
                total_cases INTEGER,
                total_deaths INTEGER,
                population INTEGER,
                aggregation_level TEXT, -- daily, weekly, monthly
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Tabla de métricas de calidad
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.tables['quality_metrics']} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_date DATE,
                quality_score REAL,
                total_datasets INTEGER,
                passed_checks INTEGER,
                total_checks INTEGER,
                detailed_report TEXT,
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info("✅ Base de datos inicializada")
    
    def load_all_data(self, transformed_data, quality_report):
        """Carga todos los datos transformados"""
        load_results = {
            'tables_updated': [],
            'total_records': 0,
            'load_timestamp': datetime.now()
        }
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Cargar datos globales actuales
            if 'global_processed' in transformed_data:
                global_data = transformed_data['global_processed']
                self._load_global_data(conn, global_data)
                load_results['tables_updated'].append('global_data')
                load_results['total_records'] += 1
            
            # Cargar datos por países
            if 'countries_processed' in transformed_data:
                countries_data = transformed_data['countries_processed']
                records_loaded = self._load_countries_data(conn, countries_data)
                load_results['tables_updated'].append('countries_data')
                load_results['total_records'] += records_loaded
            
            # Cargar datos históricos
            if 'historical_processed' in transformed_data:
                historical_data = transformed_data['historical_processed']
                records_loaded = self._load_historical_data(conn, historical_data)
                load_results['tables_updated'].append('historical_data')
                load_results['total_records'] += records_loaded
            
            # Cargar métricas de calidad
            self._load_quality_metrics(conn, quality_report)
            load_results['tables_updated'].append('quality_metrics')
            
            conn.commit()
            logger.info(f"✅ Datos cargados: {load_results['total_records']} registros")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error cargando datos: {e}")
            raise
        finally:
            conn.close()
        
        return load_results
    
    def _load_global_data(self, conn, global_data):
        """Carga datos globales actuales"""
        cursor = conn.cursor()
        
        # Limpiar tabla antes de insertar (solo mantener último registro)
        cursor.execute(f"DELETE FROM {self.tables['current_global']}")
        
        cursor.execute(f"""
            INSERT INTO {self.tables['current_global']} 
            (updated, cases, deaths, recovered, active, critical)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            global_data.get('updated'),
            global_data.get('cases'),
            global_data.get('deaths'),
            global_data.get('recovered'),
            global_data.get('active'),
            global_data.get('critical')
        ))
    
    def _load_countries_data(self, conn, countries_data):
        """Carga datos por países"""
        # Limpiar tabla antes de insertar
        conn.execute(f"DELETE FROM {self.tables['current_countries']}")
        
        # Preparar datos para inserción
        records = []
        for _, row in countries_data.iterrows():
            records.append((
                row.get('country'),
                row.get('countryInfo', {}).get('iso3') if isinstance(row.get('countryInfo'), dict) else None,
                row.get('cases'),
                row.get('deaths'),
                row.get('recovered'),
                row.get('active'),
                row.get('critical'),
                row.get('tests'),
                row.get('population'),
                row.get('continent'),
                row.get('income_level'),
                row.get('region'),
                row.get('outbreak_severity'),
                row.get('cases_per_million'),
                row.get('deaths_per_million'),
                row.get('fatality_rate'),
                row.get('infection_rate')
            ))
        
        # Insertar en lote
        cursor = conn.cursor()
        cursor.executemany(f"""
            INSERT INTO {self.tables['current_countries']} 
            (country, country_code, cases, deaths, recovered, active, critical, 
             tests, population, continent, income_level, region, outbreak_severity,
             cases_per_million, deaths_per_million, fatality_rate, infection_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records)
        
        return len(records)