# scripts/database_reporter.py
import sqlite3
import pandas as pd
from datetime import datetime
import os

def generate_database_report():
    """Genera un reporte completo de la base de datos"""
    
    db_path = '/opt/airflow/data/covid_dashboard.db'
    
    if not os.path.exists(db_path):
        print("❌ Base de datos no encontrada:", db_path)
        return
    
    conn = sqlite3.connect(db_path)
    
    print("=" * 60)
    print("📊 REPORTE DE BASE DE DATOS COVID-19")
    print("=" * 60)
    print(f"📅 Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Base de datos: {db_path}")
    print()
    
    # 1. Información de tablas
    print("🗃️ TABLAS EN LA BASE DE DATOS:")
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
    print(tables.to_string(index=False))
    print()
    
    # 2. Conteo de registros por tabla
    print("📈 ESTADÍSTICAS DE REGISTROS:")
    counts_data = []
    for table in tables['name']:
        count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table};", conn).iloc[0]['count']
        counts_data.append({'Tabla': table, 'Registros': count})
    
    counts_df = pd.DataFrame(counts_data)
    print(counts_df.to_string(index=False))
    print()
    
    # 3. Datos globales actuales
    print("🌍 DATOS GLOBALES ACTUALES:")
    try:
        global_data = pd.read_sql("SELECT * FROM current_global_stats;", conn)
        if not global_data.empty:
            print(global_data.to_string(index=False))
        else:
            print("   No hay datos globales")
    except Exception as e:
        print(f"   Error: {e}")
    print()
    
    # 4. Top países
    print("🏆 TOP 10 PAÍSES POR CASOS:")
    try:
        countries_data = pd.read_sql("""
            SELECT country, cases, deaths, recovered, cases_per_million, fatality_rate
            FROM current_countries_stats 
            ORDER BY cases DESC 
            LIMIT 10;
        """, conn)
        print(countries_data.to_string(index=False))
    except Exception as e:
        print(f"   Error: {e}")
    print()
    
    # 5. Métricas de calidad recientes
    print("✅ MÉTRICAS DE CALIDAD (últimas 5 ejecuciones):")
    try:
        quality_data = pd.read_sql("""
            SELECT execution_date, quality_score, passed_checks, total_checks
            FROM data_quality_metrics 
            ORDER BY execution_date DESC 
            LIMIT 5;
        """, conn)
        print(quality_data.to_string(index=False))
    except Exception as e:
        print(f"   Error: {e}")
    
    conn.close()
    print()
    print("🎯 REPORTE COMPLETADO")

if __name__ == "__main__":
    generate_database_report()