from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sqlite3
import os

def test_database_connection():
    """Prueba simple de conexión a la base de datos"""
    db_path = '/opt/airflow/data/covid_dashboard.db'
    
    print("🧪 TESTING DATABASE CONNECTION")
    print("=" * 40)
    print(f"📁 DB Path: {db_path}")
    print(f"📄 File exists: {os.path.exists(db_path)}")
    
    if not os.path.exists(db_path):
        print("❌ Database file does not exist!")
        return {'status': 'file_not_found'}
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Verificar tablas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print("🗃️ Tables in database:")
        for table in tables:
            print(f"   - {table[0]}")
        
        # Crear tabla de prueba si no existe
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY,
                test_value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Insertar dato de prueba
        cursor.execute("INSERT INTO test_table (test_value) VALUES (?)", ("Hello from Airflow!",))
        conn.commit()
        
        # Leer dato de prueba
        cursor.execute("SELECT * FROM test_table ORDER BY id DESC LIMIT 1")
        result = cursor.fetchone()
        
        print(f"✅ Test data inserted: {result}")
        
        conn.close()
        return {'status': 'success', 'tables': [t[0] for t in tables]}
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return {'status': 'error', 'error': str(e)}

with DAG(
    'covid_test_database',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'database']
) as dag:
    
    test_task = PythonOperator(
        task_id='test_database',
        python_callable=test_database_connection
    )

# Ejecutar este DAG de prueba