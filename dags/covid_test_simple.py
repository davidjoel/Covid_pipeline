from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def extract_data():
    """Tarea de extracción simple"""
    logger.info("🔍 Extrayendo datos de COVID-19...")
    return {"casos_totales": 1000000, "muertes_totales": 50000, "paises": 180}

def transform_data(**kwargs):
    """Tarea de transformación simple"""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    
    logger.info("🔄 Transformando datos...")
    data['tasa_mortalidad'] = (data['muertes_totales'] / data['casos_totales']) * 100
    data['fecha_procesamiento'] = datetime.now().isoformat()
    
    return data

def load_data(**kwargs):
    """Tarea de carga simple"""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_data')
    
    logger.info("💾 Cargando datos...")
    logger.info(f"Datos procesados: {data}")
    
    return {"status": "success", "registros_procesados": 1}

# Configuración del DAG
default_args = {
    'owner': 'covid_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'covid_etl_simple',
    default_args=default_args,
    description='ETL simple para datos COVID-19',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['covid', 'test']
)

# Definir tareas
start = DummyOperator(task_id='start', dag=dag)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

# Definir dependencias
start >> extract_task >> transform_task >> load_task >> end
