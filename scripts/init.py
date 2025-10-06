# scripts/__init__.py
import sys
import os

# Configurar paths GLOBALMENTE para todos los scripts
sys.path.insert(0, '/opt/airflow/config')
sys.path.insert(0, '/opt/airflow/scripts')
sys.path.insert(0, '/opt/airflow/database')

print("✅ Scripts package initialized - Paths configured")
