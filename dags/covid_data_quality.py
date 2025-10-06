from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from io import StringIO

def generate_data_quality_report():
    """Genera reporte completo de calidad de datos"""
    
    # Conectar a la base de datos
    conn = sqlite3.connect('/tmp/covid_dashboard.db')
    
    # Leer métricas de calidad
    quality_df = pd.read_sql("SELECT * FROM data_quality_metrics ORDER BY execution_date DESC LIMIT 10", conn)
    
    # Generar reporte HTML
    report_html = """
    <html>
    <head>
        <title>COVID-19 Data Quality Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .metric { background: #f5f5f5; padding: 20px; margin: 10px 0; border-radius: 5px; }
            .good { border-left: 5px solid #4CAF50; }
            .warning { border-left: 5px solid #FFC107; }
            .error { border-left: 5px solid #F44336; }
        </style>
    </head>
    <body>
        <h1>📊 COVID-19 Data Quality Report</h1>
    """
    
    # Métricas de completitud
    completeness = quality_df['quality_score'].mean()
    status_class = "good" if completeness > 0.9 else "warning" if completeness > 0.7 else "error"
    
    report_html += f"""
    <div class="metric {status_class}">
        <h2>📈 Completitud General</h2>
        <p>Score promedio: <strong>{completeness:.2%}</strong></p>
        <p>Estado: {'✅ Excelente' if completeness > 0.9 else '⚠️ Aceptable' if completeness > 0.7 else '❌ Necesita Mejora'}</p>
    </div>
    """
    
    # Tendencia de calidad
    report_html += """
    <div class="metric">
        <h2>📅 Tendencia de Calidad</h2>
    """
    
    # Generar gráfico de tendencia
    plt.figure(figsize=(10, 6))
    plt.plot(quality_df['execution_date'], quality_df['quality_score'], marker='o')
    plt.title('Evolución del Score de Calidad')
    plt.xlabel('Fecha')
    plt.ylabel('Score de Calidad')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig('/tmp/quality_trend.png')
    plt.close()
    
    report_html += '<img src="/tmp/quality_trend.png" width="100%">'
    report_html += "</div>"
    
    # Detalles por dataset
    report_html += """
    <div class="metric">
        <h2>🔍 Detalles por Dataset</h2>
    """
    
    # Aquí iría análisis detallado por dataset
    report_html += "<p>Análisis detallado de completitud, precisión y frescura por cada fuente de datos.</p>"
    report_html += "</div>"
    
    report_html += """
        <p><em>Reporte generado automáticamente el: """ + datetime.now().strftime('%Y-%m-%d %H:%M') + """</em></p>
    </body>
    </html>
    """
    
    # Guardar reporte
    with open('/tmp/data_quality_report.html', 'w') as f:
        f.write(report_html)
    
    conn.close()
    return report_html