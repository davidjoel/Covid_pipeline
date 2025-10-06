import pandas as pd
import numpy as np
from datetime import datetime
import logging

# ✅ AGREGAR ESTO
import sys
import os
sys.path.append('/opt/airflow/scripts')
sys.path.append('/opt/airflow/config')

from config.settings import QUALITY_THRESHOLDS

from config.settings import QUALITY_THRESHOLDS

logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self):
        self.thresholds = QUALITY_THRESHOLDS
        self.quality_report = {}
    
    def validate_completeness(self, df, dataset_name):
        """Valida completitud de los datos"""
        total_records = len(df)
        null_counts = df.isnull().sum()
        completeness_ratio = 1 - (null_counts.sum() / (total_records * len(df.columns)))
        
        validation_passed = completeness_ratio >= self.thresholds['completeness']
        
        self.quality_report[dataset_name] = {
            'completeness_ratio': completeness_ratio,
            'null_counts': null_counts.to_dict(),
            'total_records': total_records,
            'completeness_passed': validation_passed
        }
        
        logger.info(f"Completitud {dataset_name}: {completeness_ratio:.2%}")
        return validation_passed
    
    def validate_freshness(self, data, dataset_name):
        """Valida frescura de los datos"""
        if dataset_name == 'historical':
            # Para datos históricos, verificar la fecha más reciente
            latest_date = pd.to_datetime(data['date']).max()
            days_since_update = (datetime.now() - latest_date).days
            freshness_passed = days_since_update <= 1  # Máximo 1 día de antigüedad
        else:
            # Para datos de API, verificar timestamp de actualización
            update_time = datetime.fromtimestamp(data.get('updated', 0) / 1000)
            hours_since_update = (datetime.now() - update_time).total_seconds() / 3600
            freshness_passed = hours_since_update <= self.thresholds['freshness_hours']
        
        self.quality_report[dataset_name]['freshness'] = freshness_passed
        logger.info(f"Frescura {dataset_name}: {'✅' if freshness_passed else '❌'}")
        return freshness_passed
    
    def validate_accuracy(self, df, dataset_name):
        """Valida precisión y consistencia de los datos"""
        accuracy_issues = []
        
        # Verificar que no hay valores negativos en métricas clave
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        negative_values = (df[numeric_columns] < 0).sum().sum()
        
        if negative_values > 0:
            accuracy_issues.append(f"Valores negativos encontrados: {negative_values}")
        
        # Verificar consistencia en relaciones lógicas
        if 'deaths' in df.columns and 'cases' in df.columns:
            death_rate_anomalies = (df['deaths'] > df['cases']).sum()
            if death_rate_anomalies > 0:
                accuracy_issues.append(f"Muertes > Casos: {death_rate_anomalies} registros")
        
        accuracy_passed = len(accuracy_issues) == 0
        
        self.quality_report[dataset_name]['accuracy_issues'] = accuracy_issues
        self.quality_report[dataset_name]['accuracy_passed'] = accuracy_passed
        
        logger.info(f"Precisión {dataset_name}: {'✅' if accuracy_passed else '❌'}")
        return accuracy_passed
    
    def generate_quality_summary(self):
        """Genera resumen de calidad"""
        total_checks = len(self.quality_report) * 3  # 3 validaciones por dataset
        passed_checks = sum([
            1 for dataset in self.quality_report.values() 
            if all([dataset.get('completeness_passed', False),
                   dataset.get('freshness', False),
                   dataset.get('accuracy_passed', False)])
        ])
        
        quality_score = passed_checks / total_checks if total_checks > 0 else 0
        
        return {
            'quality_score': quality_score,
            'total_datasets': len(self.quality_report),
            'passed_checks': passed_checks,
            'total_checks': total_checks,
            'detailed_report': self.quality_report
        }

def validate_all_data(extracted_data):
    """Función principal de validación"""
    validator = DataValidator()
    
    for name, data in extracted_data.items():
        if name == 'historical':
            # Validar DataFrame de datos históricos
            validator.validate_completeness(data, name)
            validator.validate_freshness(data, name)
            validator.validate_accuracy(data, name)
        else:
            # Validar datos de API (diccionarios)
            df_temp = pd.DataFrame([data]) if isinstance(data, dict) else pd.DataFrame(data)
            validator.validate_completeness(df_temp, name)
            validator.validate_freshness(data, name)
            validator.validate_accuracy(df_temp, name)
    
    return validator.generate_quality_summary()