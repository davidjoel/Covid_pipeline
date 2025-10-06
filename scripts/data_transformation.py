import pandas as pd
import numpy as np
from datetime import datetime
import logging

# ✅ AGREGAR ESTO  
import sys
import os
sys.path.append('/opt/airflow/scripts')
sys.path.append('/opt/airflow/config')

logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self):
        self.taxonomy = self._load_taxonomy()
    
    def _load_taxonomy(self):
        """Carga taxonomía para clasificación de países"""
        return {
            'income_level': {
                'high_income': ['USA', 'GBR', 'DEU', 'FRA', 'CAN', 'AUS', 'JPN', 'KOR'],
                'upper_middle': ['CHN', 'RUS', 'BRA', 'MEX', 'TUR', 'ARG', 'THA'],
                'lower_middle': ['IND', 'IDN', 'EGY', 'PHL', 'VNM', 'NGA'],
                'low_income': ['ETH', 'BDI', 'MLI', 'NER', 'TCD']
            },
            'region': {
                'north_america': ['USA', 'CAN', 'MEX'],
                'europe': ['GBR', 'DEU', 'FRA', 'ITA', 'ESP'],
                'asia': ['CHN', 'IND', 'JPN', 'KOR', 'IDN'],
                'south_america': ['BRA', 'ARG', 'COL', 'PER'],
                'africa': ['NGA', 'EGY', 'ZAF', 'KEN'],
                'oceania': ['AUS', 'NZL']
            },
            'outbreak_severity': {
                'low': [0, 1000],
                'medium': [1001, 10000],
                'high': [10001, 100000],
                'critical': [100001, float('inf')]
            }
        }
    
    def classify_countries(self, df_countries):
        """Clasifica países por categorías"""
        df = df_countries.copy()
        
        # Clasificar por nivel de ingresos
        def get_income_level(country_code):
            for level, countries in self.taxonomy['income_level'].items():
                if country_code in countries:
                    return level
            return 'unknown'
        
        # Clasificar por región
        def get_region(country_code):
            for region, countries in self.taxonomy['region'].items():
                if country_code in countries:
                    return region
            return 'unknown'
        
        # Clasificar por severidad del brote
        def get_severity(cases):
            for severity, (min_val, max_val) in self.taxonomy['outbreak_severity'].items():
                if min_val <= cases <= max_val:
                    return severity
            return 'unknown'
        
        # Aplicar clasificaciones
        if 'country' in df.columns:
            df['income_level'] = df['country'].apply(get_income_level)
            df['region'] = df['country'].apply(get_region)
            df['outbreak_severity'] = df['cases'].apply(get_severity)
        
        return df
    
    def calculate_metrics(self, df):
        """Calcula métricas derivadas"""
        df_metrics = df.copy()
        
        # Calcular tasas por millón de habitantes
        if 'cases' in df.columns and 'population' in df.columns:
            df_metrics['cases_per_million'] = (df['cases'] / df['population']) * 1e6
            df_metrics['deaths_per_million'] = (df['deaths'] / df['population']) * 1e6
        
        # Calcular tasas de letalidad
        if 'deaths' in df.columns and 'cases' in df.columns:
            df_metrics['fatality_rate'] = (df['deaths'] / df['cases'] * 100).fillna(0)
        
        # Calcular porcentaje de población afectada
        if 'cases' in df.columns and 'population' in df.columns:
            df_metrics['infection_rate'] = (df['cases'] / df['population'] * 100).fillna(0)
        
        return df_metrics
    
    def create_aggregates(self, df_historical):
        """Crea agregados temporales"""
        df_agg = df_historical.copy()
        df_agg['date'] = pd.to_datetime(df_agg['date'])
        
        # Agregados semanales
        weekly_agg = df_agg.groupby(['location', pd.Grouper(key='date', freq='W')]).agg({
            'new_cases': 'sum',
            'new_deaths': 'sum',
            'total_cases': 'last',
            'total_deaths': 'last'
        }).reset_index()
        
        # Agregados mensuales
        monthly_agg = df_agg.groupby(['location', pd.Grouper(key='date', freq='M')]).agg({
            'new_cases': 'sum',
            'new_deaths': 'sum',
            'total_cases': 'last',
            'total_deaths': 'last'
        }).reset_index()
        
        return {
            'daily': df_agg,
            'weekly': weekly_agg,
            'monthly': monthly_agg
        }
    
    def transform_all_data(self, extracted_data):
        """Transforma todos los datos"""
        transformed_data = {}
        
        # Transformar datos actuales por países
        if 'countries_current' in extracted_data:
            df_countries = pd.DataFrame(extracted_data['countries_current'])
            df_countries_classified = self.classify_countries(df_countries)
            df_countries_metrics = self.calculate_metrics(df_countries_classified)
            transformed_data['countries_processed'] = df_countries_metrics
        
        # Transformar datos históricos
        if 'historical' in extracted_data:
            aggregates = self.create_aggregates(extracted_data['historical'])
            transformed_data['historical_processed'] = aggregates
        
        # Transformar datos globales
        if 'global_current' in extracted_data:
            global_data = extracted_data['global_current']
            transformed_data['global_processed'] = global_data
        
        logger.info("✅ Transformación de datos completada")
        return transformed_data