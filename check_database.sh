#!/bin/bash
echo "=== COVID DATABASE REPORT ==="
echo ""

DB_PATH="/opt/airflow/data/covid_dashboard.db"

if [ ! -f "$DB_PATH" ]; then
    echo "❌ Base de datos no encontrada: $DB_PATH"
    exit 1
fi

echo "📊 TABLAS EN LA BASE DE DATOS:"
sqlite3 "$DB_PATH" ".tables"

echo ""
echo "📈 DATOS GLOBALES ACTUALES:"
sqlite3 "$DB_PATH" "SELECT * FROM current_global_stats;"

echo ""
echo "🌍 DATOS POR PAÍSES (primeros 5):"
sqlite3 "$DB_PATH" "SELECT country, cases, deaths, recovered FROM current_countries_stats LIMIT 5;"

echo ""
echo "📅 DATOS HISTÓRICOS (conteo):"
sqlite3 "$DB_PATH" "SELECT COUNT(*) as total_records FROM historical_covid_data;"

echo ""
echo "✅ REPORTE COMPLETADO"
