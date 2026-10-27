
import duckdb

# Ruta de los archivos Gold dentro de Docker
DATA_PATH = "/opt/airflow/data/gold/ventas/**/*.parquet"

# Conexión a DuckDB
conn = duckdb.connect()

# ======================================
# 1. FULL SCAN
# ======================================

print("\n===== FULL SCAN =====")

consulta_full = f"""
EXPLAIN ANALYZE
SELECT year, month, COUNT(*) AS registros
FROM read_parquet(
    '{DATA_PATH}',
    hive_partitioning = true
)
GROUP BY year, month
"""

resultado_full = conn.execute(consulta_full).fetchall()

print(resultado_full[0][1])


# ======================================
# 2. PRUNED SCAN
# ======================================

print("\n===== PRUNED SCAN: ENERO 2017 =====")

consulta_pruned = f"""
EXPLAIN ANALYZE
SELECT year, month, COUNT(*) AS registros
FROM read_parquet(
    '{DATA_PATH}',
    hive_partitioning = true
)
WHERE year = 2017 AND month = 1
GROUP BY year, month
"""

resultado_pruned = conn.execute(consulta_pruned).fetchall()

print(resultado_pruned[0][1])

conn.close()
