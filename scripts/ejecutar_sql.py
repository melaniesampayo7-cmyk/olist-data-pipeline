"""Ejecuta las consultas de sql/consultas_gold.sql con DuckDB y muestra los resultados.

Uso, desde la raíz del repositorio:
    python scripts/ejecutar_sql.py

Requiere tener la capa Gold generada en data/gold/ventas/.
"""
import os
from pathlib import Path

import duckdb

BASE = Path(__file__).resolve().parent.parent
ARCHIVO_SQL = BASE / "sql" / "consultas_gold.sql"

# Las rutas del .sql son relativas a la raíz del repositorio
os.chdir(BASE)

con = duckdb.connect()
texto = ARCHIVO_SQL.read_text(encoding="utf-8")

for bloque in texto.split(";"):
    # Ignorar comentarios y bloques vacíos
    lineas = [
        linea for linea in bloque.splitlines()
        if linea.strip() and not linea.strip().startswith("--")
    ]
    if not lineas:
        continue

    consulta = "\n".join(lineas)
    print("\n" + "=" * 70)
    print(lineas[0].strip())
    print("=" * 70)

    resultado = con.sql(consulta)
    if resultado is None:
        print("OK")
    elif consulta.upper().startswith("EXPLAIN"):
        print(resultado.fetchall()[0][1])
    else:
        print(resultado)

con.close()
