"""Pipeline Medallion de Olist: Bronze -> Silver -> PII -> Gold -> catálogo DuckDB.

Esqueleto (Fase 3): las tareas todavía no procesan datos.
Cada una devuelve por XCom un diccionario chico (rutas y cantidades),
nunca las tablas: los datos viajan por disco.
"""

from datetime import timedelta
from pathlib import Path
import shutil

import pendulum
from airflow.sdk import dag, task

import csv

DATA_DIR = "/opt/airflow/data"  # carpeta data/ del proyecto, montada en Docker

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="olist_pipeline",
    schedule="@monthly",
    start_date=pendulum.datetime(2016, 9, 1, tz="UTC"),
    
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["olist", "tp"],
)
def olist_pipeline():

    
    @task
    def bronze_ingesta(**context):
        mes = context["data_interval_start"]
        # Fecha de ingesta del lote original de Olist.
        # Se mantiene fija para garantizar reprocesamientos reproducibles.
        fecha_ingesta = pendulum.datetime(2026, 10, 27, tz="UTC")

        raw_dir = Path("/opt/airflow/raw")
        bronze_dir = Path(DATA_DIR) / "bronze"

        archivos = sorted(raw_dir.glob("*.csv"))

        if not archivos:
            raise ValueError("No se encontraron archivos CSV en raw/")

        archivos_copiados = []

        for archivo in archivos:
            nombre_tabla = archivo.stem

            destino = (
                bronze_dir
                / nombre_tabla
                / f"ingested_year={fecha_ingesta.year}"
                / f"ingested_month={fecha_ingesta.month:02d}"
            )

            destino.mkdir(parents=True, exist_ok=True)

            archivo_destino = destino / archivo.name

            # Sobrescribir evita acumular copias al reejecutar.
            shutil.copyfile(archivo, archivo_destino)

            archivos_copiados.append(str(archivo_destino))

            print(f"[Bronze] Copiado: {archivo.name}")

        print(f"[Bronze] Total de archivos: {len(archivos_copiados)}")

        return {
            "mes": f"{mes:%Y-%m}",
            "bronze_dir": str(bronze_dir),
            "archivos": archivos_copiados,
            "tablas_ok": len(archivos_copiados),
        }


    
    @task
    def bronze_validacion(bronze: dict):

        tablas_esperadas = {
            "olist_customers_dataset.csv",
            "olist_geolocation_dataset.csv",
            "olist_orders_dataset.csv",
            "olist_order_items_dataset.csv",
            "olist_order_payments_dataset.csv",
            "olist_order_reviews_dataset.csv",
            "olist_products_dataset.csv",
            "olist_sellers_dataset.csv",
            "pii_customers.csv",
        }
        
        columnas_obligatorias = {
            "olist_customers_dataset.csv": {
                "customer_id",
                "customer_unique_id",
                "customer_zip_code_prefix",
            },
            "olist_geolocation_dataset.csv": {
                "geolocation_zip_code_prefix",
                "geolocation_lat",
                "geolocation_lng",
            },
            "olist_orders_dataset.csv": {
                "order_id",
                "customer_id",
                "order_status",
                "order_purchase_timestamp",
            },
            "olist_order_items_dataset.csv": {
                "order_id",
                "order_item_id",
                "product_id",
                "seller_id",
                "price",
                "freight_value",
            },
            "olist_order_payments_dataset.csv": {
                "order_id",
                "payment_sequential",
                "payment_type",
                "payment_value",
            },
            "olist_order_reviews_dataset.csv": {
                "review_id",
                "order_id",
                "review_creation_date",
            },
            "olist_products_dataset.csv": {
                "product_id",
                "product_category_name",
            },
            "olist_sellers_dataset.csv": {
                "seller_id",
                "seller_zip_code_prefix",
            },
            "pii_customers.csv": {
                "customer_id",
                "customer_unique_id",
                "full_name",
                "email",
                "phone_number",
            },
        }

        archivos = [Path(ruta) for ruta in bronze["archivos"]]

        nombres = [archivo.name for archivo in archivos]

        faltantes = tablas_esperadas - set(nombres)
        inesperados = set(nombres) - tablas_esperadas

        if faltantes:
            raise ValueError(
                f"Faltan tablas en Bronze: {sorted(faltantes)}"
            )

        if inesperados:
            raise ValueError(
                f"Archivos inesperados en Bronze: {sorted(inesperados)}"
            )

        if len(archivos) != len(tablas_esperadas):
            raise ValueError("Cantidad de archivos incorrecta")

        for archivo in archivos:

            if not archivo.is_file():
                raise FileNotFoundError(
                    f"No existe el archivo: {archivo}"
                )

            if archivo.stat().st_size == 0:
                raise ValueError(
                    f"Archivo vacio: {archivo.name}"
                )

            with archivo.open(
                "r", encoding="utf-8-sig", newline=""
            ) as f:
                lector = csv.reader(f)
                encabezado = next(lector, None)
                primera_fila = next(lector, None)

                if not encabezado or primera_fila is None:
                    raise ValueError(
                        f"CSV sin datos: {archivo.name}"
                    )

                if len(encabezado) != len(primera_fila):
                    raise ValueError(
                        f"Columnas inconsistentes: {archivo.name}"
                    )
                
                columnas_presentes = set(encabezado)
                requeridas = columnas_obligatorias[archivo.name]
                faltantes_columnas = requeridas - columnas_presentes

                if faltantes_columnas:
                    raise ValueError(
                        f"Faltan columnas obligatorias en "
                        f"{archivo.name}: {sorted(faltantes_columnas)}"
                    )

            print(f"[Bronze] OK: {archivo.name}")

        print("[Bronze] Validacion completa: 9 archivos OK")

        return {
            "mes": bronze["mes"],
            "bronze_dir": bronze["bronze_dir"],
            "tablas_ok": len(archivos),
        }


    @task
    def silver_limpieza(validacion: dict):
        print(f"[Silver] Tipar, deduplicar y resolver nulos ({validacion['mes']})")
        return {"silver_dir": f"{DATA_DIR}/silver", "filas": 0}

    @task
    def silver_enmascaramiento_pii(validacion: dict):
        print(f"[Silver] Hashear PII con salt ({validacion['mes']})")
        return {"pii_dir": f"{DATA_DIR}/silver/pii_customers", "filas": 0}

    @task
    def gold_particiones(silver: dict, pii: dict, **context):
        mes = context["data_interval_start"]
        print(f"[Gold] Unir {silver['silver_dir']} y {pii['pii_dir']}")
        print(f"[Gold] Escribir partición year={mes:%Y}/month={mes:%m}")
        return {"particion": f"year={mes:%Y}/month={mes:%m}", "filas": 0}

    @task
    def gold_catalogo(gold: dict):
        print(f"[Gold] Exponer en DuckDB la partición {gold['particion']}")

    # Las tareas se pasan metadatos por XCom
    bronze = bronze_ingesta()
    validacion = bronze_validacion(bronze)
    silver = silver_limpieza(validacion)
    pii = silver_enmascaramiento_pii(validacion)
    gold = gold_particiones(silver, pii)
    catalogo = gold_catalogo(gold)

    # Dependencias explícitas (lo pide la consigna)
    bronze >> validacion >> [silver, pii] >> gold >> catalogo


olist_pipeline()