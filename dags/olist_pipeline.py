"""Pipeline Medallion de Olist: Bronze -> Silver -> PII -> Gold -> catálogo DuckDB.

Esqueleto (Fase 3): las tareas todavía no procesan datos.
Cada una devuelve por XCom un diccionario chico (rutas y cantidades),
nunca las tablas: los datos viajan por disco.
"""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

DATA_DIR = "/opt/airflow/data"  # carpeta data/ del proyecto, montada en Docker

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="olist_pipeline",
    schedule="@monthly",
    start_date=pendulum.datetime(2016, 9, 1, tz="UTC"),
    end_date=pendulum.datetime(2018, 10, 31, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["olist", "tp"],
)
def olist_pipeline():

    @task
    def bronze_ingesta(**context):
        mes = context["data_interval_start"]
        print(f"[Bronze] Copiar CSV crudos para el mes {mes:%Y-%m}")
        return {"mes": f"{mes:%Y-%m}", "bronze_dir": f"{DATA_DIR}/bronze"}

    @task
    def bronze_validacion(bronze: dict):
        print(f"[Bronze] Validar tablas en {bronze['bronze_dir']}")
        # Fase 4: si falta una tabla o está vacía -> raise ValueError(...)
        return {"mes": bronze["mes"], "tablas_ok": 0}

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