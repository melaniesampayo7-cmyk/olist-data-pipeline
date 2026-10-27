"""Pipeline Medallion de Olist: Bronze -> Silver -> PII -> Gold -> catálogo DuckDB.

Esqueleto (Fase 3): las tareas todavía no procesan datos.
Cada una devuelve por XCom un diccionario chico (rutas y cantidades),
nunca las tablas: los datos viajan por disco.
"""

from datetime import timedelta
from pathlib import Path
import shutil

import pendulum
from airflow.sdk import dag, task, Variable 

import csv

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import hashlib

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
        
        # Mes de negocio: permite ejecutar períodos históricos
        configuracion = context["dag_run"].conf or {}

        mes_configurado = configuracion.get("business_month")

        if mes_configurado:
            mes = pendulum.from_format(
                mes_configurado,
                "YYYY-MM",
                tz="UTC"
            )
        else:
            mes = context["data_interval_start"]

        fecha_ingesta = pendulum.from_format(
            Variable.get("BRONZE_INGESTION_DATE"),
            "YYYY-MM-DD",
            tz="UTC"
        )

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
        print(f"[Silver] Procesando mes {validacion['mes']}")

        # Archivo de clientes de Bronze
        bronze_dir = Path(validacion["bronze_dir"])

        
        fecha_ingesta = pendulum.from_format(
            Variable.get("BRONZE_INGESTION_DATE"),
            "YYYY-MM-DD",
            tz="UTC"
        )

        archivo_clientes = (
            bronze_dir
            / "olist_customers_dataset"
            / f"ingested_year={fecha_ingesta.year}"
            / f"ingested_month={fecha_ingesta.month:02d}"
            / "olist_customers_dataset.csv"
        )

        if not archivo_clientes.is_file():
            raise FileNotFoundError(
                f"No se encontró el archivo: {archivo_clientes}"
            )


        

        # Leer CSV conservando el código postal como texto
        df = pd.read_csv(
            archivo_clientes,
            dtype={"customer_zip_code_prefix": "string"}
        )

        filas_originales = len(df)

        # Eliminar duplicados por identificador de cliente
        df = df.drop_duplicates(
            subset=["customer_id"],
            keep="first"
        ).copy()

        # Guardar Silver en formato Parquet
        silver_dir = (
            Path(DATA_DIR)
            / "silver"
            / "olist_customers_dataset"
        )

        silver_dir.mkdir(parents=True, exist_ok=True)

        destino = silver_dir / "customers.parquet"

        tabla = pa.Table.from_pandas(
            df,
            preserve_index=False
        )

        pq.write_table(tabla, destino)

        print(
            f"[Silver] Clientes: {filas_originales} "
            f"originales, {len(df)} finales"
        )


        # ======================================
        # SILVER - PEDIDOS
        # ======================================

        archivo_orders = (
            bronze_dir
            / "olist_orders_dataset"
            / f"ingested_year={fecha_ingesta.year}"
            / f"ingested_month={fecha_ingesta.month:02d}"
            / "olist_orders_dataset.csv"
        )

        if not archivo_orders.is_file():
            raise FileNotFoundError(
                f"No se encontró: {archivo_orders}"
            )

        df_orders = pd.read_csv(archivo_orders)
        filas_orders_originales = len(df_orders)

        # Eliminar registros sin identificadores
        df_orders = df_orders.dropna(
            subset=["order_id", "customer_id"]
        )

        # Convertir columnas temporales
        columnas_fecha = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ]

        for columna in columnas_fecha:
            df_orders[columna] = pd.to_datetime(
                df_orders[columna],
                errors="coerce"
            )

        # Deduplicación por pedido
        df_orders = df_orders.drop_duplicates(
            subset=["order_id"],
            keep="first"
        ).copy()

        # Guardar como Parquet
        silver_orders_dir = (
            Path(DATA_DIR)
            / "silver"
            / "olist_orders_dataset"
        )

        silver_orders_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        destino_orders = (
            silver_orders_dir / "orders.parquet"
        )

        tabla_orders = pa.Table.from_pandas(
            df_orders,
            preserve_index=False
        )

        pq.write_table(tabla_orders, destino_orders)

        print(
            f"[Silver] Pedidos: "
            f"{filas_orders_originales} originales, "
            f"{len(df_orders)} finales"
        )


        
        # ======================================
        # SILVER - ITEMS DE PEDIDOS
        # ======================================

        archivo_items = (
            bronze_dir
            / "olist_order_items_dataset"
            / f"ingested_year={fecha_ingesta.year}"
            / f"ingested_month={fecha_ingesta.month:02d}"
            / "olist_order_items_dataset.csv"
        )

        if not archivo_items.is_file():
            raise FileNotFoundError(
                f"No se encontró: {archivo_items}"
            )

        df_items = pd.read_csv(archivo_items)
        filas_items_originales = len(df_items)

        # Eliminar registros sin identificadores
        df_items = df_items.dropna(
            subset=[
                "order_id",
                "order_item_id",
                "product_id",
                "seller_id"
            ]
        )

        # Convertir fecha a datetime
        df_items["shipping_limit_date"] = pd.to_datetime(
            df_items["shipping_limit_date"],
            errors="coerce"
        )

        # Garantizar tipos numéricos
        for columna in ["price", "freight_value"]:
            df_items[columna] = pd.to_numeric(
                df_items[columna],
                errors="coerce"
            )

        # Deduplicar usando clave compuesta
        df_items = df_items.drop_duplicates(
            subset=["order_id", "order_item_id"],
            keep="first"
        ).copy()

        # Guardar Parquet
        silver_items_dir = (
            Path(DATA_DIR)
            / "silver"
            / "olist_order_items_dataset"
        )

        silver_items_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        destino_items = (
            silver_items_dir / "order_items.parquet"
        )

        tabla_items = pa.Table.from_pandas(
            df_items,
            preserve_index=False
        )

        pq.write_table(tabla_items, destino_items)

        print(
            f"[Silver] Items: "
            f"{filas_items_originales} originales, "
            f"{len(df_items)} finales"
        )


        
        # ======================================
        # SILVER - PRODUCTOS
        # ======================================

        archivo_productos = (
            bronze_dir
            / "olist_products_dataset"
            / f"ingested_year={fecha_ingesta.year}"
            / f"ingested_month={fecha_ingesta.month:02d}"
            / "olist_products_dataset.csv"
        )

        if not archivo_productos.is_file():
            raise FileNotFoundError(
                f"No se encontró: {archivo_productos}"
            )

        df_productos = pd.read_csv(archivo_productos)
        filas_productos_originales = len(df_productos)

        # Eliminar registros sin identificador
        df_productos = df_productos.dropna(
            subset=["product_id"]
        )

        # Deduplicar por producto
        df_productos = df_productos.drop_duplicates(
            subset=["product_id"],
            keep="first"
        ).copy()

        # Completar categorías desconocidas
        df_productos["product_category_name"] = (
            df_productos["product_category_name"]
            .fillna("desconocida")
        )

        # Corregir nombres de columnas
        df_productos = df_productos.rename(
            columns={
                "product_name_lenght": "product_name_length",
                "product_description_lenght": "product_description_length"
            }
        )

        # Conservar los demás nulos sin inventar valores

        # Guardar Parquet
        silver_productos_dir = (
            Path(DATA_DIR)
            / "silver"
            / "olist_products_dataset"
        )

        silver_productos_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        destino_productos = (
            silver_productos_dir / "products.parquet"
        )

        tabla_productos = pa.Table.from_pandas(
            df_productos,
            preserve_index=False
        )

        pq.write_table(tabla_productos, destino_productos)

        print(
            f"[Silver] Productos: "
            f"{filas_productos_originales} originales, "
            f"{len(df_productos)} finales"
        )


        
        # ======================================
        # SILVER - PAGOS
        # ======================================

        archivo_pagos = (
            bronze_dir
            / "olist_order_payments_dataset"
            / f"ingested_year={fecha_ingesta.year}"
            / f"ingested_month={fecha_ingesta.month:02d}"
            / "olist_order_payments_dataset.csv"
        )

        if not archivo_pagos.is_file():
            raise FileNotFoundError(
                f"No se encontró: {archivo_pagos}"
            )

        df_pagos = pd.read_csv(archivo_pagos)
        filas_pagos_originales = len(df_pagos)

        # Eliminar registros sin clave
        df_pagos = df_pagos.dropna(
            subset=["order_id", "payment_sequential"]
        )

        # Convertir tipos numéricos
        df_pagos["payment_sequential"] = pd.to_numeric(
            df_pagos["payment_sequential"],
            errors="raise"
        ).astype("int64")

        df_pagos["payment_installments"] = pd.to_numeric(
            df_pagos["payment_installments"],
            errors="raise"
        ).astype("int64")

        df_pagos["payment_value"] = pd.to_numeric(
            df_pagos["payment_value"],
            errors="raise"
        )

        # Deduplicación por clave compuesta
        df_pagos = df_pagos.drop_duplicates(
            subset=["order_id", "payment_sequential"],
            keep="first"
        ).copy()

        # Guardar Parquet
        silver_pagos_dir = (
            Path(DATA_DIR)
            / "silver"
            / "olist_order_payments_dataset"
        )

        silver_pagos_dir.mkdir(
            parents=True, exist_ok=True
        )

        destino_pagos = silver_pagos_dir / "payments.parquet"

        tabla_pagos = pa.Table.from_pandas(
            df_pagos,
            preserve_index=False
        )

        pq.write_table(tabla_pagos, destino_pagos)

        print(
            f"[Silver] Pagos: "
            f"{filas_pagos_originales} originales, "
            f"{len(df_pagos)} finales"
        )



        return {
            "silver_dir": str(Path(DATA_DIR) / "silver"),
            "mes": validacion["mes"],
            "filas_clientes": len(df),
            "filas_pedidos": len(df_orders),
            "filas_items": len(df_items),
            "filas_productos": len(df_productos),
            "filas_pagos": len(df_pagos)
        }


    
    @task
    def silver_enmascaramiento_pii(validacion: dict):

        print(f"[Silver PII] Procesando mes {validacion['mes']}")

        # Recuperar salt desde Airflow
        salt = Variable.get("PII_SALT")

        # Identificar la partición de Bronze
        fecha_ingesta = pendulum.from_format(
            Variable.get("BRONZE_INGESTION_DATE"),
            "YYYY-MM-DD",
            tz="UTC"
        )

        archivo_pii = (
            Path(validacion["bronze_dir"])
            / "pii_customers"
            / f"ingested_year={fecha_ingesta.year}"
            / f"ingested_month={fecha_ingesta.month:02d}"
            / "pii_customers.csv"
        )

        if not archivo_pii.is_file():
            raise FileNotFoundError(
                f"No se encontró: {archivo_pii}"
            )

        # Leer la tabla original
        df_pii = pd.read_csv(
            archivo_pii,
            dtype="string"
        )

        # Verificar las columnas personales
        columnas_pii = [
            "full_name",
            "email",
            "phone_number"
        ]

        requeridas = ["customer_id"] + columnas_pii

        faltantes = set(requeridas) - set(df_pii.columns)

        if faltantes:
            raise ValueError(
                f"Columnas PII faltantes: {sorted(faltantes)}"
            )

        filas_originales = len(df_pii)

        # Eliminar registros sin identificador
        df_pii = df_pii.dropna(
            subset=["customer_id"]
        )

        # Deduplicar por cliente
        df_pii = df_pii.drop_duplicates(
            subset=["customer_id"],
            keep="first"
        ).copy()

        # Hash SHA-256 con salt
        def generar_hash(valor):
            if pd.isna(valor):
                return None

            contenido = f"{salt}:{str(valor).strip().lower()}"

            return hashlib.sha256(
                contenido.encode("utf-8")
            ).hexdigest()

        # Reemplazar PII por hashes
        for columna in columnas_pii:
            df_pii[f"{columna}_hash"] = (
                df_pii[columna].apply(generar_hash)
            )

        # Eliminar las columnas originales
        df_pii = df_pii.drop(columns=columnas_pii)

        # Guardar en Parquet
        pii_dir = (
            Path(DATA_DIR)
            / "silver"
            / "pii_customers"
        )

        pii_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        destino = pii_dir / "pii_customers.parquet"

        tabla = pa.Table.from_pandas(
            df_pii,
            preserve_index=False
        )

        pq.write_table(tabla, destino)

        print(
            f"[Silver PII] {filas_originales} originales, "
            f"{len(df_pii)} finales"
        )

        return {
            "pii_dir": str(pii_dir),
            "filas": len(df_pii)
        }


    @task
    def gold_particiones(silver: dict, pii: dict, **context):
        
        # Usar el mismo mes procesado desde Bronze/Silver
        mes = pendulum.from_format(
            silver["mes"],
            "YYYY-MM",
            tz="UTC"
        )

        
        # ======================================
        # GOLD - LECTURA DE TABLAS SILVER
        # ======================================

        silver_dir = Path(silver["silver_dir"])

        df_orders = pd.read_parquet(
            silver_dir / "olist_orders_dataset" / "orders.parquet"
        )

        df_items = pd.read_parquet(
            silver_dir / "olist_order_items_dataset" / "order_items.parquet"
        )

        df_productos = pd.read_parquet(
            silver_dir / "olist_products_dataset" / "products.parquet"
        )

        df_clientes = pd.read_parquet(
            silver_dir / "olist_customers_dataset" / "customers.parquet"
        )

        df_pagos = pd.read_parquet(
            silver_dir / "olist_order_payments_dataset" / "payments.parquet"
        )

        print("[Gold] Tablas Silver cargadas correctamente")
        print(f"[Gold] Pedidos: {len(df_orders)}")
        print(f"[Gold] Items: {len(df_items)}")
        print(f"[Gold] Productos: {len(df_productos)}")
        print(f"[Gold] Clientes: {len(df_clientes)}")
        print(f"[Gold] Pagos: {len(df_pagos)}")


        
        # ======================================
        # GOLD - FILTRADO POR MES DE NEGOCIO
        # ======================================

        inicio_mes = mes.start_of("month")
        fin_mes = inicio_mes.add(months=1)

        # Seleccionar pedidos del período solicitado
        df_orders_mes = df_orders[
            (df_orders["order_purchase_timestamp"] >= inicio_mes.naive())
            & (df_orders["order_purchase_timestamp"] < fin_mes.naive())
        ].copy()

        print(
            f"[Gold] Período: {inicio_mes:%Y-%m-%d} "
            f"hasta {fin_mes:%Y-%m-%d} (exclusivo)"
        )

        print(
            f"[Gold] Pedidos del mes {mes:%Y-%m}: "
            f"{len(df_orders_mes)}"
        )

        
        # ======================================
        # GOLD - AGREGACION DE PAGOS POR PEDIDO
        # ======================================

        df_pagos_agrupados = (
            df_pagos
            .groupby("order_id", as_index=False)
            .agg(
                total_pagado=("payment_value", "sum"),
                cantidad_pagos=("payment_sequential", "count")
            )
        )

        # Verificar que quede una fila por pedido
        duplicados = df_pagos_agrupados.duplicated(
            subset=["order_id"]
        ).sum()

        if duplicados != 0:
            raise ValueError(
                "Existen pedidos duplicados en los pagos agrupados"
            )

        print(
            f"[Gold] Pagos agrupados: "
            f"{len(df_pagos_agrupados)} pedidos únicos"
        )

        
        # ======================================
        # GOLD - UNION DE TABLAS SILVER
        # ======================================

        # 1. Pedidos del mes + items
        df_gold = df_orders_mes.merge(
            df_items,
            on="order_id",
            how="inner",
            validate="one_to_many"
        )

        # 2. Agregar información de productos
        df_gold = df_gold.merge(
            df_productos[
                ["product_id", "product_category_name"]
            ],
            on="product_id",
            how="left",
            validate="many_to_one"
        )

        # 3. Agregar información de clientes
        df_gold = df_gold.merge(
            df_clientes[
                ["customer_id", "customer_city", "customer_state"]
            ],
            on="customer_id",
            how="left",
            validate="many_to_one"
        )

        # 4. Agregar pagos agrupados por pedido
        df_gold = df_gold.merge(
            df_pagos_agrupados,
            on="order_id",
            how="left",
            validate="many_to_one"
        )

        # Verificar la clave única de cada item
        duplicados = df_gold.duplicated(
            ["order_id", "order_item_id"]
        ).sum()

        if duplicados:
            raise ValueError(
                f"Gold tiene {duplicados} items duplicados"
            )

        print(f"[Gold] Filas después de uniones: {len(df_gold)}")
        print(f"[Gold] Pedidos únicos: {df_gold['order_id'].nunique()}")
        print(f"[Gold] Duplicados de items: {duplicados}")

        
        # ======================================
        # GOLD - SELECCION DE COLUMNAS
        # ======================================

        columnas_gold = [
            "order_id",
            "order_item_id",
            "order_purchase_timestamp",
            "order_status",
            "customer_id",
            "customer_city",
            "customer_state",
            "product_id",
            "product_category_name",
            "seller_id",
            "price",
            "freight_value",
            "total_pagado",
            "cantidad_pagos"
        ]

        df_gold = df_gold[columnas_gold].copy()

        print(
            f"[Gold] Tabla final: "
            f"{len(df_gold)} filas y {len(df_gold.columns)} columnas"
        )


        
        # ======================================
        # GOLD - ESCRITURA PARQUET PARTICIONADO
        # ======================================

        particion = (
            Path(DATA_DIR)
            / "gold"
            / "ventas"
            / f"year={mes:%Y}"
            / f"month={mes:%m}"
        )

        particion.mkdir(parents=True, exist_ok=True)

        archivo_gold = particion / "ventas.parquet"

        # Sobrescribir la partición del mes procesado
        pq.write_table(
            pa.Table.from_pandas(df_gold, preserve_index=False),
            archivo_gold
        )

        print(f"[Gold] Archivo generado: {archivo_gold}")
        print(f"[Gold] Filas guardadas: {len(df_gold)}")

        return {
            "particion": f"year={mes:%Y}/month={mes:%m}",
            "archivo": str(archivo_gold),
            "filas": len(df_gold)
        }


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