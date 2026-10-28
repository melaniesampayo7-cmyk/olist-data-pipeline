# Olist Data Pipeline – TP Ingeniería de Datos 2026

Pipeline de datos end-to-end con **arquitectura Medallion** (Bronze → Silver → Gold), orquestado con **Apache Airflow 3.0.4** en Docker y consultado con **DuckDB**, sobre el dataset público [Brazilian E-Commerce (Olist)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

El pipeline es parametrizado, determinístico e idempotente: cada ejecución procesa un mes de negocio y puede re-ejecutarse sin duplicar datos.

**Integrantes:** Baéz, Echeverría, Nogueira, Sampayo y Uesato

---

## Índice

1. [Arquitectura](#arquitectura)
2. [Estructura del repositorio](#estructura-del-repositorio)
3. [Cómo ejecutarlo](#cómo-ejecutarlo)
4. [El DAG](#el-dag)
5. [Capa Bronze](#capa-bronze)
6. [Capa Silver: decisiones de calidad de datos](#capa-silver-decisiones-de-calidad-de-datos)
7. [Gobernanza de PII](#gobernanza-de-pii)
8. [Capa Gold: modelo y particionamiento](#capa-gold-modelo-y-particionamiento)
9. [Catálogo DuckDB y consultas SQL](#catálogo-duckdb-y-consultas-sql)
10. [Idempotencia](#idempotencia)
11. [Validación de partition pruning (FinOps)](#validación-de-partition-pruning-finops)
12. [Flujo de trabajo con Git](#flujo-de-trabajo-con-git)
13. [Migración a AWS](#migración-a-aws)
14. [Mejoras futuras](#mejoras-futuras)

---

## Arquitectura

```
raw/ (CSV de Kaggle + PII sintética)
   │
   ▼
BRONZE   data/bronze/{tabla}/ingested_year=YYYY/ingested_month=MM/   CSV sin modificar
   │
   ▼
SILVER   data/silver/{tabla}/*.parquet                               limpio, tipado, deduplicado
         data/silver/pii_customers/pii_customers.parquet              PII hasheada (SHA-256 + salt)
   │
   ▼
GOLD     data/gold/ventas/year=YYYY/month=MM/ventas.parquet          tabla analítica particionada
   │
   ▼
DuckDB   data/gold/catalogo.duckdb  →  vista `ventas`
```

| Componente | Herramienta |
|---|---|
| Orquestación | Apache Airflow 3.0.4 (Docker Compose, imagen propia) |
| Transformación | Python con pandas y pyarrow, dentro de tareas `@task` |
| Almacenamiento | Sistema de archivos local; CSV en Bronze, Apache Parquet en Silver y Gold |
| Consulta | DuckDB con `read_parquet(..., hive_partitioning=true)` |

> **Nota sobre el entorno:** la consigna propone instalar Airflow con WSL y `airflow standalone`. Este proyecto usa en cambio el `docker-compose.yaml` oficial de Airflow 3.0.4. Por eso, la carpeta de datos dentro del contenedor es `/opt/airflow/data` (equivalente a `~/airflow/data`) y los comandos de Airflow se ejecutan con `docker compose exec`.

---

## Estructura del repositorio

```
olist-data-pipeline/
├── dags/
│   └── olist_pipeline.py            # DAG con las 6 tareas del pipeline
├── scripts/
│   ├── generador_pii_sintetica.py   # genera raw/pii_customers.csv
│   ├── backfill_historico.ps1       # dispara el DAG para cada mes del histórico
│   ├── finops_validation.py         # EXPLAIN ANALYZE: escaneo completo vs. con pruning
│   ├── finops_resultados.txt        # salida de la validación
│   └── ejecutar_sql.py              # ejecuta sql/consultas_gold.sql con DuckDB
├── sql/
│   └── consultas_gold.sql           # vista, validación FinOps y consultas de análisis
├── docs/
│   ├── 01_eda_hallazgos.md          # hallazgos del análisis exploratorio
│   └── resultados_consultas_sql.txt # salida de las consultas SQL
├── notebooks/
│   └── 01_eda_olist.ipynb           # EDA
├── Dockerfile                       # imagen de Airflow con las librerías del proyecto
├── docker-compose.yaml
├── requirements.txt                 # pandas, pyarrow, duckdb, faker
└── .gitignore
```

No se versionan (están en `.gitignore`): `raw/` (CSV fuente y PII sintética), `data/` (Bronze, Silver, Gold y catálogo), `logs/`, `config/` (contiene el `airflow.cfg` con claves generadas) y `.env`.

---

## Cómo ejecutarlo

### 1. Requisitos

Git, Docker Desktop (con al menos 4 GB de memoria disponibles), Python 3.12 en la máquina local (para generar la PII sintética, correr el notebook y ejecutar las consultas SQL sin Airflow) y una cuenta de Kaggle.

### 2. Clonar el repositorio

```bash
git clone https://github.com/melaniesampayo7-cmyk/olist-data-pipeline.git
cd olist-data-pipeline
git checkout develop
```

### 3. Preparar los datos fuente

1. Descargar el dataset de Kaggle y copiar los CSV **directamente** en `raw/` (sin subcarpetas). El pipeline ingiere estas 8 tablas:
   `olist_customers_dataset.csv`, `olist_geolocation_dataset.csv`, `olist_orders_dataset.csv`, `olist_order_items_dataset.csv`, `olist_order_payments_dataset.csv`, `olist_order_reviews_dataset.csv`, `olist_products_dataset.csv` y `olist_sellers_dataset.csv`.
2. **`product_category_name_translation.csv` no debe quedar en `raw/`**, porque la validación de Bronze espera exactamente 9 archivos (ver [Mejoras futuras](#mejoras-futuras)). Se puede mover a una subcarpeta, por ejemplo `raw/no_ingerir/`.
3. Generar la tabla sintética de PII:
   ```bash
   pip install pandas faker
   python scripts/generador_pii_sintetica.py
   ```
   Debe imprimir `OK: 99441 filas, 96096 personas`.

### 4. Configurar y levantar Airflow

Crear un archivo `.env` en la raíz con:

```
AIRFLOW_UID=50000
```

Construir la imagen (instala las librerías de `requirements.txt` una sola vez) y levantar los servicios:

```bash
docker compose build
docker compose up airflow-init   # solo la primera vez
docker compose up -d
```

La interfaz queda en http://localhost:8080 (usuario y contraseña: `airflow`).

| Acción | Comando |
|---|---|
| Apagar | `docker compose down` |
| Volver a prender | `docker compose up -d` |
| Ver estado de los servicios | `docker compose ps` |

> No usar `docker compose down -v`: borra la base de datos de Airflow, incluidas las Variables y el historial.

### 5. Crear las Variables de Airflow

El DAG necesita dos Variables, que **no se guardan en el repositorio**. Se crean desde **Admin → Variables** o por CLI:

| Variable | Formato | Uso |
|---|---|---|
| `PII_SALT` | Texto secreto, igual para todo el equipo | Salt del hashing SHA-256 de la PII |
| `BRONZE_INGESTION_DATE` | `YYYY-MM-DD` | Fecha de ingesta que define la ruta de Bronze |

```bash
docker compose exec airflow-scheduler airflow variables set PII_SALT "<valor-secreto>"
docker compose exec airflow-scheduler airflow variables set BRONZE_INGESTION_DATE "2026-10-27"
```

### 6. Ejecutar el pipeline

**Un mes puntual:** botón **Trigger** → **Single Run**, y en **Advanced Options** la configuración:

```json
{"business_month": "2017-01"}
```

**Todo el histórico:** `scripts/backfill_historico.ps1` dispara una ejecución del DAG por cada mes con pedidos (25 meses, de septiembre de 2016 a octubre de 2018), pasando `business_month` en la configuración. Es equivalente a lanzar cada mes desde **Trigger**, pero automatizado. El script solo envía las solicitudes: Airflow las ejecuta de a una (`max_active_runs=1`).

```powershell
.\scripts\backfill_historico.ps1
```

### 7. Consultar Gold con SQL (sin Airflow)

Con la capa Gold generada en `data/gold/ventas/`, las consultas de `sql/consultas_gold.sql` se pueden ejecutar localmente con DuckDB, sin Docker:

```bash
pip install duckdb
python scripts/ejecutar_sql.py
```

---

## El DAG

```
bronze_ingesta >> bronze_validacion >> [silver_limpieza, silver_enmascaramiento_pii] >> gold_particiones >> gold_catalogo
```

| Tarea | Responsabilidad |
|---|---|
| `bronze_ingesta` | Copia los CSV de `raw/` a Bronze sin modificarlos |
| `bronze_validacion` | Verifica que estén los 9 archivos, que no estén vacíos y que tengan las columnas obligatorias; si algo falla, detiene el pipeline |
| `silver_limpieza` | Tipado, deduplicación y tratamiento de nulos de 5 tablas; escribe Parquet |
| `silver_enmascaramiento_pii` | Hashea nombre, email y teléfono con SHA-256 y salt |
| `gold_particiones` | Une las tablas Silver, filtra el mes de negocio y escribe la partición `year=/month=` |
| `gold_catalogo` | Actualiza la vista `ventas` de DuckDB sobre todas las particiones |

**Configuración:** `schedule="@monthly"`, `start_date` septiembre de 2016, `catchup=False` (los meses históricos se procesan con ejecuciones manuales, no automáticamente al activar el DAG), `max_active_runs=1` (dos ejecuciones nunca escriben las mismas carpetas a la vez) y 2 reintentos por tarea con 1 minuto de espera.

**Decisiones de diseño:**

- **Una tarea por responsabilidad.** Si algo falla, el grafo muestra exactamente dónde, y Airflow reintenta solo esa tarea sin rehacer las anteriores.
- **Rama en paralelo.** La limpieza y el enmascaramiento de PII no dependen entre sí, así que corren a la vez. Tener el enmascaramiento como tarea propia hace visible y auditable la gobernanza de PII, y es la única tarea que lee el salt.
- **XCom solo para metadatos.** Las tareas se pasan rutas, meses y cantidades de filas, nunca tablas: los datos viajan por disco, porque XCom está pensado para valores chicos.
- **Nombres `capa_acción`.** El recorrido Bronze → Silver → Gold se lee con solo mirar los nombres de las tareas.

**Mes de negocio.** Cada ejecución procesa un único mes, que se determina así:

1. Si la ejecución recibe `business_month` (formato `YYYY-MM`) en su configuración, se usa ese mes.
2. Si no, se usa la fecha lógica de Airflow, `data_interval_start`, por lo que el DAG también funciona con el schedule mensual o con el backfill nativo de Airflow.

Como Olist es un dataset histórico, el histórico completo se procesó con la primera opción, eligiendo explícitamente cada mes de negocio. En Airflow 3, los schedules cron usan por defecto un *timetable* donde `data_interval_start` y `data_interval_end` coinciden; por eso el pipeline toma el inicio del mes y calcula el fin en el código, en lugar de filtrar entre ambas fechas.

---

## Capa Bronze

`bronze_ingesta` copia cada CSV de `raw/` con `shutil.copyfile`, sin alterar su contenido, a:

```
data/bronze/{tabla}/ingested_year=YYYY/ingested_month=MM/{archivo}.csv
```

**Fecha de ingesta vs. fecha de negocio.** Son dos fechas distintas y el pipeline las separa a propósito:

- La **fecha de ingesta** indica *cuándo se cargaron* los datos. Define la ruta de Bronze y se toma de la Variable `BRONZE_INGESTION_DATE`, para no dejarla fija en el código y para que las re-ejecuciones escriban siempre en la misma carpeta.
- La **fecha de negocio** indica *cuándo ocurrió* el evento: la fecha de compra (`order_purchase_timestamp`). Define la partición de Gold.

Bronze conserva el dataset histórico completo; el filtrado por mes de negocio ocurre en Gold.

`bronze_validacion` controla que estén exactamente los 9 archivos esperados (8 tablas de Olist más `pii_customers.csv`), que ninguno esté vacío, que el encabezado sea consistente con la primera fila y que cada tabla tenga sus columnas obligatorias.

---

## Capa Silver: decisiones de calidad de datos

Las decisiones surgen del análisis exploratorio (`notebooks/01_eda_olist.ipynb`, resumido en `docs/01_eda_hallazgos.md`). `silver_limpieza` procesa 5 tablas y escribe un Parquet por tabla en `data/silver/{tabla}/`.

| Tabla | Registros en Silver |
|---|---|
| Clientes | 99.441 |
| Pedidos | 99.441 |
| Ítems de pedidos | 112.650 |
| Productos | 32.951 |
| Pagos | 103.886 |

### Tipado

- **Fechas.** Las 5 columnas de fecha de `orders` (`order_purchase_timestamp`, `order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date`, `order_estimated_delivery_date`) y `shipping_limit_date` de `order_items` llegan como texto y se convierten a `datetime`. Es necesario para filtrar por mes y particionar Gold por fecha de compra.
- **Montos.** `price` y `freight_value` (ítems) y `payment_value` (pagos) se convierten a numérico; `payment_sequential` y `payment_installments`, a entero. En pagos se usa `errors="raise"`, para que un valor no numérico detenga el pipeline en lugar de convertirse en nulo silenciosamente.
- **Código postal.** `customer_zip_code_prefix` se lee como texto. Los códigos postales brasileños pueden empezar con 0 (por ejemplo, `01310`) y al leerlos como número se pierde ese cero.
- **Nombres de columnas.** En `products`, `product_name_lenght` y `product_description_lenght` se renombran a `length`.

### Deduplicación

Cada tabla se deduplica según su propia clave natural, conservando la primera aparición:

| Tabla | Clave |
|---|---|
| customers | `customer_id` |
| orders | `order_id` |
| order_items | `order_id` + `order_item_id` |
| products | `product_id` |
| order_payments | `order_id` + `payment_sequential` |
| pii_customers | `customer_id` |

En estas tablas no se encontraron duplicados por clave: la deduplicación funciona como control, para que una re-carga del origen no duplique entidades. El EDA encontró duplicados reales en `geolocation` (261.831 filas duplicadas completas, 26 %), una tabla que hoy no se procesa en Silver (ver [Mejoras futuras](#mejoras-futuras)).

En `order_reviews`, el EDA mostró que un `review_id` repetido **no** es un duplicado: es una misma encuesta asociada a pedidos distintos. La clave correcta es (`review_id`, `order_id`), sin duplicados reales.

### Tratamiento de nulos

| Tabla y columna | Decisión | Justificación de negocio |
|---|---|---|
| orders: `order_id`, `customer_id` | Se eliminan las filas con nulo | Sin identificador, el pedido no puede unirse con ninguna otra tabla |
| orders: `order_approved_at` (160), `order_delivered_carrier_date` (1.783), `order_delivered_customer_date` (2.965) | Se conservan como nulos | Se verificó contra `order_status`: corresponden a pedidos que no llegaron a esa etapa (cancelados, sin stock, en tránsito). Un pedido cancelado sigue siendo un pedido; el nulo significa "todavía no ocurrió" |
| order_items: `order_id`, `order_item_id`, `product_id`, `seller_id` | Se eliminan las filas con nulo | Son la clave del ítem y sus vínculos; sin ellos el registro no se puede integrar |
| order_items: `price`, `freight_value` | Valores no numéricos se convierten a nulo | Evita que un dato mal formado frene todo el pipeline; no se detectaron casos |
| products: `product_id` | Se eliminan las filas con nulo | No pueden unirse con los ítems |
| products: `product_category_name` (610, 1,85 %) | Se imputa `desconocida` | Conserva esos productos en los análisis por categoría en lugar de perderlos al agrupar |
| products: largo de nombre y descripción, cantidad de fotos (610) | Se conservan como nulos | Imputar 0 sería falso: el valor no fue informado, no es cero |
| products: peso y dimensiones (2) | Se conservan como nulos | Cualquier medida imputada sería inventada |
| order_payments: `order_id`, `payment_sequential` | Se eliminan las filas con nulo | Son la clave del pago |
| pii_customers: `customer_id` | Se eliminan las filas con nulo | No pueden vincularse con ningún pedido |

En `order_reviews`, el título (88 % nulo) y el mensaje (59 % nulo) del comentario son opcionales para el cliente: el nulo significa "no dejó comentario". La tabla no se procesa hoy en Silver.

---

## Gobernanza de PII

**Tabla sintética.** Olist no incluye datos personales directos, así que `scripts/generador_pii_sintetica.py` genera una tabla con Faker (`pt_BR`):

- Una identidad (nombre, email, teléfono) **por persona** (`customer_unique_id`, 96.096 identidades), que cada `customer_id` hereda (99.441 filas). Así, una misma persona tiene el mismo email y, por lo tanto, el mismo hash en todos sus pedidos, lo que permite unir por el hash.
- **Determinística:** usa semilla fija y orden fijo. Se verificó con SHA-256 que dos ejecuciones generan archivos idénticos. Por eso se versiona el script y no el CSV.
- Los emails usan el dominio reservado `example.com`, que no coincide con ninguna casilla real.
- El CSV queda en `raw/`, excluido del repositorio.

**Enmascaramiento.** `silver_enmascaramiento_pii` reemplaza `full_name`, `email` y `phone_number` por `full_name_hash`, `email_hash` y `phone_number_hash`, calculados como:

```
SHA-256( salt + ":" + valor normalizado )
```

donde el valor se normaliza sin espacios en los extremos y en minúsculas, para que un mismo dato escrito distinto produzca el mismo hash. Se conservan `customer_id` y `customer_unique_id` para vincular con las demás tablas, y las columnas originales se eliminan del Parquet. Resultado: 99.441 registros, 0 duplicados, 0 columnas de PII en claro y todos los hashes de 64 caracteres.

**Por qué el salt.** Sin salt, alguien con acceso a los Parquet podría hashear una lista de emails conocidos y comparar resultados hasta encontrar coincidencias. Con el salt, eso no es posible sin conocerlo, y SHA-256 no es reversible. Al mismo tiempo, como el mismo email con el mismo salt siempre da el mismo hash, los analistas pueden unir tablas por el hash sin ver el dato original.

**Dónde vive el salt.** Se guarda como Variable de Airflow (`PII_SALT`), nunca en el código ni en el repositorio: si estuviera como constante de Python, quedaría expuesto en GitHub y el hash dejaría de proteger. Se lee con `Variable.get("PII_SALT")` **dentro** de la tarea, y no a nivel de módulo, porque Airflow vuelve a leer el archivo del DAG periódicamente y una lectura a nivel de módulo consultaría la base de metadatos en cada parseo.

---

## Capa Gold: modelo y particionamiento

**Granularidad:** una fila por ítem de pedido (clave `order_id` + `order_item_id`).

**Lógica de unión** (cinco entidades Silver):

1. Pedidos del mes **inner join** ítems (`one_to_many`): solo interesan pedidos con productos.
2. **left join** productos (`many_to_one`): agrega `product_category_name`.
3. **left join** clientes (`many_to_one`): agrega `customer_city` y `customer_state`.
4. **left join** pagos **agregados por pedido** (`many_to_one`): `total_pagado` y `cantidad_pagos`.

Los pagos se agregan **antes** del join porque un pedido puede tener varios pagos y varios ítems: unirlos directamente multiplicaría las filas. Cada join declara su cardinalidad esperada con `validate=`, y al final se verifica que no haya ítems duplicados; si los hay, la tarea falla.

**Columnas (14):** `order_id`, `order_item_id`, `order_purchase_timestamp`, `order_status`, `customer_id`, `customer_city`, `customer_state`, `product_id`, `product_category_name`, `seller_id`, `price`, `freight_value`, `total_pagado`, `cantidad_pagos`.

**Por qué desnormalizar.** Gold está pensada para el análisis: las consultas típicas (ventas por mes, por categoría o por estado) se resuelven sobre una sola tabla, sin que cada analista tenga que repetir los joins ni conocer el modelo relacional. Los joins se calculan una vez, en el pipeline, y se validan ahí. La tercera forma normal sigue disponible en Silver.

**Partición:** `data/gold/ventas/year=YYYY/month=MM/ventas.parquet`, según la fecha de compra (`order_purchase_timestamp`). Se eligió esa clave porque es la fecha de negocio central, existe en todos los pedidos (no tiene nulos, a diferencia de las fechas de entrega) y responde al patrón de consulta más frecuente: analizar períodos. Particionar por estado o categoría generaría muchas carpetas desparejas y no ayudaría a las consultas temporales.

**Resultado del histórico:** 25 particiones mensuales, de septiembre de 2016 a octubre de 2018, con **112.650 filas** en total, exactamente la cantidad de ítems de Olist. Dos casos particulares:

- **Noviembre de 2016** no tiene pedidos, así que no se procesó y no tiene partición.
- **Octubre de 2018** tiene partición, pero vacía: sus 4 pedidos están cancelados y no tienen ítems, por lo que no generan filas al unir pedidos con ítems.

---

## Catálogo DuckDB y consultas SQL

`gold_catalogo` crea o actualiza el archivo `data/gold/catalogo.duckdb` con una vista:

```sql
CREATE OR REPLACE VIEW ventas AS
SELECT * FROM read_parquet('/opt/airflow/data/gold/ventas/**/*.parquet', hive_partitioning = true);
```

La vista no copia datos: guarda cómo leer los Parquet. Con `hive_partitioning=true`, DuckDB reconoce las carpetas `year=` y `month=` como columnas, y nuevas particiones quedan disponibles sin cambiar la vista.

**Consultas SQL.** `sql/consultas_gold.sql` reúne las consultas del proyecto sobre Gold: la vista, el control de filas por partición, las dos consultas de la validación FinOps y tres consultas de análisis. Usa rutas relativas a la raíz del repositorio, para poder ejecutarlas fuera del contenedor con `python scripts/ejecutar_sql.py`. La salida completa está en `docs/resultados_consultas_sql.txt`.

Algunos resultados de las consultas de análisis:

- **Crecimiento:** las ventas pasan de unos pocos cientos de pedidos por mes en 2016 a más de 6.000 por mes en 2018, con una facturación mensual superior a 1 millón de reales.
- **Volumen no es lo mismo que facturación:** `cama_mesa_banho` es la categoría con más ítems vendidos, pero queda tercera en facturación, por debajo de `beleza_saude` y `relogios_presentes`, que venden productos más caros.
- **Concentración geográfica:** San Pablo concentra más del 40 % de los pedidos, pero tiene el ticket promedio más bajo; los estados más alejados, como Acre o Alagoas, tienen tickets más altos, probablemente por el costo del flete.

---

## Idempotencia

Ejecutar dos veces el mismo mes produce exactamente la misma cantidad de registros en Silver y Gold, porque cada capa **sobrescribe** en lugar de agregar:

| Capa | Estrategia de escritura |
|---|---|
| Bronze | La ruta depende de `BRONZE_INGESTION_DATE` (fija); `copyfile` reemplaza el archivo |
| Silver | Un Parquet por tabla con nombre fijo, reescrito en cada ejecución |
| Gold | Cada ejecución reescribe solo `ventas.parquet` de su partición, sin tocar los demás meses |
| Catálogo | `CREATE OR REPLACE VIEW` |

**Evidencia:**

| Prueba | Resultado |
|---|---|
| Re-ejecución de Bronze | 9 archivos con hash SHA-256 idéntico antes y después |
| Re-ejecución de enero 2017 | Enero se mantuvo en 955 filas y febrero en 1.951 |
| Ítems duplicados en Gold (enero 2017) | 0 |

---

## Validación de partition pruning (FinOps)

En la nube, servicios como Amazon Athena cobran por datos escaneados. `scripts/finops_validation.py` verifica con `EXPLAIN ANALYZE` que el particionamiento hace que DuckDB descarte los archivos de los meses que no cumplen el filtro:

- **Escaneo completo:** `SELECT year, month, COUNT(*) ... GROUP BY year, month` sobre todo Gold.
- **Escaneo con pruning:** la misma consulta con `WHERE year = 2017 AND month = 1`.

Como el script lee la ruta del contenedor, se ejecuta dentro de él. Desde PowerShell:

```powershell
Get-Content scripts/finops_validation.py | docker compose exec -T airflow-scheduler python -
```

**Resultados** sobre el histórico completo (salida completa en `scripts/finops_resultados.txt`):

| Métrica | Escaneo completo | Con pruning (enero 2017) |
|---|---|---|
| Archivos Parquet leídos | 25 | 1 |
| Filas leídas | 112.650 | 955 |
| Datos leídos | 379,1 kB | 26,0 kB |

El escaneo con filtro leyó **96 % menos archivos**, **99,2 % menos filas** y **93 % menos datos**. En el plan de ejecución, la evidencia está en el nodo *Table Scan*: `File Filters: (year = 2017)(CAST(month AS INTEGER) = 1)` y `Scanning Files: 1/25`, es decir, DuckDB aplicó el filtro sobre los nombres de las carpetas y descartó las otras 24 particiones sin abrirlas.

Las mismas dos consultas están en `sql/consultas_gold.sql`; al ejecutarlas localmente con `scripts/ejecutar_sql.py` se reprodujeron los mismos resultados en otra máquina (salida en `docs/resultados_consultas_sql.txt`).

---

## Flujo de trabajo con Git

- `main`: versión estable, protegida; solo se modifica por Pull Request con una aprobación.
- `develop`: rama de integración.
- `feature/...`: una rama por tarea (`feature/eda`, `feature/pii-sintetica`, `feature/gold-particiones`, `feature/duckdb-catalogo`, `feature/backfill-historico`, `feature/sql-consultas`, `feature/readme`, etc.), integrada a `develop` por Pull Request revisado por otra integrante.

---

## Migración a AWS

| Componente local | Servicio de AWS |
|---|---|
| `raw/`, `data/bronze`, `data/silver`, `data/gold` | Amazon S3 (un bucket o prefijo por capa) |
| Airflow en Docker | Amazon MWAA (Airflow administrado) |
| Tareas con pandas | AWS Glue jobs, o tareas en MWAA para volúmenes chicos |
| Catálogo DuckDB | AWS Glue Data Catalog |
| Consultas DuckDB | Amazon Athena |
| Variable `PII_SALT` | AWS Secrets Manager o SSM Parameter Store, como backend de secretos de Airflow |

**Cambios de código principales:** reemplazar las rutas locales (`Path(...)`) por rutas `s3://` y leer y escribir con `pyarrow`/`s3fs` o `awswrangler`; reemplazar `shutil.copyfile` por copias entre objetos de S3; registrar la tabla Gold y sus particiones en el Glue Data Catalog; y configurar el backend de secretos para que `Variable.get("PII_SALT")` siga funcionando sin cambios en las tareas.

---

## Mejoras futuras

Durante el análisis exploratorio se identificaron mejoras que quedaron fuera del alcance de esta versión del pipeline:

- **`geolocation`:** deduplicar las 261.831 filas repetidas y dejar una fila por código postal (promedio de latitud y longitud), para poder cruzarla con clientes y vendedores.
- **`order_reviews`:** procesarla en Silver con la clave (`review_id`, `order_id`) y dejar una reseña por pedido (la más reciente), para no duplicar filas al unirla con pedidos.
- **`sellers` y `geolocation`:** tratar el código postal como texto, igual que en clientes, para no perder los ceros iniciales.
- **Pedidos `delivered` con fechas faltantes** (14 sin aprobación, 2 sin despacho, 8 sin entrega): marcarlos con una columna de calidad, para excluirlos de los cálculos de tiempos de entrega.
- **`product_category_name_translation`:** incorporarla al pipeline y completar a mano las 2 categorías sin traducción (`pc_gamer` y `portateis_cozinha_e_preparadores_de_alimentos`).
- **Teléfonos de la PII:** normalizarlos a solo dígitos antes de hashear, porque llegan con formatos distintos (con y sin `+55`) y un mismo número escrito de dos formas daría hashes distintos.
- **PII en Gold:** incorporar `email_hash` a la tabla analítica, para mostrar uniones por hash sin exponer el dato original.
