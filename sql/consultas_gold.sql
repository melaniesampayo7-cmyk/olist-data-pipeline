-- =====================================================================
-- Consultas SQL sobre la capa Gold (DuckDB)
--
-- Las rutas son relativas a la raíz del repositorio, para ejecutarlas
-- localmente con DuckDB. Dentro del pipeline (contenedor de Airflow)
-- la misma carpeta está en /opt/airflow/data/gold/ventas.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. Catálogo: vista sobre todas las particiones de Gold
--    (equivalente a la que crea la tarea gold_catalogo)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW ventas AS
SELECT *
FROM read_parquet(
    'data/gold/ventas/**/*.parquet',
    hive_partitioning = true
);


-- ---------------------------------------------------------------------
-- 2. Control: filas por partición (año y mes de compra)
-- ---------------------------------------------------------------------
SELECT year, month, COUNT(*) AS filas
FROM ventas
GROUP BY year, month
ORDER BY year, month;


-- ---------------------------------------------------------------------
-- 3. Validación FinOps: escaneo completo (sin filtro de partición)
-- ---------------------------------------------------------------------
EXPLAIN ANALYZE
SELECT year, month, COUNT(*) AS registros
FROM read_parquet(
    'data/gold/ventas/**/*.parquet',
    hive_partitioning = true
)
GROUP BY year, month;


-- ---------------------------------------------------------------------
-- 4. Validación FinOps: escaneo con pruning (una sola partición)
-- ---------------------------------------------------------------------
EXPLAIN ANALYZE
SELECT year, month, COUNT(*) AS registros
FROM read_parquet(
    'data/gold/ventas/**/*.parquet',
    hive_partitioning = true
)
WHERE year = 2017 AND month = 1
GROUP BY year, month;


-- ---------------------------------------------------------------------
-- 5. Análisis: facturación de productos por mes
--    (precio + flete de cada ítem, Gold tiene una fila por ítem)
-- ---------------------------------------------------------------------
SELECT
    year,
    month,
    COUNT(DISTINCT order_id) AS pedidos,
    COUNT(*) AS items,
    ROUND(SUM(price + freight_value), 2) AS facturacion
FROM ventas
GROUP BY year, month
ORDER BY year, month;



-- ---------------------------------------------------------------------
-- 6. Análisis: top 10 categorías por facturación
-- ---------------------------------------------------------------------
SELECT
    product_category_name AS categoria,
    COUNT(*) AS items,
    ROUND(SUM(price), 2) AS facturacion
FROM ventas
GROUP BY product_category_name
ORDER BY facturacion DESC
LIMIT 10;


-- ---------------------------------------------------------------------
-- 7. Análisis: pedidos y ticket promedio por estado del cliente
--    total_pagado se repite en cada ítem del pedido, por eso
--    primero se lleva a una fila por pedido.
-- ---------------------------------------------------------------------
WITH pedidos AS (
    SELECT DISTINCT order_id, customer_state, total_pagado
    FROM ventas
)
SELECT
    customer_state AS estado,
    COUNT(*) AS pedidos,
    ROUND(AVG(total_pagado), 2) AS ticket_promedio
FROM pedidos
GROUP BY customer_state
ORDER BY pedidos DESC;