# EDA de Olist: hallazgos de calidad de datos

Dataset: 9 tablas CSV. Tabla más grande: `geolocation` (1.000.163 filas).

## Problemas encontrados y propuesta para Silver

| Tabla | Hallazgo | Propuesta |
|---|---|---|
| geolocation | 261.831 filas duplicadas completas | Deduplicar con `drop_duplicates` |
| customers | `customer_zip_code_prefix` es int64 y pierde ceros iniciales | Convertir a texto |
| customers | 99.441 `customer_id` vs 96.096 `customer_unique_id` | Generar PII por `customer_unique_id` |
| orders | 7 columnas de fecha como texto | Convertir a datetime |
| orders | Nulos en fechas de aprobación y entrega | Ver sección de nulos |
| order_items | `shipping_limit_date` como texto | Convertir a datetime |
| order_reviews | `review_id` repetido en 814 filas, con distintos `order_id` | Clave `(review_id, order_id)`; sin duplicados reales |
| order_reviews | Título 88% nulo y mensaje 59% nulo | Mantener nulo (comentario opcional) |
| order_reviews | Fechas como texto | Convertir a datetime |
| products | 610 nulos (1,85%) en categoría, largo de nombre/descripción y fotos | Imputar categoría con "desconocida" |
| products | 2 nulos en peso y dimensiones | A definir con el grupo |
| products | Columnas con typo (`lenght`) | Renombrar a `length` |

## Nulos en `order_delivered_customer_date`

Hay 2.965 nulos. 2.957 corresponden a pedidos no entregados todavía
(shipped, canceled, unavailable, invoiced, processing, created, approved).
Decisión propuesta: no eliminar ni imputar; el nulo significa
"todavía no entregado". Además, 8 pedidos en estado `delivered` no tienen
fecha de entrega (inconsistencia del origen): a definir si se dejan o se marcan.

## Duplicados en reviews

El `review_id` por sí solo no es único, pero `(review_id, order_id)` sí lo es
(0 duplicados). No se eliminan filas en esa tabla.