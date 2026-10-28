# EDA de Olist: hallazgos de calidad de datos

Dataset: 9 tablas CSV. Tabla más grande: `geolocation` (1.000.163 filas).

## Problemas encontrados y propuesta para Silver

| Tabla | Hallazgo | Propuesta |
|---|---|---|
| geolocation | 261.831 filas duplicadas completas | Deduplicar con `drop_duplicates` |
| geolocation | Aun sin duplicados, varias coordenadas por código postal | Agregar a una fila por `zip_code_prefix` (promedio de lat/lng) antes de cruzar |
| customers, sellers, geolocation | `zip_code_prefix` es int64 y pierde ceros iniciales | Convertir a texto en las tres tablas |
| customers | 99.441 `customer_id` vs 96.096 `customer_unique_id` (un `customer_id` por pedido) | Generar la identidad sintética de PII por `customer_unique_id` y asignarla a cada `customer_id`, para que cada persona tenga los mismos datos en todos sus pedidos |
| orders | 5 columnas de fecha como texto (incluida `order_approved_at`) | Convertir a datetime |
| orders | Nulos en fechas de aprobación (160), despacho (1.783) y entrega (2.965) | Ver sección de nulos |
| orders | Pedidos `delivered` con fechas faltantes: 14 sin aprobación, 2 sin despacho, 8 sin entrega | Marcarlos y excluirlos de los cálculos de tiempos |
| order_items | `shipping_limit_date` como texto | Convertir a datetime |
| order_reviews | `review_id` repetido en 814 filas, con distintos `order_id` | Clave `(review_id, order_id)`; sin duplicados reales |
| order_reviews | 551 reseñas extra: hay pedidos con más de una reseña | Dejar una reseña por pedido (la más reciente según `review_answer_timestamp`) |
| order_reviews | Título 88% nulo y mensaje 59% nulo | Mantener nulo (comentario opcional) |
| order_reviews | 2 columnas de fecha como texto | Convertir a datetime |
| products | 610 nulos (1,85%) en categoría, largo de nombre/descripción y fotos | Imputar categoría con "desconocida"; mantener nulos en las numéricas |
| products | 2 nulos en peso y dimensiones | A definir con el grupo |
| products | Columnas con typo (`lenght`) | Renombrar a `length` |
| product_category_name_translation | 2 categorías de products sin traducción: `pc_gamer` y `portateis_cozinha_e_preparadores_de_alimentos` | Agregar las traducciones a mano |

Sin problemas de calidad: `order_payments` y `sellers` (salvo el código postal).

Total de columnas de fecha a convertir: 8 (5 en orders, 2 en reviews, 1 en order_items).
Nota: el filtro por nombre `date|timestamp` no detecta `order_approved_at`.

## Nulos en las fechas de `orders`

Hay 2.965 nulos en `order_delivered_customer_date`. 2.957 corresponden a pedidos
no entregados todavía (shipped, canceled, unavailable, invoiced, processing,
created, approved). Decisión propuesta: no eliminar ni imputar; el nulo significa
"todavía no entregado".

Los nulos de `order_approved_at` (160) y `order_delivered_carrier_date` (1.783)
siguen el mismo patrón: corresponden a pedidos que no llegaron a esa etapa
(canceled, created, unavailable, invoiced, processing, approved). Las excepciones
son pedidos en estado `delivered` con fechas faltantes (14 sin aprobación,
2 sin despacho, 8 sin entrega), que se marcan y se excluyen de los cálculos de tiempos.

## Duplicados en reviews

El `review_id` por sí solo no es único, pero `(review_id, order_id)` sí lo es
(0 duplicados). Los repetidos son una misma encuesta asociada a varios pedidos
(mismo puntaje y fecha). No se eliminan filas por este motivo.

El caso inverso también existe: hay 551 reseñas extra en pedidos con más de una
reseña. Para evitar duplicar filas al unir con `orders`, en Silver se deja una
reseña por pedido (la más reciente según `review_answer_timestamp`).

## Tabla sintética de PII

Olist no incluye campos de identificación directa (nombre, email, teléfono):
los identificadores de cliente ya vienen anonimizados. Para poder implementar la
gobernanza de PII que pide la consigna (hashing SHA-256 con salt), se generó una
tabla sintética con `scripts/generador_pii_sintetica.py` (Faker, semilla fija).

- La identidad (nombre, email, teléfono) se genera una vez por persona
  (`customer_unique_id`, 96.096 identidades).
- Cada `customer_id` hereda la identidad de su persona: la tabla final tiene
  99.441 filas, una por `customer_id`, y se une directamente con `orders`.
- Una misma persona tiene el mismo email en todos sus pedidos, por lo que su
  hash también es el mismo y permite hacer joins sobre el hash.
- Salida: `raw/pii_customers.csv`. No se sube al repositorio (está en `.gitignore`):
  para generarla, correr `python scripts/generador_pii_sintetica.py`.