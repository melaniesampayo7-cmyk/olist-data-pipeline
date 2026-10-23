"""Genera la tabla sintética de PII (nombre, email, teléfono) para Olist.

- La identidad se genera por customer_unique_id (la persona real) y cada
  customer_id hereda la identidad de su persona.
- Es determinística: misma semilla y mismo orden dan el mismo resultado.
- Salida: raw/pii_customers.csv (ignorado por git).
"""
from pathlib import Path

import pandas as pd
from faker import Faker

SEED = 42
BASE = Path(__file__).resolve().parent.parent
ENTRADA = BASE / "raw" / "olist_customers_dataset.csv"
SALIDA = BASE / "raw" / "pii_customers.csv"

clientes = pd.read_csv(ENTRADA, usecols=["customer_id", "customer_unique_id"])

# Una identidad por persona, en orden fijo para que sea reproducible
personas = sorted(clientes["customer_unique_id"].unique())

Faker.seed(SEED)
fake = Faker("pt_BR")

filas = []
for uid in personas:
    filas.append({
        "customer_unique_id": uid,
        "full_name": fake.name(),
        # El sufijo del id garantiza emails únicos; example.com es un dominio
        # reservado, así que no coincide con ninguna casilla real.
        "email": f"{fake.user_name()}.{uid[:8]}@example.com",
        "phone_number": fake.phone_number(),
    })
identidades = pd.DataFrame(filas)

# Cada customer_id hereda la identidad de su customer_unique_id
pii = clientes.merge(identidades, on="customer_unique_id", how="left")
pii = pii.sort_values("customer_id").reset_index(drop=True)

# Validaciones
assert len(pii) == len(clientes), "cambió la cantidad de filas"
assert pii["customer_id"].is_unique, "customer_id repetido"
assert pii.notna().all().all(), "hay nulos"
assert identidades["email"].is_unique, "emails repetidos entre personas"

pii.to_csv(SALIDA, index=False)
print(f"OK: {len(pii)} filas, {len(identidades)} personas -> {SALIDA}")
print(pii.head())
