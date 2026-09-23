# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuração compartilhada
# MAGIC
# MAGIC Este notebook **não é executado sozinho**: os demais o chamam com `%run ./00_config`.
# MAGIC Ele centraliza nomes de catálogo, schemas, tabelas e o caminho do Volume onde ficam os CSVs brutos,
# MAGIC para que qualquer mudança de nome seja feita em um único lugar.
# MAGIC
# MAGIC **Parâmetro:** `catalogo` (padrão `auto`). No modo `auto`, o projeto usa o catálogo `mvp_moda`; se o seu workspace não
# MAGIC permitir `CREATE CATALOG`, o notebook 01 cria os schemas no catálogo `workspace` (que já existe no Databricks Free Edition)
# MAGIC e os demais notebooks detectam isso sozinhos. Para forçar um catálogo, escreva o nome no widget `catalogo`.

# COMMAND ----------

import os

# Rodando no Databricks? (a variável abaixo existe em qualquer cluster/serverless do Databricks)
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

if IS_DATABRICKS:
    dbutils.widgets.text("catalogo", "auto", "Catálogo do projeto (auto = mvp_moda ou workspace)")
    CATALOG = dbutils.widgets.get("catalogo").strip()
    if CATALOG in ("", "auto"):
        _existentes = {r[0] for r in spark.sql("SHOW CATALOGS").collect()}
        if "mvp_moda" in _existentes:
            CATALOG = "mvp_moda"
        elif "workspace" in _existentes and spark.�atalog.databaseExists("workspace.bronze"):
            CATALOG = "workspace"       # fallback criado pelo notebook 01
        else:
            CATALOG = "mvp_moda"
    RAW_DIR = f"/Volumes/{CATALOG}/bronze/raw_files"
else:
    # Modo teste local (tests/teste_local.py). Não é usado no Databricks.
    CATALOG = os.environ.get("MVP_CATALOG", "spark_catalog")
    RAW_DIR = os.environ.get("MVP_RAW_DIR", "./raw_files")

BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

# Arquivos do dataset Olist que entram no pipeline (nome do CSV -> nome da tabela Bronze).
# olist_geolocation_dataset.csv foi propositalmente deixado de fora: não responde a nenhuma
# das perguntas de negócio (usamos UF/cidade que já vêm em customers e sellers).
ARQUIVOS_BRONZE = {
    "olist_customers_dataset.csv": "olist_customers",
    "olist_orders_dataset.csv": "olist_orders",
    "olist_order_items_dataset.csv": "olist_order_items",
    "olist_order_payments_dataset.csv": "olist_order_payments",
    "olist_order_reviews_dataset.csv": "olist_order_reviews",
    "olist_products_dataset.csv": "olist_products",
    "olist_sellers_dataset.csv": "olist_sellers",
    "product_category_name_translation.csv": "category_translation",
}

# Mapeamento categoria (nome original em português no dataset) -> segmento de moda.
# Toda categoria que começa com "fashion_" e não estiver aqui cai em "Moda - outros".
# "malas_acessorios" (malas e acessórios de viagem) entra como ponte moda × viagem.
SEGMENTOS_MODA = {
    "fashion_roupa_feminina": "Vestuário feminino",
    "fashion_roupa_masculina": "Vestuário masculino",
    "fashion_roupa_infanto_juvenil": "Vestuário infantojuvenil",
    "fashion_calcados": "Calçados",
    "fashion_bolsas_e_acessorios": "Bolsas e acessórios",
    "fashion_underwear_e_moda_praia": "Moda íntima e praia",
    "fashion_esporte": "Moda esportiva",
    "malas_acessorios": "Malas e acessórios de viagem",
}

REGIOES = {
    "Norte": ["AC", "AP", "AM", "PA", "RO", "RR", "TO"],
    "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Centro-Oeste": ["DF", "GO", "MT", "MS"],
    "Sudeste": ["ES", "MG", "RJ", "SP"],
    "Sul": ["PR", "RS", "SC"],
}
UFS_VALIDAS = [uf for ufs in REGIOES.values() for uf in ufs]

print(f"Catálogo: {CATALOG} | Bronze: {BRONZE} | Silver: {SILVER} | Gold: {GOLD}")
print(f"Pasta dos CSVs brutos: {RAW_DIR}")
