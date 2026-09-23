# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Setup: catálogo, schemas e Volume
# MAGIC
# MAGIC **Etapa 4.2 (Coleta)** — prepara o "terreno" no Unity Catalog:
# MAGIC
# MAGIC | Objeto | Nome | Função |
# MAGIC |---|---|---|
# MAGIC | Catálogo | `mvp_moda` (ou `workspace`, se o Free Edition não permitir criar catálogos) | Agrupa todo o projeto |
# MAGIC | Schema | `bronze` | Dado bruto, exatamente como veio da fonte |
# MAGIC | Schema | `silver` | Dado limpo, tipado e deduplicado |
# MAGIC | Schema | `gold` | Modelo dimensional (estrela) e agregações para análise |
# MAGIC | Volume | `bronze.raw_files` | Pasta na nuvem onde os CSVs originais são carregados |
# MAGIC
# MAGIC Depois de rodar este notebook, faça o upload dos CSVs do Olist para o Volume (passo a passo no README).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

if IS_DATABRICKS and CATALOG == "mvp_moda":
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'MVP Engenharia de Dados: moda e e-commerce (Olist) sob a ótica de mídia paga'")
    except Exception as e:
        # Fallback: usa o catálogo padrão do Free Edition. Os notebooks seguintes detectam isso automaticamente.
        print(f"Não foi possível criar o catálogo 'mvp_moda' ({str(e).splitlines()[0][:150]}).")
        print("Usando o catálogo 'workspace' no lugar.")
        CATALOG = "workspace"
        BRONZE, SILVER, GOLD = f"{CATALOG}.bronze", f"{CATALOG}.silver", f"{CATALOG}.gold"
        RAW_DIR = f"/Volumes/{CATALOG}/bronze/raw_files"

for schema, descricao in [
    ("bronze", "Camada Bronze: dado bruto como veio da fonte, mais metadados de ingestão"),
    ("silver", "Camada Silver: dado limpo, tipado, padronizado e deduplicado"),
    ("gold", "Camada Gold: modelo dimensional (estrela) e agregações prontas para análise"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    if IS_DATABRICKS:
        spark.sql(f"COMMENT ON SCHEMA {CATALOG}.{schema} IS '{descricao}'")

if IS_DATABRICKS:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.raw_files COMMENT 'CSVs originais do Olist (Kaggle), sem alteração'")

print("Schemas criados:")
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferindo o upload
# MAGIC
# MAGIC Após carregar os CSVs no Volume (Catalog → `mvp_moda` → `bronze` → `raw_files` → *Upload to this volume*),
# MAGIC a célula abaixo lista os arquivos e confirma se todos os 8 esperados estão presentes.

# COMMAND ----------

presentes = sorted(f for f in os.listdir(RAW_DIR)) if os.path.isdir(RAW_DIR) else []
faltando = [a for a in ARQUIVOS_BRONZE if a not in presentes]

print(f"Arquivos em {RAW_DIR}:")
for f in presentes:
    tamanho = os.path.getsize(os.path.join(RAW_DIR, f)) / 1_048_576
    print(f"  - {f}  ({tamanho:,.1f} MB)")

if faltando:
    print("\nATENÇÃO, arquivos ainda não enviados:", faltando)
else:
    print("\nOK: todos os arquivos esperados estão no Volume.")
