# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze: ingestão dos CSVs brutos
# MAGIC
# MAGIC **Etapa 4.2 (Coleta) e 4.4 (ETL 1 de 4)** — lê cada CSV do Volume e grava uma tabela Delta na camada Bronze.
# MAGIC
# MAGIC **Regras da camada Bronze**
# MAGIC 1. Nenhuma limpeza: todas as colunas ficam como `string`, exatamente como vieram (inclusive os erros de digitação
# MAGIC    do dataset original, como `product_name_lenght`). Isso preserva rastreabilidade: se algo der errado adiante, o original está aqui.
# MAGIC 2. Acrescentamos apenas **metadados de controle**: `_ingested_at` (quando entrou), `_source_file` (de qual arquivo veio).
# MAGIC 3. A carga é **idempotente** (`overwrite`): rodar de novo produz o mesmo resultado, sem duplicar linhas.
# MAGIC
# MAGIC **Detalhe técnico:** `olist_order_reviews_dataset.csv` tem comentários com quebras de linha e aspas dentro do texto,
# MAGIC por isso a leitura usa `multiLine=true` e `escape='"'`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

resumo = []

for arquivo, tabela in ARQUIVOS_BRONZE.items():
    caminho = f"{RAW_DIR}/{arquivo}"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)     # tudo string: Bronze não interpreta o dado
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(caminho)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(arquivo))
    )

    destino = f"{BRONZE}.{tabela}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino)

    n = spark.table(destino).count()
    resumo.append((destino, n, len(df.columns)))
    print(f"{destino:<45} {n:>10,} linhas | {len(df.columns)} colunas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência da carga
# MAGIC Tabela-resumo (útil como evidência): nome, linhas e colunas de cada tabela Bronze persistida.

# COMMAND ----------

display(spark.createDataFrame(resumo, ["tabela_bronze", "linhas", "colunas"]))

# COMMAND ----------

# Amostra do dado bruto (note que tudo é string e há colunas de metadados no fim)
display(spark.table(f"{BRONZE}.olist_orders").limit(5))
