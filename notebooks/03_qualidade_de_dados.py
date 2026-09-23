# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Qualidade de dados (sobre a camada Bronze)
# MAGIC
# MAGIC **Etapa 4.5 (Qualidade de Dados)** — verifica o dado **bruto**, antes de qualquer tratamento, nas cinco dimensões pedidas:
# MAGIC
# MAGIC | Dimensão | Pergunta | Exemplo neste projeto |
# MAGIC |---|---|---|
# MAGIC | Completude | Há nulos ou vazios? Em que proporção? | categoria do produto nula |
# MAGIC | Consistência | O valor segue o padrão esperado? | UF fora das 27, CEP sem 5 dígitos, data não interpretável |
# MAGIC | Unicidade | Há duplicatas onde não deveria? | `review_id` repetido, chaves de item de pedido |
# MAGIC | Acurácia | O valor faz sentido no contexto? | preço ≤ 0, nota fora de 1–5, entrega antes da compra |
# MAGIC | Outliers | Há valores extremos que distorcem médias? | preço, frete e peso (regra do IQR) |
# MAGIC
# MAGIC Além dessas, checamos a **integridade referencial** (toda chave estrangeira tem correspondência?).
# MAGIC
# MAGIC **Saídas persistidas** em `silver`: `dq_resultados`, `dq_completude` e `dq_outliers`.
# MAGIC Cada regra traz o **tratamento** que o notebook 04 (Silver) aplica, então o problema e a solução ficam documentados lado a lado.
# MAGIC
# MAGIC > Este notebook só **mede**; ele não altera nenhum dado.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

resultados = []   # (tabela, coluna, dimensao, regra, total, problemas, pct, tratamento)


def registrar(tabela, coluna, dimensao, regra, total, problemas, tratamento):
    pct = round(100.0 * problemas / total, 4) if total else None
    resultados.append((tabela, coluna, dimensao, regra, int(total), int(problemas), pct, tratamento))


def bronze(tabela):
    return spark.table(f"{BRONZE}.{tabela}")


def checar(tabela, checks):
    """checks = [(coluna, dimensao, regra, condicao_SQL_que_indica_problema, tratamento), ...]
    Todas as regras da tabela são calculadas em uma única passada pelos dados."""
    df = bronze(tabela)
    aggs = [F.count(F.lit(1)).alias("_total")] + [
        F.sum(F.when(F.expr(cond), 1).otherwise(0)).alias(f"c{i}")
        for i, (_, _, _, cond, _) in enumerate(checks)
    ]
    linha = df.agg(*aggs).collect()[0]
    for i, (coluna, dimensao, regra, _, tratamento) in enumerate(checks):
        registrar(tabela, coluna, dimensao, regra, linha["_total"], linha[f"c{i}"] or 0, tratamento)


def unicidade(tabela, colunas, tratamento):
    df = bronze(tabela)
    total = df.count()
    distintos = df.select(*colunas).distinct().count()
    registrar(tabela, "+".join(colunas), "Unicidade", f"Chave ({', '.join(colunas)}) sem repetição", total, total - distintos, tratamento)


def integridade(tabela, coluna, ref_tabela, ref_coluna, tratamento):
    df = bronze(tabela).where(F.col(coluna).isNotNull())
    ref = bronze(ref_tabela).select(F.col(ref_coluna).alias("_ref")).distinct()
    total = df.count()
    orfaos = df.join(ref, df[coluna] == ref["_ref"], "left_anti").count()
    registrar(tabela, coluna, "Consistência (integridade referencial)",
              f"{coluna} existe em {ref_tabela}.{ref_coluna}", total, orfaos, tratamento)


UFS_SQL = ", ".join(f"'{uf}'" for uf in UFS_VALIDAS)
TS = "yyyy-MM-dd HH:mm:ss"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Completude: nulos e vazios em **todas** as colunas
# MAGIC Varre cada coluna de cada tabela Bronze e calcula a proporção de valores nulos ou em branco.

# COMMAND ----------

linhas_completude = []
for tabela in ARQUIVOS_BRONZE.values():
    df = bronze(tabela)
    colunas = [c for c in df.columns if not c.startswith("_")]
    aggs = [F.count(F.lit(1)).alias("_total")] + [
        F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), 1).otherwise(0)).alias(c)
        for c in colunas
    ]
    r = df.agg(*aggs).collect()[0]
    for c in colunas:
        nulos = r[c] or 0
        linhas_completude.append((tabela, c, int(r["_total"]), int(nulos), round(100.0 * nulos / r["_total"], 4) if r["_total"] else None))

df_completude = spark.createDataFrame(linhas_completude, "tabela string, coluna string, total long, nulos long, pct_nulos double")
df_completude.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.dq_completude")

print("Colunas com algum valor nulo/vazio (ordenadas pela proporção):")
display(spark.table(f"{SILVER}.dq_completude").where("nulos > 0").orderBy(F.desc("pct_nulos")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Regras por tabela (consistência, acurácia)

# COMMAND ----------

# --- customers ---------------------------------------------------------------------------------
checar("olist_customers", [
    ("customer_id", "Completude", "customer_id preenchido", "customer_id is null", "Registro enviado para quarentena"),
    ("customer_zip_code_prefix", "Consistência", "CEP com exatamente 5 dígitos",
     "customer_zip_code_prefix rlike '^[0-9]+$' and length(customer_zip_code_prefix) <> 5",
     "lpad com zeros à esquerda até 5 dígitos (o zero inicial se perde na exportação do CSV)"),
    ("customer_state", "Consistência", "UF pertence às 27 UFs", f"upper(trim(customer_state)) not in ({UFS_SQL})",
     "UF inválida vira 'NI' (não informado); região 'Não informada'"),
    ("customer_city", "Consistência", "Cidade sem espaços nas pontas", "customer_city <> trim(customer_city)",
     "trim + initcap para padronizar a caixa do texto"),
])
unicidade("olist_customers", ["customer_id"], "Deduplicação por customer_id (mantém a primeira ocorrência)")

# --- orders ------------------------------------------------------------------------------------
STATUS_VALIDOS = "'delivered','shipped','canceled','unavailable','invoiced','processing','created','approved'"
checar("olist_orders", [
    ("order_id", "Completude", "order_id preenchido", "order_id is null", "Quarentena"),
    ("customer_id", "Completude", "customer_id preenchido", "customer_id is null", "Quarentena"),
    ("order_status", "Consistência", "Status dentro do domínio esperado", f"lower(trim(order_status)) not in ({STATUS_VALIDOS})",
     "Padronizado em minúsculas; valor fora do domínio é mantido e sinalizado na análise"),
    ("order_purchase_timestamp", "Completude", "Data de compra preenchida", "order_purchase_timestamp is null", "Quarentena (sem data de compra o pedido não entra na análise temporal)"),
    ("order_purchase_timestamp", "Consistência", "Data de compra interpretável como timestamp",
     f"order_purchase_timestamp is not null and try_to_timestamp(order_purchase_timestamp, '{TS}') is null", "Quarentena"),
    ("order_delivered_customer_date", "Acurácia", "Pedido 'delivered' possui data de entrega",
     "lower(order_status) = 'delivered' and order_delivered_customer_date is null",
     "Mantido; métricas de prazo ficam nulas para esses pedidos"),
    ("order_delivered_customer_date", "Acurácia", "Entrega ocorre depois da compra",
     f"try_to_timestamp(order_delivered_customer_date, '{TS}') < try_to_timestamp(order_purchase_timestamp, '{TS}')",
     "Sinalizado em flag_datas_inconsistentes; métricas de prazo anuladas"),
    ("order_delivered_carrier_date", "Acurácia", "Postagem ocorre depois da compra",
     f"try_to_timestamp(order_delivered_carrier_date, '{TS}') < try_to_timestamp(order_purchase_timestamp, '{TS}')",
     "Sinalizado em flag_datas_inconsistentes"),
    ("order_status", "Acurácia", "Pedido cancelado não deveria ter data de entrega",
     "lower(order_status) = 'canceled' and order_delivered_customer_date is not null",
     "Mantido; análises de receita consideram só pedidos não cancelados/indisponíveis"),
])
unicidade("olist_orders", ["order_id"], "Deduplicação por order_id")
integridade("olist_orders", "customer_id", "olist_customers", "customer_id", "Left join no Gold; cliente ausente vira membro 'Não informado' (sk = -1)")

# --- order_items -------------------------------------------------------------------------------
checar("olist_order_items", [
    ("order_id", "Completude", "order_id preenchido", "order_id is null", "Quarentena"),
    ("product_id", "Completude", "product_id preenchido", "product_id is null", "Quarentena"),
    ("price", "Consistência", "Preço numérico", "price is not null and try_cast(price as double) is null", "Quarentena"),
    ("price", "Acurácia", "Preço maior que zero", "try_cast(price as double) <= 0", "Quarentena"),
    ("freight_value", "Acurácia", "Frete maior ou igual a zero", "try_cast(freight_value as double) < 0", "Quarentena"),
    ("freight_value", "Acurácia", "Frete zerado (possível frete grátis)", "try_cast(freight_value as double) = 0",
     "Mantido: é informação de negócio (frete grátis), não erro"),
    ("shipping_limit_date", "Consistência", "Data limite de postagem interpretável",
     f"shipping_limit_date is not null and try_to_timestamp(shipping_limit_date, '{TS}') is null", "Convertida para nula quando inválida"),
])
unicidade("olist_order_items", ["order_id", "order_item_id"], "Deduplicação por (order_id, item_seq)")
integridade("olist_order_items", "order_id", "olist_orders", "order_id", "Item sem pedido correspondente vai para quarentena")
integridade("olist_order_items", "product_id", "olist_products", "product_id", "Left join no Gold; produto ausente vira membro 'Não informado'")
integridade("olist_order_items", "seller_id", "olist_sellers", "seller_id", "Left join no Gold; vendedor ausente vira membro 'Não informado'")

# --- order_payments ----------------------------------------------------------------------------
checar("olist_order_payments", [
    ("payment_type", "Consistência", "Tipo de pagamento dentro do domínio",
     "lower(trim(payment_type)) not in ('credit_card','boleto','voucher','debit_card','not_defined')",
     "Padronizado em minúsculas; 'not_defined' renomeado para 'nao_definido'"),
    ("payment_installments", "Acurácia", "Parcelas maiores ou iguais a 1", "try_cast(payment_installments as int) < 1",
     "Ajustado para 1 parcela e sinalizado em flag_parcelas_corrigida"),
    ("payment_value", "Acurácia", "Valor pago maior ou igual a zero", "try_cast(payment_value as double) < 0", "Quarentena"),
    ("payment_value", "Acurácia", "Valor pago diferente de zero", "try_cast(payment_value as double) = 0",
     "Mantido (ex.: voucher integral); somado normalmente"),
])
unicidade("olist_order_payments", ["order_id", "payment_sequential"], "Deduplicação por (order_id, payment_sequential)")
integridade("olist_order_payments", "order_id", "olist_orders", "order_id", "Pagamento sem pedido é descartado no Gold (join interno com pedidos)")

# --- order_reviews -----------------------------------------------------------------------------
checar("olist_order_reviews", [
    ("review_score", "Acurácia", "Nota entre 1 e 5", "try_cast(review_score as int) not between 1 and 5 or try_cast(review_score as int) is null",
     "Quarentena"),
    ("review_creation_date", "Consistência", "Data da avaliação interpretável",
     f"review_creation_date is not null and try_to_timestamp(review_creation_date, '{TS}') is null", "Convertida para nula quando inválida"),
])
unicidade("olist_order_reviews", ["review_id"], "review_id repetido em pedidos diferentes é aceito; a chave analítica passa a ser order_id")
unicidade("olist_order_reviews", ["order_id"], "Mantida apenas a avaliação mais recente por pedido (1 avaliação por order_id)")
integridade("olist_order_reviews", "order_id", "olist_orders", "order_id", "Avaliação sem pedido é descartada")

# --- products ----------------------------------------------------------------------------------
checar("olist_products", [
    ("product_id", "Completude", "product_id preenchido", "product_id is null", "Quarentena"),
    ("product_category_name", "Completude", "Categoria preenchida", "product_category_name is null or trim(product_category_name) = ''",
     "Categoria = 'sem_categoria'"),
    ("product_weight_g", "Acurácia", "Peso maior que zero", "try_cast(product_weight_g as double) <= 0", "Peso anulado (nulo)"),
    ("product_length_cm", "Acurácia", "Comprimento maior que zero", "try_cast(product_length_cm as double) <= 0", "Dimensão anulada (nulo)"),
    ("product_height_cm", "Acurácia", "Altura maior que zero", "try_cast(product_height_cm as double) <= 0", "Dimensão anulada (nulo)"),
    ("product_width_cm", "Acurácia", "Largura maior que zero", "try_cast(product_width_cm as double) <= 0", "Dimensão anulada (nulo)"),
])
unicidade("olist_products", ["product_id"], "Deduplicação por product_id")
integridade("olist_products", "product_category_name", "category_translation", "product_category_name",
            "Sem tradução: usa-se o próprio nome em português como categoria em inglês (coalesce)")

# --- sellers -----------------------------------------------------------------------------------
checar("olist_sellers", [
    ("seller_id", "Completude", "seller_id preenchido", "seller_id is null", "Quarentena"),
    ("seller_state", "Consistência", "UF pertence às 27 UFs", f"upper(trim(seller_state)) not in ({UFS_SQL})", "UF inválida vira 'NI'"),
    ("seller_zip_code_prefix", "Consistência", "CEP com exatamente 5 dígitos",
     "seller_zip_code_prefix rlike '^[0-9]+$' and length(seller_zip_code_prefix) <> 5", "lpad até 5 dígitos"),
])
unicidade("olist_sellers", ["seller_id"], "Deduplicação por seller_id")

# --- category_translation ----------------------------------------------------------------------
unicidade("category_translation", ["product_category_name"], "Deduplicação por categoria")

print(f"{len(resultados)} verificações executadas até aqui.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Outliers (regra do IQR)
# MAGIC Para as variáveis numéricas que mais pesam nas análises: limite inferior = Q1 − 1,5·IQR e limite superior = Q3 + 1,5·IQR.
# MAGIC Valores fora dos limites são contados e **mantidos**: preço alto de moda/viagem pode ser legítimo,
# MAGIC então as análises usam **mediana e percentis** ao lado da média para não se deixar distorcer.

# COMMAND ----------

linhas_outliers = []
for tabela, coluna in [("olist_order_items", "price"), ("olist_order_items", "freight_value"), ("olist_products", "product_weight_g")]:
    df = bronze(tabela).select(F.expr(f"try_cast({coluna} as double)").alias("v")).where("v is not null")
    q = df.agg(
        F.min("v").alias("minimo"),
        F.expr("percentile_approx(v, 0.25)").alias("q1"),
        F.expr("percentile_approx(v, 0.50)").alias("mediana"),
        F.expr("percentile_approx(v, 0.75)").alias("q3"),
        F.max("v").alias("maximo"),
        F.count("v").alias("n"),
    ).collect()[0]
    iqr = q["q3"] - q["q1"]
    li, ls = q["q1"] - 1.5 * iqr, q["q3"] + 1.5 * iqr
    qtd = df.where((F.col("v") < li) | (F.col("v") > ls)).count()
    linhas_outliers.append((tabela, coluna, q["minimo"], q["q1"], q["mediana"], q["q3"], q["maximo"], li, ls, int(qtd), round(100.0 * qtd / q["n"], 4)))
    registrar(tabela, coluna, "Outliers", "Valor dentro de [Q1 - 1,5·IQR ; Q3 + 1,5·IQR]", q["n"], qtd,
              "Mantidos e sinalizados; análises usam mediana/percentis além da média")

df_out = spark.createDataFrame(linhas_outliers,
    "tabela string, coluna string, minimo double, q1 double, mediana double, q3 double, maximo double, "
    "limite_inferior double, limite_superior double, qtd_outliers long, pct_outliers double")
df_out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.dq_outliers")
display(spark.table(f"{SILVER}.dq_outliers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Categorias de moda encontradas
# MAGIC Confere quais categorias reais do dataset foram classificadas como moda (mapeamento definido em `00_config`).
# MAGIC Se alguma categoria de moda não aparecer aqui, ajuste `SEGMENTOS_MODA`.

# COMMAND ----------

cat_moda = (bronze("olist_products")
    .groupBy("product_category_name").count()
    .where(F.col("product_category_name").rlike("^fashion_") | F.col("product_category_name").isin(list(SEGMENTOS_MODA)))
    .withColumn("segmento", F.coalesce(*[F.when(F.col("product_category_name") == k, F.lit(v)) for k, v in SEGMENTOS_MODA.items()], F.lit("Moda - outros")))
    .orderBy(F.desc("count")))
display(cat_moda)
assert cat_moda.count() > 0, "Nenhuma categoria de moda encontrada: confira os nomes das categorias em 00_config."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Consolidando e persistindo o relatório de qualidade

# COMMAND ----------

schema_dq = "tabela string, coluna string, dimensao string, regra string, total_registros long, registros_com_problema long, pct_problema double, tratamento string"
df_dq = spark.createDataFrame(resultados, schema_dq).withColumn("verificado_em", F.current_timestamp())
df_dq.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.dq_resultados")

print("Regras com problemas encontrados (ordenadas pela proporção):")
display(spark.table(f"{SILVER}.dq_resultados").where("registros_com_problema > 0").orderBy(F.desc("pct_problema")))

# COMMAND ----------

print("Resumo por dimensão de qualidade:")
display(spark.sql(f"""
    SELECT dimensao,
           COUNT(*)                                             AS regras_verificadas,
           SUM(CASE WHEN registros_com_problema > 0 THEN 1 ELSE 0 END) AS regras_com_problema,
           SUM(registros_com_problema)                          AS registros_afetados
    FROM {SILVER}.dq_resultados
    GROUP BY dimensao
    ORDER BY dimensao
"""))
