# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Silver: limpeza, tipagem e padronização
# MAGIC
# MAGIC **Etapa 4.4 (ETL 2 de 4)** — transforma cada tabela Bronze em uma tabela Silver confiável.
# MAGIC Cada transformação abaixo responde a um problema medido no notebook 03 (qualidade de dados).
# MAGIC
# MAGIC | Transformação | Por quê | Impacto |
# MAGIC |---|---|---|
# MAGIC | Conversão de tipos com `try_cast` / `try_to_timestamp` | Bronze é tudo texto; valores inválidos viram nulo em vez de derrubar o job | Datas, números e inteiros de verdade |
# MAGIC | Nomes de colunas em português, `snake_case` | Padronização e legibilidade do modelo | Corrige também os typos da fonte (`lenght`) |
# MAGIC | `trim`, `lower`/`upper`, `initcap`, `lpad` do CEP | Padronizar texto e recuperar o zero à esquerda do CEP | Joins e agrupamentos consistentes |
# MAGIC | Deduplicação por chave | Garantir unicidade | 1 linha por chave de negócio |
# MAGIC | **Quarentena** (`silver.quarentena_registros`) | Registros que violam regra dura (chave nula, preço ≤ 0, nota fora de 1–5, órfãos) não entram na Silver, mas não são perdidos | Rastreabilidade do que foi rejeitado e por quê |
# MAGIC | Flags e métricas derivadas (`dias_ate_entrega`, `atraso_dias`, `flag_*`) | Enriquecer sem perder o dado original | Prazo de entrega e sinalização de inconsistências |
# MAGIC | Segmento de moda (`segmento_moda`, `eh_moda`) | Isolar o universo de moda (+ malas de viagem) do resto do marketplace | Base para todas as perguntas de negócio |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F, Window

TS = "yyyy-MM-dd HH:mm:ss"
quarentena_partes = []


def b(tabela):
    return spark.table(f"{BRONZE}.{tabela}")


def ts(coluna):
    return F.expr(f"try_to_timestamp({coluna}, '{TS}')")


def num(coluna, tipo="double"):
    return F.expr(f"try_cast({coluna} as {tipo})")


def coluna_existente(df, candidatas):
    """Devolve o primeiro nome de coluna que existe (a fonte tem typos como 'lenght')."""
    for c in candidatas:
        if c in df.columns:
            return c
    return None


def separar(df, tabela_origem, chave, regras):
    """Separa registros válidos e inválidos.
    regras = [(motivo, expressão SQL que indica registro INVÁLIDO), ...]. A primeira regra violada define o motivo.
    Os inválidos vão para a lista de quarentena (gravada no fim do notebook) e os válidos seguem no fluxo."""
    motivo = F.coalesce(*[F.when(F.expr(cond), F.lit(m)) for m, cond in regras])
    df = df.withColumn("_motivo", motivo)
    campos = [c for c in df.columns if not c.startswith("_")]
    invalidos = df.where("_motivo is not null").select(
        F.lit(tabela_origem).alias("tabela_origem"),
        F.concat_ws("|", *[F.col(c).cast("string") for c in chave]).alias("chave"),
        F.col("_motivo").alias("motivo"),
        F.to_json(F.struct(*campos)).alias("registro_json"),
    )
    quarentena_partes.append(invalidos)
    return df.where("_motivo is null").drop("_motivo")


def dedup(df, chave, ordem):
    w = Window.partitionBy(*chave).orderBy(*ordem)
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def gravar(df, nome):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.{nome}")
    print(f"{SILVER}.{nome:<18} {spark.table(f'{SILVER}.{nome}').count():>10,} linhas")


UF_REGIAO = {uf: regiao for regiao, ufs in REGIOES.items() for uf in ufs}
MAPA_REGIAO = F.create_map(*[x for uf, r in UF_REGIAO.items() for x in (F.lit(uf), F.lit(r))])
UFS_SQL = ", ".join(f"'{uf}'" for uf in UFS_VALIDAS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 `silver.clientes`
# MAGIC Padroniza UF (inválida → `NI`), recupera o zero à esquerda do CEP (`lpad` até 5 dígitos), padroniza a cidade e deriva a **região** a partir da UF.

# COMMAND ----------

c = separar(b("olist_customers"), "olist_customers", ["customer_id"], [("customer_id nulo", "customer_id is null")])

clientes = c.select(
    F.trim("customer_id").alias("customer_id"),
    F.trim("customer_unique_id").alias("cliente_unico_id"),
    F.lpad(F.trim("customer_zip_code_prefix"), 5, "0").alias("cep_prefixo"),
    F.initcap(F.trim("customer_city")).alias("cidade"),
    F.when(F.upper(F.trim("customer_state")).isin(UFS_VALIDAS), F.upper(F.trim("customer_state"))).otherwise("NI").alias("uf"),
    F.col("_ingested_at").alias("ingerido_em"),
)
clientes = clientes.withColumn("regiao", F.coalesce(F.try_element_at(MAPA_REGIAO, F.col("uf")), F.lit("Não informada")))
clientes = dedup(clientes, ["customer_id"], [F.col("ingerido_em")])
gravar(clientes, "clientes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 `silver.vendedores`

# COMMAND ----------

s = separar(b("olist_sellers"), "olist_sellers", ["seller_id"], [("seller_id nulo", "seller_id is null")])
vendedores = s.select(
    F.trim("seller_id").alias("seller_id"),
    F.lpad(F.trim("seller_zip_code_prefix"), 5, "0").alias("cep_prefixo"),
    F.initcap(F.trim("seller_city")).alias("cidade"),
    F.when(F.upper(F.trim("seller_state")).isin(UFS_VALIDAS), F.upper(F.trim("seller_state"))).otherwise("NI").alias("uf"),
    F.col("_ingested_at").alias("ingerido_em"),
)
gravar(dedup(vendedores, ["seller_id"], [F.col("ingerido_em")]), "vendedores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 `silver.produtos`
# MAGIC * **Join** com `category_translation` pelo nome da categoria (traz o nome em inglês; sem tradução, mantém o nome em português via `coalesce`).
# MAGIC * Categoria nula → `sem_categoria`. Peso e dimensões ≤ 0 viram nulos.
# MAGIC * `segmento_moda` / `eh_moda`: classifica a categoria conforme `SEGMENTOS_MODA` (`00_config`). Qualquer `fashion_*` fora do mapa vira `Moda - outros`.

# COMMAND ----------

p_raw = separar(b("olist_products"), "olist_products", ["product_id"], [("product_id nulo", "product_id is null")])

trad = (b("category_translation")
        .select(F.trim("product_category_name").alias("_cat_pt"), F.trim("product_category_name_english").alias("categoria_en_trad"))
        .dropDuplicates(["_cat_pt"]))

col_nome_len = coluna_existente(p_raw, ["product_name_lenght", "product_name_length"])
col_desc_len = coluna_existente(p_raw, ["product_description_lenght", "product_description_length"])

p = p_raw.select(
    F.trim("product_id").alias("product_id"),
    F.coalesce(F.nullif(F.trim("product_category_name"), F.lit("")), F.lit("sem_categoria")).alias("categoria_pt"),
    num(col_nome_len, "int").alias("nome_qtd_caracteres"),
    num(col_desc_len, "int").alias("descricao_qtd_caracteres"),
    num("product_photos_qty", "int").alias("fotos_qtd"),
    num("product_weight_g").alias("_peso"),
    num("product_length_cm").alias("_comp"),
    num("product_height_cm").alias("_alt"),
    num("product_width_cm").alias("_larg"),
    F.col("_ingested_at").alias("ingerido_em"),
)
for novo, antigo in [("peso_g", "_peso"), ("comprimento_cm", "_comp"), ("altura_cm", "_alt"), ("largura_cm", "_larg")]:
    p = p.withColumn(novo, F.when(F.col(antigo) > 0, F.col(antigo))).drop(antigo)

p = p.join(trad, p["categoria_pt"] == trad["_cat_pt"], "left").drop("_cat_pt")
p = p.withColumn("categoria_en", F.coalesce(F.col("categoria_en_trad"), F.col("categoria_pt"))).drop("categoria_en_trad")

MAPA_SEG = F.create_map(*[x for k, v in SEGMENTOS_MODA.items() for x in (F.lit(k), F.lit(v))])
p = p.withColumn(
    "segmento_moda",
    F.coalesce(F.try_element_at(MAPA_SEG, F.col("categoria_pt")),
               F.when(F.col("categoria_pt").startswith("fashion_"), F.lit("Moda - outros"))),
).withColumn("eh_moda", F.col("segmento_moda").isNotNull())

produtos = dedup(p, ["product_id"], [F.col("ingerido_em")]).select(
    "product_id", "categoria_pt", "categoria_en", "segmento_moda", "eh_moda", "nome_qtd_caracteres",
    "descricao_qtd_caracteres", "fotos_qtd", "peso_g", "comprimento_cm", "altura_cm", "largura_cm", "ingerido_em")
gravar(produtos, "produtos")

print("Segmentos de moda encontrados (produtos):")
display(spark.table(f"{SILVER}.produtos").groupBy("segmento_moda").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 `silver.pedidos`
# MAGIC * Converte as cinco colunas de data para `timestamp`.
# MAGIC * Deriva `dias_ate_entrega` (compra → entrega ao cliente), `atraso_dias` (entrega − data estimada; positivo = atrasou) e `entregue_no_prazo`,
# MAGIC   **somente** para pedidos `delivered` com datas coerentes.
# MAGIC * `flag_datas_inconsistentes` marca entrega/postagem anterior à compra; nesses casos as métricas de prazo ficam nulas.

# COMMAND ----------

o = separar(b("olist_orders"), "olist_orders", ["order_id"], [
    ("order_id nulo", "order_id is null"),
    ("customer_id nulo", "customer_id is null"),
    ("data de compra ausente ou inválida", f"try_to_timestamp(order_purchase_timestamp, '{TS}') is null"),
])

ped = o.select(
    F.trim("order_id").alias("order_id"),
    F.trim("customer_id").alias("customer_id"),
    F.lower(F.trim("order_status")).alias("status"),
    ts("order_purchase_timestamp").alias("dt_compra"),
    ts("order_approved_at").alias("dt_aprovacao"),
    ts("order_delivered_carrier_date").alias("dt_postagem"),
    ts("order_delivered_customer_date").alias("dt_entrega"),
    ts("order_estimated_delivery_date").alias("dt_entrega_estimada"),
    F.col("_ingested_at").alias("ingerido_em"),
)
ped = dedup(ped, ["order_id"], [F.col("ingerido_em")])

inconsistente = F.coalesce((F.col("dt_entrega") < F.col("dt_compra")) | (F.col("dt_postagem") < F.col("dt_compra")), F.lit(False))
entregue_ok = (F.col("status") == "delivered") & F.col("dt_entrega").isNotNull() & ~inconsistente

pedidos = (ped
    .withColumn("flag_datas_inconsistentes", inconsistente)
    .withColumn("dias_ate_entrega", F.when(entregue_ok, F.datediff(F.to_date("dt_entrega"), F.to_date("dt_compra"))))
    .withColumn("atraso_dias", F.when(entregue_ok & F.col("dt_entrega_estimada").isNotNull(),
                                      F.datediff(F.to_date("dt_entrega"), F.to_date("dt_entrega_estimada"))))
    .withColumn("entregue_no_prazo", F.when(F.col("atraso_dias").isNotNull(), F.col("atraso_dias") <= 0))
)
gravar(pedidos, "pedidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 `silver.itens_pedido`
# MAGIC * Converte `price` e `freight_value` para `decimal(12,2)`.
# MAGIC * **Quarentena**: chave nula, preço ausente/≤ 0, frete inválido/negativo, ou item cujo pedido não existe em `silver.pedidos`.
# MAGIC * `valor_total_item = preco + frete`.

# COMMAND ----------

pedidos_ids = spark.table(f"{SILVER}.pedidos").select(F.col("order_id").alias("_ped"))

i = b("olist_order_items")
i = i.join(pedidos_ids, F.trim(i["order_id"]) == pedidos_ids["_ped"], "left")
i = separar(i, "olist_order_items", ["order_id", "order_item_id"], [
    ("order_id nulo", "order_id is null"),
    ("product_id nulo", "product_id is null"),
    ("preço ausente, inválido ou menor/igual a zero", "try_cast(price as decimal(12,2)) is null or try_cast(price as decimal(12,2)) <= 0"),
    ("frete ausente, inválido ou negativo", "try_cast(freight_value as decimal(12,2)) is null or try_cast(freight_value as decimal(12,2)) < 0"),
    ("pedido inexistente em silver.pedidos", "_ped is null"),
])

itens = i.select(
    F.trim("order_id").alias("order_id"),
    num("order_item_id", "int").alias("item_seq"),
    F.trim("product_id").alias("product_id"),
    F.trim("seller_id").alias("seller_id"),
    ts("shipping_limit_date").alias("dt_limite_postagem"),
    num("price", "decimal(12,2)").alias("preco"),
    num("freight_value", "decimal(12,2)").alias("frete"),
    F.col("_ingested_at").alias("ingerido_em"),
)
itens = dedup(itens, ["order_id", "item_seq"], [F.col("ingerido_em")])
itens = itens.withColumn("valor_total_item", F.col("preco") + F.col("frete"))
gravar(itens, "itens_pedido")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 `silver.pagamentos`
# MAGIC * `payment_installments` menor que 1 vira **1** e é sinalizado em `flag_parcelas_corrigida`.
# MAGIC * `not_defined` → `nao_definido`. Valor negativo ou pedido inexistente vão para a quarentena.
# MAGIC * Um pedido pode ter **vários** pagamentos (ex.: cartão + voucher); a consolidação por pedido acontece no Gold.

# COMMAND ----------

pg = b("olist_order_payments")
pg = pg.join(pedidos_ids, F.trim(pg["order_id"]) == pedidos_ids["_ped"], "left")
pg = separar(pg, "olist_order_payments", ["order_id", "payment_sequential"], [
    ("order_id nulo", "order_id is null"),
    ("valor pago ausente, inválido ou negativo", "try_cast(payment_value as decimal(12,2)) is null or try_cast(payment_value as decimal(12,2)) < 0"),
    ("pedido inexistente em silver.pedidos", "_ped is null"),
])

parcelas_raw = num("payment_installments", "int")
pagamentos = pg.select(
    F.trim("order_id").alias("order_id"),
    num("payment_sequential", "int").alias("pagamento_seq"),
    F.regexp_replace(F.lower(F.trim("payment_type")), "^not_defined$", "nao_definido").alias("tipo_pagamento"),
    F.when(parcelas_raw.isNull() | (parcelas_raw < 1), F.lit(1)).otherwise(parcelas_raw).alias("parcelas"),
    (parcelas_raw.isNull() | (parcelas_raw < 1)).alias("flag_parcelas_corrigida"),
    num("payment_value", "decimal(12,2)").alias("valor_pago"),
    F.col("_ingested_at").alias("ingerido_em"),
)
gravar(dedup(pagamentos, ["order_id", "pagamento_seq"], [F.col("ingerido_em")]), "pagamentos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.7 `silver.avaliacoes`
# MAGIC * Nota fora de 1–5 (ou não numérica) e avaliação de pedido inexistente vão para a quarentena.
# MAGIC * **Unicidade por pedido**: alguns pedidos têm mais de uma avaliação. Mantemos a **mais recente** (`dt_resposta` desc), resultando em 1 avaliação por `order_id`.
# MAGIC * Os campos de texto livre do comentário **não** são levados para a Silver: não são usados nas perguntas e podem conter dados pessoais.

# COMMAND ----------

rv = b("olist_order_reviews")
rv = rv.join(pedidos_ids, F.trim(rv["order_id"]) == pedidos_ids["_ped"], "left")
rv = separar(rv, "olist_order_reviews", ["review_id", "order_id"], [
    ("order_id nulo", "order_id is null"),
    ("nota ausente ou fora de 1-5", "try_cast(review_score as int) is null or try_cast(review_score as int) not between 1 and 5"),
    ("pedido inexistente em silver.pedidos", "_ped is null"),
])

avaliacoes = rv.select(
    F.trim("review_id").alias("review_id"),
    F.trim("order_id").alias("order_id"),
    num("review_score", "int").alias("nota"),
    ts("review_creation_date").alias("dt_criacao"),
    ts("review_answer_timestamp").alias("dt_resposta"),
    F.col("_ingested_at").alias("ingerido_em"),
)
avaliacoes = dedup(avaliacoes, ["order_id"], [F.col("dt_resposta").desc_nulls_last(), F.col("dt_criacao").desc_nulls_last(), F.col("review_id")])
gravar(avaliacoes, "avaliacoes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.8 `silver.quarentena_registros`
# MAGIC Todos os registros rejeitados acima, com a **tabela de origem**, a **chave**, o **motivo** e o **registro completo em JSON**.

# COMMAND ----------

quarentena = quarentena_partes[0]
for parte in quarentena_partes[1:]:
    quarentena = quarentena.unionByName(parte)
quarentena = quarentena.withColumn("quarentena_em", F.current_timestamp())
gravar(quarentena, "quarentena_registros")

print("Registros em quarentena por tabela e motivo (vazio = nenhum registro violou as regras duras):")
display(spark.table(f"{SILVER}.quarentena_registros").groupBy("tabela_origem", "motivo").count().orderBy("tabela_origem", F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.9 Balanço Bronze → Silver
# MAGIC Quantas linhas entraram, quantas foram para a quarentena e quantas foram removidas por deduplicação em cada tabela.

# COMMAND ----------

pares = [("olist_customers", "clientes"), ("olist_sellers", "vendedores"), ("olist_products", "produtos"), ("olist_orders", "pedidos"),
         ("olist_order_items", "itens_pedido"), ("olist_order_payments", "pagamentos"), ("olist_order_reviews", "avaliacoes")]

q_por_origem = {r["tabela_origem"]: r["n"] for r in spark.table(f"{SILVER}.quarentena_registros").groupBy("tabela_origem").agg(F.count("*").alias("n")).collect()}
balanco = []
for origem, destino in pares:
    n_b = spark.table(f"{BRONZE}.{origem}").count()
    n_s = spark.table(f"{SILVER}.{destino}").count()
    n_q = q_por_origem.get(origem, 0)
    balanco.append((origem, destino, n_b, n_q, n_b - n_q - n_s, n_s))

display(spark.createDataFrame(balanco, "bronze string, silver string, linhas_bronze long, em_quarentena long, removidas_por_dedup long, linhas_silver long"))

