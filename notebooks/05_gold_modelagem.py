# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Gold: modelo dimensional (Esquema Estrela)
# MAGIC
# MAGIC **Etapa 4.3 (Modelagem) e 4.4 (ETL 3 de 4)** — constrói o modelo que responde às perguntas de negócio.
# MAGIC
# MAGIC ```
# MAGIC                       dim_data                    dim_produto
# MAGIC                          │                            │
# MAGIC   dim_cliente ──── fato_pedido            fato_vendas_item ──── dim_vendedor
# MAGIC                          │                    │      │
# MAGIC                          └──── (order_id) ────┘      └── dim_data, dim_cliente, dim_produto
# MAGIC ```
# MAGIC
# MAGIC | Tabela | Tipo | Grão (1 linha =) |
# MAGIC |---|---|---|
# MAGIC | `fato_vendas_item` | Fato | um **item** vendido em um pedido |
# MAGIC | `fato_pedido` | Fato | um **pedido** (pagamento, entrega e avaliação consolidados) |
# MAGIC | `dim_data` | Dimensão | um **dia** (com datas comemorativas do varejo) |
# MAGIC | `dim_cliente` | Dimensão | um `customer_id` (cliente por pedido) |
# MAGIC | `dim_produto` | Dimensão | um produto, com segmento de moda |
# MAGIC | `dim_vendedor` | Dimensão | um vendedor |
# MAGIC | `agg_mensal_segmento` | Agregação | mês × segmento |
# MAGIC | `agg_uf_moda` | Agregação | UF (somente moda) |
# MAGIC
# MAGIC **Por que duas tabelas fato?** Pagamento, entrega e avaliação existem no nível do **pedido**; preço e frete existem no nível do **item**.
# MAGIC Colocar tudo no grão do item repetiria o valor pago e a nota em cada item e **inflaria somas**. Separar os grãos evita a contagem dupla.
# MAGIC
# MAGIC **Membro "Não informado" (`sk = -1`)**: toda dimensão tem uma linha para chaves sem correspondência, então nenhuma venda some por causa de um join.
# MAGIC
# MAGIC **`conta_receita`**: `true` quando o pedido não está `canceled` nem `unavailable`. É o filtro padrão das análises de receita.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import datetime as dt
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, BooleanType

def s(tabela):
    return spark.table(f"{SILVER}.{tabela}")


def gravar(df, nome):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.{nome}")
    print(f"{GOLD}.{nome:<22} {spark.table(f'{GOLD}.{nome}').count():>10,} linhas")


def com_membro_desconhecido(df, sk_col, valores):
    """Acrescenta a linha sk = -1 ('Não informado') com o mesmo schema da dimensão."""
    campos = []
    for f in df.schema.fields:
        if f.name == sk_col:
            campos.append(F.lit(-1).cast(f.dataType).alias(f.name))
        elif f.name in valores:
            campos.append(F.lit(valores[f.name]).cast(f.dataType).alias(f.name))
        else:
            campos.append(F.lit(None).cast(f.dataType).alias(f.name))
    return df.unionByName(spark.range(1).select(*campos))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 `gold.dim_data`
# MAGIC Calendário de 01/09/2016 a 31/12/2018 com atributos de tempo e **janelas de datas comemorativas do varejo brasileiro**
# MAGIC (Semana do Consumidor, Dia das Mães, Dia dos Namorados, Dia dos Pais, Dia das Crianças, Black Friday, Natal).
# MAGIC Essas janelas são a base da pergunta de sazonalidade e do planejamento de verba em mídia.

# COMMAND ----------

MESES = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
DIAS = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]   # weekday(): segunda = 0


def enesimo_dia_da_semana(ano, mes, dia_semana, n):
    """n-ésima ocorrência de um dia da semana (0 = segunda) em um mês."""
    d = dt.date(ano, mes, 1)
    d += dt.timedelta(days=(dia_semana - d.weekday()) % 7)
    return d + dt.timedelta(weeks=n - 1)


def janelas_comemorativas(ano):
    """(nome, data-âncora, dias_antes, dias_depois). A janela vai de âncora - dias_antes até âncora + dias_depois."""
    return [
        ("Semana do Consumidor", dt.date(ano, 3, 15), 3, 0),
        ("Dia das Mães", enesimo_dia_da_semana(ano, 5, 6, 2), 7, 0),        # 2º domingo de maio
        ("Dia dos Namorados", dt.date(ano, 6, 12), 7, 0),
        ("Dia dos Pais", enesimo_dia_da_semana(ano, 8, 6, 2), 7, 0),        # 2º domingo de agosto
        ("Dia das Crianças", dt.date(ano, 10, 12), 7, 0),
        ("Black Friday", enesimo_dia_da_semana(ano, 11, 4, 4), 3, 3),       # 4ª sexta de novembro
        ("Natal", dt.date(ano, 12, 25), 14, 0),
    ]


evento_por_data = {}
for ano in range(2016, 2019):
    for nome, ancora, antes, depois in janelas_comemorativas(ano):
        for delta in range(-antes, depois + 1):
            evento_por_data[ancora + dt.timedelta(days=delta)] = nome

linhas, d, fim = [], dt.date(2016, 9, 1), dt.date(2018, 12, 31)
while d <= fim:
    linhas.append((
        int(d.strftime("%Y%m%d")), d, d.year, d.month, MESES[d.month - 1], d.strftime("%Y-%m"), (d.month - 1) // 3 + 1,
        d.isoweekday(), DIAS[d.weekday()], d.isocalendar()[1], d.weekday() >= 5, evento_por_data.get(d, "Sem evento"),
    ))
    d += dt.timedelta(days=1)

schema_data = StructType([
    StructField("sk_data", IntegerType(), False), StructField("data", DateType(), False), StructField("ano", IntegerType()),
    StructField("mes", IntegerType()), StructField("nome_mes", StringType()), StructField("ano_mes", StringType()),
    StructField("trimestre", IntegerType()), StructField("dia_semana_num", IntegerType()), StructField("dia_semana", StringType()),
    StructField("semana_iso", IntegerType()), StructField("fim_de_semana", BooleanType()), StructField("evento_comercial", StringType()),
])
gravar(spark.createDataFrame(linhas, schema_data), "dim_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Dimensões `dim_cliente`, `dim_vendedor` e `dim_produto`
# MAGIC Chave substituta (`sk_*`) gerada por `row_number()` sobre a chave natural, mais o membro `-1` "Não informado".
# MAGIC Na `dim_produto`, o segmento de moda vira o texto `Fora de moda` quando o produto não é de moda, para facilitar comparações com o resto do marketplace.

# COMMAND ----------

def com_sk(df, chave, nome_sk):
    return df.withColumn(nome_sk, F.row_number().over(Window.orderBy(chave))).select(nome_sk, *[c for c in df.columns])


dim_cliente = com_sk(s("clientes").select("customer_id", "cliente_unico_id", "cep_prefixo", "cidade", "uf", "regiao"), "customer_id", "sk_cliente")
dim_cliente = com_membro_desconhecido(dim_cliente, "sk_cliente", {"customer_id": "NAO_INFORMADO", "cliente_unico_id": "NAO_INFORMADO", "uf": "NI", "regiao": "Não informada", "cidade": "Não informada"})
gravar(dim_cliente, "dim_cliente")

dim_vendedor = com_sk(s("vendedores").select("seller_id", "cep_prefixo", "cidade", "uf"), "seller_id", "sk_vendedor")
dim_vendedor = com_membro_desconhecido(dim_vendedor, "sk_vendedor", {"seller_id": "NAO_INFORMADO", "uf": "NI", "cidade": "Não informada"})
gravar(dim_vendedor, "dim_vendedor")

dim_produto = s("produtos").select(
    "product_id", "categoria_pt", "categoria_en",
    F.coalesce("segmento_moda", F.lit("Fora de moda")).alias("segmento"),
    "eh_moda", "fotos_qtd", "peso_g",
    (F.col("comprimento_cm") * F.col("altura_cm") * F.col("largura_cm")).alias("volume_cm3"),
)
dim_produto = com_sk(dim_produto, "product_id", "sk_produto")
dim_produto = com_membro_desconhecido(dim_produto, "sk_produto", {"product_id": "NAO_INFORMADO", "categoria_pt": "sem_categoria", "categoria_en": "sem_categoria", "segmento": "Fora de moda", "eh_moda": False})
gravar(dim_produto, "dim_produto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 `gold.fato_vendas_item`  (grão: 1 item de pedido)
# MAGIC **JOIN** `itens_pedido` × `pedidos` (data, status) e *lookups* das chaves substitutas nas três dimensões (`left join` + `coalesce(sk, -1)`).
# MAGIC `frete_pct_preco` = frete ÷ preço do item, a "fricção de frete" da pergunta 4.

# COMMAND ----------

it = s("itens_pedido").alias("i")
pd_ = s("pedidos").select("order_id", "customer_id", "status", "dt_compra")

dc = spark.table(f"{GOLD}.dim_cliente").select("sk_cliente", "customer_id")
dp = spark.table(f"{GOLD}.dim_produto").select("sk_produto", "product_id")
dv = spark.table(f"{GOLD}.dim_vendedor").select("sk_vendedor", "seller_id")

fato_item = (it.join(pd_, "order_id", "inner")
    .join(dc, "customer_id", "left")
    .join(dp, "product_id", "left")
    .join(dv, "seller_id", "left")
    .select(
        F.col("order_id"), F.col("item_seq"),
        F.date_format("dt_compra", "yyyyMMdd").cast("int").alias("sk_data_compra"),
        F.coalesce("sk_cliente", F.lit(-1)).alias("sk_cliente"),
        F.coalesce("sk_produto", F.lit(-1)).alias("sk_produto"),
        F.coalesce("sk_vendedor", F.lit(-1)).alias("sk_vendedor"),
        F.col("status").alias("status_pedido"),
        (~F.col("status").isin("canceled", "unavailable")).alias("conta_receita"),
        F.col("preco"), F.col("frete"), F.col("valor_total_item"),
        F.round(F.col("frete") / F.col("preco"), 4).cast("double").alias("frete_pct_preco"),
    ))
gravar(fato_item, "fato_vendas_item")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 `gold.fato_pedido`  (grão: 1 pedido)
# MAGIC Consolida, por pedido:
# MAGIC * **itens** → quantidade, receita, frete e o que é de **moda** (`GROUP BY order_id` sobre `itens_pedido` + `produtos`);
# MAGIC * **pagamentos** → valor total, maior nº de parcelas e o **meio de pagamento principal** (o de maior valor);
# MAGIC * **avaliação** → nota (1 por pedido) e **entrega** → prazo e atraso.

# COMMAND ----------

itens_prod = (s("itens_pedido").join(s("produtos").select("product_id", "eh_moda"), "product_id", "left")
              .withColumn("eh_moda", F.coalesce("eh_moda", F.lit(False))))

ag_itens = itens_prod.groupBy("order_id").agg(
    F.count("*").alias("qtd_itens"),
    F.sum(F.when(F.col("eh_moda"), 1).otherwise(0)).alias("qtd_itens_moda"),
    F.sum("preco").alias("receita_itens"),
    F.sum(F.when(F.col("eh_moda"), F.col("preco")).otherwise(F.lit(0).cast("decimal(12,2)"))).alias("receita_moda"),
    F.sum("frete").alias("frete_total"),
)

pg = s("pagamentos")
w_principal = Window.partitionBy("order_id").orderBy(F.col("valor_pago").desc(), F.col("pagamento_seq"))
principal = (pg.withColumn("_rn", F.row_number().over(w_principal)).where("_rn = 1")
             .select("order_id", F.col("tipo_pagamento").alias("tipo_pagamento_principal")))
ag_pag = pg.groupBy("order_id").agg(
    F.sum("valor_pago").alias("valor_pago_total"),
    F.max("parcelas").alias("parcelas_max"),
    F.count("*").alias("qtd_meios_pagamento"),
).join(principal, "order_id", "left")

fato_pedido = (s("pedidos")
    .join(dc, "customer_id", "left")
    .join(ag_itens, "order_id", "left")
    .join(ag_pag, "order_id", "left")
    .join(s("avaliacoes").select("order_id", F.col("nota").alias("nota_avaliacao")), "order_id", "left")
    .select(
        "order_id",
        F.date_format("dt_compra", "yyyyMMdd").cast("int").alias("sk_data_compra"),
        F.coalesce("sk_cliente", F.lit(-1)).alias("sk_cliente"),
        F.col("status").alias("status_pedido"),
        (~F.col("status").isin("canceled", "unavailable")).alias("conta_receita"),
        F.coalesce("qtd_itens", F.lit(0)).alias("qtd_itens"),
        F.coalesce("qtd_itens_moda", F.lit(0)).alias("qtd_itens_moda"),
        (F.coalesce("qtd_itens_moda", F.lit(0)) > 0).alias("eh_pedido_moda"),
        F.col("receita_itens"), F.col("receita_moda"), F.col("frete_total"),
        F.col("valor_pago_total"), F.col("parcelas_max"), F.col("qtd_meios_pagamento"), F.col("tipo_pagamento_principal"),
        F.col("nota_avaliacao"),
        F.col("dias_ate_entrega"), F.col("atraso_dias"), F.col("entregue_no_prazo"),
    ))
gravar(fato_pedido, "fato_pedido")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5 Agregações prontas para consumo
# MAGIC Métricas pré-calculadas (camada de consumo), alimentam dashboards sem repetir joins:
# MAGIC * `agg_mensal_segmento`: pedidos, itens, receita, frete e preço por **mês × segmento** (moda e "Fora de moda");
# MAGIC * `agg_uf_moda`: mesmas métricas por **UF**, só moda, incluindo o **peso do frete** na receita.

# COMMAND ----------

fi = spark.table(f"{GOLD}.fato_vendas_item").where("conta_receita")
dprod = spark.table(f"{GOLD}.dim_produto").select("sk_produto", "segmento", "eh_moda")
ddata = spark.table(f"{GOLD}.dim_data").select("sk_data", "ano", "mes", "ano_mes")
dcli = spark.table(f"{GOLD}.dim_cliente").select("sk_cliente", "cliente_unico_id", "uf", "regiao")

base = (fi.join(dprod, "sk_produto").join(ddata, fi["sk_data_compra"] == ddata["sk_data"]).join(dcli, "sk_cliente"))

agg_mensal = base.groupBy("ano_mes", "ano", "mes", "segmento", "eh_moda").agg(
    F.countDistinct("order_id").alias("pedidos"),
    F.count("*").alias("itens"),
    F.sum("preco").alias("receita"),
    F.sum("frete").alias("frete_total"),
    F.round(F.avg("preco"), 2).cast("decimal(12,2)").alias("preco_medio_item"),
    F.round(F.expr("percentile_approx(preco, 0.5)"), 2).cast("decimal(12,2)").alias("preco_mediano_item"),
)
gravar(agg_mensal, "agg_mensal_segmento")

agg_uf = base.where("eh_moda").groupBy("uf", "regiao").agg(
    F.countDistinct("order_id").alias("pedidos"),
    F.countDistinct("cliente_unico_id").alias("clientes_unicos"),
    F.count("*").alias("itens"),
    F.sum("preco").alias("receita"),
    F.sum("frete").alias("frete_total"),
).withColumn("receita_por_pedido", F.round(F.col("receita") / F.col("pedidos"), 2).cast("decimal(14,2)")
).withColumn("frete_pct_da_receita", F.round(F.col("frete_total") / F.col("receita"), 4).cast("double"))
gravar(agg_uf, "agg_uf_moda")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.6 Chaves primárias e estrangeiras (informativas)
# MAGIC No Unity Catalog, `PRIMARY KEY` e `FOREIGN KEY` são **informativas**: não bloqueiam gravações, mas documentam o modelo e permitem visualizar
# MAGIC o **diagrama de relacionamentos** no Catalog Explorer (aba *Relationships*), evidência para a etapa de modelagem.
# MAGIC Fora do Databricks (teste local), esta célula é ignorada.

# COMMAND ----------

def tentar(sql):
    try:
        spark.sql(sql)
    except Exception as e:
        print("  (ignorado)", str(e).split("\n")[0][:140])


if IS_DATABRICKS:
    pks = {
        "dim_data": ["sk_data"], "dim_cliente": ["sk_cliente"], "dim_produto": ["sk_produto"], "dim_vendedor": ["sk_vendedor"],
        "fato_vendas_item": ["order_id", "item_seq"], "fato_pedido": ["order_id"],
    }
    for tabela, cols in pks.items():
        for c in cols:
            tentar(f"ALTER TABLE {GOLD}.{tabela} ALTER COLUMN {c} SET NOT NULL")
        tentar(f"ALTER TABLE {GOLD}.{tabela} ADD CONSTRAINT pk_{tabela} PRIMARY KEY ({', '.join(cols)})")

    fks = [
        ("fato_vendas_item", "sk_data_compra", "dim_data", "sk_data"), ("fato_vendas_item", "sk_cliente", "dim_cliente", "sk_cliente"),
        ("fato_vendas_item", "sk_produto", "dim_produto", "sk_produto"), ("fato_vendas_item", "sk_vendedor", "dim_vendedor", "sk_vendedor"),
        ("fato_pedido", "sk_data_compra", "dim_data", "sk_data"), ("fato_pedido", "sk_cliente", "dim_cliente", "sk_cliente"),
    ]
    for tabela, col, ref, ref_col in fks:
        tentar(f"ALTER TABLE {GOLD}.{tabela} ADD CONSTRAINT fk_{tabela}_{col} FOREIGN KEY ({col}) REFERENCES {GOLD}.{ref}({ref_col})")
else:
    print("Execução local: constraints informativas ignoradas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.7 Checagem de reconciliação (a Gold bate com a Silver?)
# MAGIC Duas somas que devem ser **idênticas**: receita total dos itens na Silver e na `fato_vendas_item`; e número de pedidos entre `pedidos` e `fato_pedido`.

# COMMAND ----------

conf = spark.sql(f"""
SELECT 'receita itens: silver vs gold' AS checagem,
       (SELECT CAST(SUM(preco) AS DECIMAL(18,2)) FROM {SILVER}.itens_pedido) AS silver,
       (SELECT CAST(SUM(preco) AS DECIMAL(18,2)) FROM {GOLD}.fato_vendas_item) AS gold
UNION ALL
SELECT 'pedidos: silver vs gold',
       (SELECT COUNT(*) FROM {SILVER}.pedidos),
       (SELECT COUNT(*) FROM {GOLD}.fato_pedido)
""").withColumn("ok", F.col("silver") == F.col("gold"))
display(conf)
assert conf.where("not ok").count() == 0, "Reconciliação falhou: Gold diverge da Silver."

