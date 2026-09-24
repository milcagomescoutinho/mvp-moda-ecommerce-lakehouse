# Databricks notebook source
# MAGIC %md
# MAGIC # Metadados do catálogo de dados
# MAGIC
# MAGIC Fonte única da documentação de **todas as tabelas e colunas** do projeto (Bronze, Silver e Gold).
# MAGIC É usada por dois consumidores:
# MAGIC * `07_catalogo_de_dados.py` grava estas descrições no **Unity Catalog** (`COMMENT ON TABLE` / `ALTER COLUMN ... COMMENT`);
# MAGIC * `docs/gerar_catalogo_md.py` gera o catálogo em Markdown que vai no README.
# MAGIC
# MAGIC Formato de cada coluna: `(nome, tipo, descrição, domínio de valores, linhagem)`.

# COMMAND ----------

def _bronze(arquivo, colunas):
    """Colunas Bronze: tudo string, cópia fiel do CSV. `colunas` = [(nome, descrição, domínio)]."""
    cols = [(n, "string", d, dom, f"CSV `{arquivo}`, coluna `{n}` (sem transformação)") for n, d, dom in colunas]
    cols.append(("_ingested_at", "timestamp", "Momento em que a linha foi carregada na Bronze.", "Timestamp da execução da carga", "Gerado em `02_bronze_ingestao` (`current_timestamp()`)"))
    cols.append(("_source_file", "string", "Nome do arquivo CSV de origem.", f"Sempre `{arquivo}`", "Gerado em `02_bronze_ingestao` (literal)"))
    return cols


CATALOGO = {}

# ======================================================================================================
# BRONZE
# ======================================================================================================
CATALOGO["bronze.olist_customers"] = {
    "camada": "bronze",
    "descricao": "Clientes do marketplace Olist, cópia fiel do CSV. Um `customer_id` por pedido; o mesmo consumidor real pode ter vários `customer_id` (identificado por `customer_unique_id`).",
    "colunas": _bronze("olist_customers_dataset.csv", [
        ("customer_id", "Identificador do cliente **por pedido**.", "Hash hexadecimal de 32 caracteres"),
        ("customer_unique_id", "Identificador do consumidor real (reaparece em vários pedidos).", "Hash hexadecimal de 32 caracteres"),
        ("customer_zip_code_prefix", "Cinco primeiros dígitos do CEP; o zero à esquerda pode ter sido perdido.", "Texto numérico de 4 ou 5 dígitos"),
        ("customer_city", "Cidade do cliente (minúsculas, sem acentos).", "Texto livre"),
        ("customer_state", "Sigla da UF do cliente.", "27 UFs brasileiras"),
    ]),
}
CATALOGO["bronze.olist_orders"] = {
    "camada": "bronze",
    "descricao": "Pedidos do marketplace com status e marcos de data (compra, aprovação, postagem, entrega). Um registro por pedido.",
    "colunas": _bronze("olist_orders_dataset.csv", [
        ("order_id", "Identificador único do pedido.", "Hash hexadecimal de 32 caracteres"),
        ("customer_id", "Cliente do pedido (chave para `olist_customers`).", "Hash hexadecimal de 32 caracteres"),
        ("order_status", "Situação do pedido.", "delivered, shipped, canceled, unavailable, invoiced, processing, created, approved"),
        ("order_purchase_timestamp", "Data e hora da compra.", "Texto `yyyy-MM-dd HH:mm:ss`"),
        ("order_approved_at", "Data e hora da aprovação do pagamento.", "Texto `yyyy-MM-dd HH:mm:ss`; pode ser nulo"),
        ("order_delivered_carrier_date", "Data em que o pedido foi entregue à transportadora.", "Texto `yyyy-MM-dd HH:mm:ss`; pode ser nulo"),
        ("order_delivered_customer_date", "Data em que o cliente recebeu o pedido.", "Texto `yyyy-MM-dd HH:mm:ss`; nulo se não entregue"),
        ("order_estimated_delivery_date", "Data de entrega prometida ao cliente.", "Texto `yyyy-MM-dd HH:mm:ss`"),
    ]),
}
CATALOGO["bronze.olist_order_items"] = {
    "camada": "bronze",
    "descricao": "Itens vendidos em cada pedido. Um registro por item; um pedido pode ter vários itens.",
    "colunas": _bronze("olist_order_items_dataset.csv", [
        ("order_id", "Pedido ao qual o item pertence.", "Hash hexadecimal de 32 caracteres"),
        ("order_item_id", "Sequência do item dentro do pedido (1, 2, 3...).", "Inteiro positivo em texto"),
        ("product_id", "Produto vendido (chave para `olist_products`).", "Hash hexadecimal de 32 caracteres"),
        ("seller_id", "Vendedor (chave para `olist_sellers`).", "Hash hexadecimal de 32 caracteres"),
        ("shipping_limit_date", "Data limite para o vendedor postar o item.", "Texto `yyyy-MM-dd HH:mm:ss`"),
        ("price", "Preço do item em reais, sem frete.", "Decimal > 0 em texto"),
        ("freight_value", "Valor do frete do item em reais.", "Decimal >= 0 em texto"),
    ]),
}
CATALOGO["bronze.olist_order_payments"] = {
    "camada": "bronze",
    "descricao": "Pagamentos dos pedidos. Um pedido pode ter vários pagamentos (ex.: cartão e voucher).",
    "colunas": _bronze("olist_order_payments_dataset.csv", [
        ("order_id", "Pedido pago.", "Hash hexadecimal de 32 caracteres"),
        ("payment_sequential", "Sequência do pagamento dentro do pedido.", "Inteiro positivo em texto"),
        ("payment_type", "Meio de pagamento.", "credit_card, boleto, voucher, debit_card, not_defined"),
        ("payment_installments", "Número de parcelas escolhido.", "Inteiro em texto (esperado >= 1)"),
        ("payment_value", "Valor pago neste pagamento, em reais.", "Decimal >= 0 em texto"),
    ]),
}
CATALOGO["bronze.olist_order_reviews"] = {
    "camada": "bronze",
    "descricao": "Avaliações dos clientes após a compra. Alguns pedidos têm mais de uma avaliação e o mesmo `review_id` pode se repetir.",
    "colunas": _bronze("olist_order_reviews_dataset.csv", [
        ("review_id", "Identificador da avaliação.", "Hash hexadecimal de 32 caracteres"),
        ("order_id", "Pedido avaliado.", "Hash hexadecimal de 32 caracteres"),
        ("review_score", "Nota dada pelo cliente.", "1 a 5 em texto"),
        ("review_comment_title", "Título do comentário (texto livre, muitas vezes vazio).", "Texto livre; não usado no projeto"),
        ("review_comment_message", "Corpo do comentário (texto livre, pode ter quebras de linha).", "Texto livre; não usado no projeto"),
        ("review_creation_date", "Data em que a pesquisa de satisfação foi enviada.", "Texto `yyyy-MM-dd HH:mm:ss`"),
        ("review_answer_timestamp", "Data e hora em que o cliente respondeu.", "Texto `yyyy-MM-dd HH:mm:ss`"),
    ]),
}
CATALOGO["bronze.olist_products"] = {
    "camada": "bronze",
    "descricao": "Catálogo de produtos com categoria e atributos físicos. Os nomes `..._lenght` mantêm o erro de digitação da fonte.",
    "colunas": _bronze("olist_products_dataset.csv", [
        ("product_id", "Identificador único do produto.", "Hash hexadecimal de 32 caracteres"),
        ("product_category_name", "Categoria do produto em português.", "Cerca de 70 categorias; nulo em alguns produtos"),
        ("product_name_lenght", "Quantidade de caracteres do nome do produto.", "Inteiro em texto"),
        ("product_description_lenght", "Quantidade de caracteres da descrição.", "Inteiro em texto"),
        ("product_photos_qty", "Número de fotos do anúncio.", "Inteiro em texto"),
        ("product_weight_g", "Peso em gramas.", "Decimal em texto"),
        ("product_length_cm", "Comprimento da embalagem em cm.", "Decimal em texto"),
        ("product_height_cm", "Altura da embalagem em cm.", "Decimal em texto"),
        ("product_width_cm", "Largura da embalagem em cm.", "Decimal em texto"),
    ]),
}
CATALOGO["bronze.olist_sellers"] = {
    "camada": "bronze",
    "descricao": "Vendedores (lojistas) do marketplace.",
    "colunas": _bronze("olist_sellers_dataset.csv", [
        ("seller_id", "Identificador único do vendedor.", "Hash hexadecimal de 32 caracteres"),
        ("seller_zip_code_prefix", "Cinco primeiros dígitos do CEP do vendedor.", "Texto numérico de 4 ou 5 dígitos"),
        ("seller_city", "Cidade do vendedor.", "Texto livre"),
        ("seller_state", "Sigla da UF do vendedor.", "27 UFs brasileiras"),
    ]),
}
CATALOGO["bronze.category_translation"] = {
    "camada": "bronze",
    "descricao": "Tabela de tradução das categorias de produto do português para o inglês.",
    "colunas": _bronze("product_category_name_translation.csv", [
        ("product_category_name", "Categoria em português (chave).", "Texto em snake_case"),
        ("product_category_name_english", "Categoria em inglês.", "Texto em snake_case"),
    ]),
}

# ======================================================================================================
# SILVER
# ======================================================================================================
_ING = ("ingerido_em", "timestamp", "Momento da carga do registro na Bronze (rastreabilidade).", "Timestamp", "`_ingested_at` da tabela Bronze correspondente")

CATALOGO["silver.clientes"] = {
    "camada": "silver",
    "descricao": "Clientes limpos e padronizados. Um registro por `customer_id`, com UF validada, CEP com 5 dígitos e região derivada.",
    "colunas": [
        ("customer_id", "string", "Identificador do cliente por pedido (chave).", "Hash de 32 caracteres, único", "`bronze.olist_customers.customer_id` (trim)"),
        ("cliente_unico_id", "string", "Identificador do consumidor real; permite medir recompra.", "Hash de 32 caracteres", "`bronze.olist_customers.customer_unique_id` (trim)"),
        ("cep_prefixo", "string", "Cinco primeiros dígitos do CEP.", "5 dígitos numéricos", "`customer_zip_code_prefix` com `lpad(5, '0')`"),
        ("cidade", "string", "Cidade do cliente com caixa padronizada.", "Texto livre", "`customer_city` com trim + initcap"),
        ("uf", "string", "Sigla da UF.", "27 UFs ou `NI` (não informado)", "`customer_state` em maiúsculas; inválida vira `NI`"),
        _ING,
        ("regiao", "string", "Região geográfica do Brasil.", "Norte, Nordeste, Centro-Oeste, Sudeste, Sul, Não informada", "Derivada de `uf` pelo dicionário `REGIOES` (00_config)"),
    ],
}
CATALOGO["silver.vendedores"] = {
    "camada": "silver",
    "descricao": "Vendedores limpos e padronizados. Um registro por `seller_id`.",
    "colunas": [
        ("seller_id", "string", "Identificador do vendedor (chave).", "Hash de 32 caracteres, único", "`bronze.olist_sellers.seller_id` (trim)"),
        ("cep_prefixo", "string", "Cinco primeiros dígitos do CEP.", "5 dígitos numéricos", "`seller_zip_code_prefix` com `lpad(5, '0')`"),
        ("cidade", "string", "Cidade do vendedor.", "Texto livre", "`seller_city` com trim + initcap"),
        ("uf", "string", "Sigla da UF.", "27 UFs ou `NI`", "`seller_state` em maiúsculas; inválida vira `NI`"),
        _ING,
    ],
}
CATALOGO["silver.produtos"] = {
    "camada": "silver",
    "descricao": "Produtos com categoria traduzida, atributos físicos numéricos e classificação de segmento de moda.",
    "colunas": [
        ("product_id", "string", "Identificador do produto (chave).", "Hash de 32 caracteres, único", "`bronze.olist_products.product_id` (trim)"),
        ("categoria_pt", "string", "Categoria em português.", "Categorias do dataset ou `sem_categoria`", "`product_category_name`; nulo/vazio vira `sem_categoria`"),
        ("categoria_en", "string", "Categoria em inglês.", "Tradução ou, na falta dela, o nome em português", "JOIN com `bronze.category_translation` por `product_category_name`; `coalesce` com `categoria_pt`"),
        ("segmento_moda", "string", "Segmento de moda do produto; nulo se não for moda.", "Vestuário feminino/masculino/infantojuvenil, Calçados, Bolsas e acessórios, Moda íntima e praia, Moda esportiva, Malas e acessórios de viagem, Moda - outros, nulo", "Mapeamento `SEGMENTOS_MODA` (00_config) sobre `categoria_pt`; `fashion_*` fora do mapa = `Moda - outros`"),
        ("eh_moda", "boolean", "Verdadeiro se o produto pertence a um segmento de moda.", "true / false", "`segmento_moda IS NOT NULL`"),
        ("nome_qtd_caracteres", "int", "Caracteres do nome do produto.", "Inteiro >= 0 ou nulo", "`product_name_lenght` (try_cast, typo corrigido no nome)"),
        ("descricao_qtd_caracteres", "int", "Caracteres da descrição do produto.", "Inteiro >= 0 ou nulo", "`product_description_lenght` (try_cast, typo corrigido no nome)"),
        ("fotos_qtd", "int", "Número de fotos do anúncio.", "Inteiro >= 0 ou nulo", "`product_photos_qty` (try_cast)"),
        ("peso_g", "double", "Peso em gramas.", "> 0 ou nulo", "`product_weight_g`; valores <= 0 viram nulo"),
        ("comprimento_cm", "double", "Comprimento da embalagem em cm.", "> 0 ou nulo", "`product_length_cm`; valores <= 0 viram nulo"),
        ("altura_cm", "double", "Altura da embalagem em cm.", "> 0 ou nulo", "`product_height_cm`; valores <= 0 viram nulo"),
        ("largura_cm", "double", "Largura da embalagem em cm.", "> 0 ou nulo", "`product_width_cm`; valores <= 0 viram nulo"),
        _ING,
    ],
}
CATALOGO["silver.pedidos"] = {
    "camada": "silver",
    "descricao": "Pedidos com datas convertidas para timestamp e métricas de prazo de entrega. Um registro por `order_id`.",
    "colunas": [
        ("order_id", "string", "Identificador do pedido (chave).", "Hash de 32 caracteres, único", "`bronze.olist_orders.order_id` (trim)"),
        ("customer_id", "string", "Cliente do pedido.", "Hash de 32 caracteres", "`bronze.olist_orders.customer_id` (trim)"),
        ("status", "string", "Situação do pedido em minúsculas.", "delivered, shipped, canceled, unavailable, invoiced, processing, created, approved", "`order_status` (trim + lower)"),
        ("dt_compra", "timestamp", "Data e hora da compra.", "Entre set/2016 e out/2018", "`order_purchase_timestamp` (try_to_timestamp)"),
        ("dt_aprovacao", "timestamp", "Data e hora da aprovação.", "Timestamp ou nulo", "`order_approved_at` (try_to_timestamp)"),
        ("dt_postagem", "timestamp", "Data de entrega à transportadora.", "Timestamp ou nulo", "`order_delivered_carrier_date` (try_to_timestamp)"),
        ("dt_entrega", "timestamp", "Data de entrega ao cliente.", "Timestamp ou nulo", "`order_delivered_customer_date` (try_to_timestamp)"),
        ("dt_entrega_estimada", "timestamp", "Data de entrega prometida.", "Timestamp ou nulo", "`order_estimated_delivery_date` (try_to_timestamp)"),
        _ING,
        ("flag_datas_inconsistentes", "boolean", "Verdadeiro se entrega ou postagem for anterior à compra.", "true / false", "`dt_entrega < dt_compra OR dt_postagem < dt_compra`"),
        ("dias_ate_entrega", "int", "Dias entre a compra e a entrega ao cliente (só pedidos `delivered` coerentes).", "Inteiro >= 0 ou nulo", "`datediff(dt_entrega, dt_compra)`"),
        ("atraso_dias", "int", "Dias de atraso em relação ao prometido; positivo = atrasou, negativo = adiantou.", "Inteiro ou nulo", "`datediff(dt_entrega, dt_entrega_estimada)`"),
        ("entregue_no_prazo", "boolean", "Verdadeiro se entregue até a data prometida.", "true / false / nulo", "`atraso_dias <= 0`"),
    ],
}
CATALOGO["silver.itens_pedido"] = {
    "camada": "silver",
    "descricao": "Itens de pedido validados. Um registro por (`order_id`, `item_seq`). Exclui itens em quarentena (preço inválido, pedido inexistente).",
    "colunas": [
        ("order_id", "string", "Pedido do item (chave composta).", "Existe em `silver.pedidos`", "`bronze.olist_order_items.order_id` (trim)"),
        ("item_seq", "int", "Sequência do item no pedido (chave composta).", "Inteiro >= 1", "`order_item_id` (try_cast int)"),
        ("product_id", "string", "Produto vendido.", "Hash de 32 caracteres", "`bronze.olist_order_items.product_id` (trim)"),
        ("seller_id", "string", "Vendedor do item.", "Hash de 32 caracteres", "`bronze.olist_order_items.seller_id` (trim)"),
        ("dt_limite_postagem", "timestamp", "Prazo do vendedor para postar.", "Timestamp ou nulo", "`shipping_limit_date` (try_to_timestamp)"),
        ("preco", "decimal(12,2)", "Preço do item em R$, sem frete.", "> 0", "`price` (try_cast decimal(12,2))"),
        ("frete", "decimal(12,2)", "Frete do item em R$.", ">= 0", "`freight_value` (try_cast decimal(12,2))"),
        _ING,
        ("valor_total_item", "decimal(13,2)", "Preço + frete do item.", "> 0", "`preco + frete`"),
    ],
}
CATALOGO["silver.pagamentos"] = {
    "camada": "silver",
    "descricao": "Pagamentos validados. Um registro por (`order_id`, `pagamento_seq`). Parcelas menores que 1 foram corrigidas para 1.",
    "colunas": [
        ("order_id", "string", "Pedido pago (chave composta).", "Existe em `silver.pedidos`", "`bronze.olist_order_payments.order_id` (trim)"),
        ("pagamento_seq", "int", "Sequência do pagamento no pedido (chave composta).", "Inteiro >= 1", "`payment_sequential` (try_cast int)"),
        ("tipo_pagamento", "string", "Meio de pagamento.", "credit_card, boleto, voucher, debit_card, nao_definido", "`payment_type` (lower); `not_defined` vira `nao_definido`"),
        ("parcelas", "int", "Número de parcelas.", "Inteiro >= 1", "`payment_installments`; < 1 ou nulo vira 1"),
        ("flag_parcelas_corrigida", "boolean", "Verdadeiro se as parcelas originais eram inválidas e foram ajustadas para 1.", "true / false", "`payment_installments IS NULL OR < 1`"),
        ("valor_pago", "decimal(12,2)", "Valor deste pagamento em R$.", ">= 0", "`payment_value` (try_cast decimal(12,2))"),
        _ING,
    ],
}
CATALOGO["silver.avaliacoes"] = {
    "camada": "silver",
    "descricao": "Avaliações válidas, com **uma por pedido** (a mais recente). Comentários em texto livre não são levados para a Silver.",
    "colunas": [
        ("review_id", "string", "Identificador da avaliação (pode se repetir entre pedidos na fonte).", "Hash de 32 caracteres", "`bronze.olist_order_reviews.review_id` (trim)"),
        ("order_id", "string", "Pedido avaliado (chave).", "Existe em `silver.pedidos`, único", "`bronze.olist_order_reviews.order_id` (trim)"),
        ("nota", "int", "Nota de satisfação.", "1 a 5", "`review_score` (try_cast int)"),
        ("dt_criacao", "timestamp", "Data de envio da pesquisa.", "Timestamp ou nulo", "`review_creation_date` (try_to_timestamp)"),
        ("dt_resposta", "timestamp", "Data e hora da resposta.", "Timestamp ou nulo", "`review_answer_timestamp` (try_to_timestamp)"),
        _ING,
    ],
}
CATALOGO["silver.quarentena_registros"] = {
    "camada": "silver",
    "descricao": "Registros rejeitados pelas regras duras da Silver, com origem, chave, motivo e o registro completo em JSON. Vazia se nenhum registro violou as regras.",
    "colunas": [
        ("tabela_origem", "string", "Tabela Bronze de onde o registro veio.", "Nome de tabela Bronze", "Literal definido em `04_silver_limpeza`"),
        ("chave", "string", "Chave de negócio do registro, separada por `|`.", "Texto", "Concatenação das colunas-chave da Bronze"),
        ("motivo", "string", "Regra violada.", "Ex.: preço ausente/<= 0, pedido inexistente, nota fora de 1-5", "Primeira regra violada em `separar()`"),
        ("registro_json", "string", "Registro completo serializado em JSON.", "JSON", "`to_json(struct(*))` das colunas Bronze"),
        ("quarentena_em", "timestamp", "Momento em que o registro foi para a quarentena.", "Timestamp", "`current_timestamp()`"),
    ],
}
CATALOGO["silver.dq_resultados"] = {
    "camada": "silver",
    "descricao": "Resultado de cada regra de qualidade de dados executada sobre a Bronze, com a proporção de problemas e o tratamento aplicado na Silver.",
    "colunas": [
        ("tabela", "string", "Tabela Bronze verificada.", "Nome de tabela Bronze", "`03_qualidade_de_dados`"),
        ("coluna", "string", "Coluna (ou chave composta) verificada.", "Texto", "`03_qualidade_de_dados`"),
        ("dimensao", "string", "Dimensão de qualidade.", "Completude, Consistência, Unicidade, Acurácia, Outliers", "`03_qualidade_de_dados`"),
        ("regra", "string", "Descrição da regra verificada.", "Texto", "`03_qualidade_de_dados`"),
        ("total_registros", "bigint", "Registros avaliados.", ">= 0", "`count(*)` na Bronze"),
        ("registros_com_problema", "bigint", "Registros que violam a regra.", ">= 0 e <= total", "Soma condicional na Bronze"),
        ("pct_problema", "double", "Percentual de registros com problema.", "0 a 100", "`registros_com_problema / total_registros * 100`"),
        ("tratamento", "string", "Tratamento aplicado na Silver.", "Texto", "Documentado em `03_qualidade_de_dados` e implementado em `04_silver_limpeza`"),
        ("verificado_em", "timestamp", "Momento da verificação.", "Timestamp", "`current_timestamp()`"),
    ],
}
CATALOGO["silver.dq_completude"] = {
    "camada": "silver",
    "descricao": "Proporção de valores nulos ou vazios em cada coluna de cada tabela Bronze.",
    "colunas": [
        ("tabela", "string", "Tabela Bronze.", "Nome de tabela Bronze", "`03_qualidade_de_dados`"),
        ("coluna", "string", "Coluna verificada.", "Nome de coluna", "`03_qualidade_de_dados`"),
        ("total", "bigint", "Linhas da tabela.", ">= 0", "`count(*)`"),
        ("nulos", "bigint", "Linhas nulas ou em branco na coluna.", ">= 0", "Soma condicional"),
        ("pct_nulos", "double", "Percentual de nulos.", "0 a 100", "`nulos / total * 100`"),
    ],
}
CATALOGO["silver.dq_outliers"] = {
    "camada": "silver",
    "descricao": "Estatísticas e contagem de outliers (regra do IQR) das variáveis numéricas principais.",
    "colunas": [
        ("tabela", "string", "Tabela Bronze.", "Nome de tabela Bronze", "`03_qualidade_de_dados`"),
        ("coluna", "string", "Coluna numérica analisada.", "price, freight_value, product_weight_g", "`03_qualidade_de_dados`"),
        ("minimo", "double", "Menor valor.", "Numérico", "`min(try_cast(col as double))`"),
        ("q1", "double", "Primeiro quartil (aproximado).", "Numérico", "`percentile_approx(., 0.25)`"),
        ("mediana", "double", "Mediana (aproximada).", "Numérico", "`percentile_approx(., 0.50)`"),
        ("q3", "double", "Terceiro quartil (aproximado).", "Numérico", "`percentile_approx(., 0.75)`"),
        ("maximo", "double", "Maior valor.", "Numérico", "`max(try_cast(col as double))`"),
        ("limite_inferior", "double", "Q1 - 1,5 x IQR.", "Numérico", "Calculado"),
        ("limite_superior", "double", "Q3 + 1,5 x IQR.", "Numérico", "Calculado"),
        ("qtd_outliers", "bigint", "Valores fora dos limites.", ">= 0", "Contagem"),
        ("pct_outliers", "double", "Percentual de outliers.", "0 a 100", "`qtd_outliers / n * 100`"),
    ],
}

# ======================================================================================================
# GOLD
# ======================================================================================================
CATALOGO["gold.dim_data"] = {
    "camada": "gold",
    "descricao": "Dimensão calendário (01/09/2016 a 31/12/2018), com janelas de datas comemorativas do varejo brasileiro.",
    "colunas": [
        ("sk_data", "int", "Chave substituta da data no formato `yyyyMMdd` (PK).", "Ex.: 20171124", "Gerada em `05_gold_modelagem`"),
        ("data", "date", "Data do calendário.", "2016-09-01 a 2018-12-31", "Sequência de datas gerada em Python"),
        ("ano", "int", "Ano.", "2016 a 2018", "Derivado de `data`"),
        ("mes", "int", "Mês.", "1 a 12", "Derivado de `data`"),
        ("nome_mes", "string", "Nome do mês em português.", "janeiro a dezembro", "Derivado de `data`"),
        ("ano_mes", "string", "Ano e mês.", "`yyyy-MM`", "Derivado de `data`"),
        ("trimestre", "int", "Trimestre do ano.", "1 a 4", "Derivado de `data`"),
        ("dia_semana_num", "int", "Dia da semana ISO.", "1 (segunda) a 7 (domingo)", "Derivado de `data`"),
        ("dia_semana", "string", "Nome do dia da semana.", "segunda a domingo", "Derivado de `data`"),
        ("semana_iso", "int", "Semana ISO do ano.", "1 a 53", "Derivado de `data`"),
        ("fim_de_semana", "boolean", "Verdadeiro para sábado e domingo.", "true / false", "Derivado de `data`"),
        ("evento_comercial", "string", "Janela de data comemorativa em que o dia se enquadra.", "Semana do Consumidor, Dia das Mães, Dia dos Namorados, Dia dos Pais, Dia das Crianças, Black Friday, Natal, Sem evento", "Regras de calendário em `05_gold_modelagem` (janela antes/depois da data-âncora)"),
    ],
}
CATALOGO["gold.dim_cliente"] = {
    "camada": "gold",
    "descricao": "Dimensão de clientes (um `customer_id` por linha) com localização e região. Inclui o membro `sk_cliente = -1` (Não informado).",
    "colunas": [
        ("sk_cliente", "int", "Chave substituta (PK). -1 = Não informado.", "-1 ou inteiro >= 1", "`row_number()` sobre `customer_id`"),
        ("customer_id", "string", "Chave natural: cliente por pedido.", "Hash de 32 caracteres ou `NAO_INFORMADO`", "`silver.clientes.customer_id`"),
        ("cliente_unico_id", "string", "Consumidor real; base da métrica de recompra.", "Hash de 32 caracteres ou `NAO_INFORMADO`", "`silver.clientes.cliente_unico_id`"),
        ("cep_prefixo", "string", "Cinco primeiros dígitos do CEP.", "5 dígitos ou nulo", "`silver.clientes.cep_prefixo`"),
        ("cidade", "string", "Cidade.", "Texto livre", "`silver.clientes.cidade`"),
        ("uf", "string", "Sigla da UF.", "27 UFs ou `NI`", "`silver.clientes.uf`"),
        ("regiao", "string", "Região do Brasil.", "Norte, Nordeste, Centro-Oeste, Sudeste, Sul, Não informada", "`silver.clientes.regiao`"),
    ],
}
CATALOGO["gold.dim_vendedor"] = {
    "camada": "gold",
    "descricao": "Dimensão de vendedores. Inclui o membro `sk_vendedor = -1` (Não informado).",
    "colunas": [
        ("sk_vendedor", "int", "Chave substituta (PK). -1 = Não informado.", "-1 ou inteiro >= 1", "`row_number()` sobre `seller_id`"),
        ("seller_id", "string", "Chave natural do vendedor.", "Hash de 32 caracteres ou `NAO_INFORMADO`", "`silver.vendedores.seller_id`"),
        ("cep_prefixo", "string", "Cinco primeiros dígitos do CEP.", "5 dígitos ou nulo", "`silver.vendedores.cep_prefixo`"),
        ("cidade", "string", "Cidade do vendedor.", "Texto livre", "`silver.vendedores.cidade`"),
        ("uf", "string", "Sigla da UF.", "27 UFs ou `NI`", "`silver.vendedores.uf`"),
    ],
}
CATALOGO["gold.dim_produto"] = {
    "camada": "gold",
    "descricao": "Dimensão de produtos com o segmento de moda. Produtos fora de moda têm `segmento = 'Fora de moda'`. Inclui o membro `sk_produto = -1`.",
    "colunas": [
        ("sk_produto", "int", "Chave substituta (PK). -1 = Não informado.", "-1 ou inteiro >= 1", "`row_number()` sobre `product_id`"),
        ("product_id", "string", "Chave natural do produto.", "Hash de 32 caracteres ou `NAO_INFORMADO`", "`silver.produtos.product_id`"),
        ("categoria_pt", "string", "Categoria em português.", "Categorias do dataset ou `sem_categoria`", "`silver.produtos.categoria_pt`"),
        ("categoria_en", "string", "Categoria em inglês.", "Tradução ou nome em português", "`silver.produtos.categoria_en`"),
        ("segmento", "string", "Segmento de moda ou `Fora de moda`.", "Vestuário feminino/masculino/infantojuvenil, Calçados, Bolsas e acessórios, Moda íntima e praia, Moda esportiva, Malas e acessórios de viagem, Moda - outros, Fora de moda", "`coalesce(silver.produtos.segmento_moda, 'Fora de moda')`"),
        ("eh_moda", "boolean", "Verdadeiro se o produto é de moda (inclui malas de viagem).", "true / false", "`silver.produtos.eh_moda`"),
        ("fotos_qtd", "int", "Número de fotos do anúncio.", "Inteiro >= 0 ou nulo", "`silver.produtos.fotos_qtd`"),
        ("peso_g", "double", "Peso em gramas.", "> 0 ou nulo", "`silver.produtos.peso_g`"),
        ("volume_cm3", "double", "Volume da embalagem em cm³.", "> 0 ou nulo", "`comprimento_cm * altura_cm * largura_cm`"),
    ],
}
CATALOGO["gold.fato_vendas_item"] = {
    "camada": "gold",
    "descricao": "Tabela fato no **grão de item de pedido**: preço e frete de cada item vendido. Use `conta_receita = true` para excluir pedidos cancelados/indisponíveis.",
    "colunas": [
        ("order_id", "string", "Pedido (chave degenerada; PK composta com `item_seq`).", "Hash de 32 caracteres", "`silver.itens_pedido.order_id`"),
        ("item_seq", "int", "Sequência do item no pedido (PK composta).", "Inteiro >= 1", "`silver.itens_pedido.item_seq`"),
        ("sk_data_compra", "int", "FK para `dim_data.sk_data` (data da compra).", "`yyyyMMdd`", "`date_format(silver.pedidos.dt_compra, 'yyyyMMdd')`"),
        ("sk_cliente", "int", "FK para `dim_cliente`.", "-1 ou inteiro >= 1", "JOIN por `customer_id` do pedido; `coalesce(..., -1)`"),
        ("sk_produto", "int", "FK para `dim_produto`.", "-1 ou inteiro >= 1", "JOIN por `product_id`; `coalesce(..., -1)`"),
        ("sk_vendedor", "int", "FK para `dim_vendedor`.", "-1 ou inteiro >= 1", "JOIN por `seller_id`; `coalesce(..., -1)`"),
        ("status_pedido", "string", "Situação do pedido.", "delivered, shipped, canceled, unavailable, invoiced, processing, created, approved", "`silver.pedidos.status`"),
        ("conta_receita", "boolean", "Verdadeiro se o pedido não está cancelado nem indisponível.", "true / false", "`status NOT IN ('canceled','unavailable')`"),
        ("preco", "decimal(12,2)", "Medida: preço do item em R$, sem frete.", "> 0", "`silver.itens_pedido.preco`"),
        ("frete", "decimal(12,2)", "Medida: frete do item em R$.", ">= 0", "`silver.itens_pedido.frete`"),
        ("valor_total_item", "decimal(13,2)", "Medida: preço + frete.", "> 0", "`silver.itens_pedido.valor_total_item`"),
        ("frete_pct_preco", "double", "Peso do frete sobre o preço (fricção de checkout). 0,25 = frete de 25% do preço.", ">= 0", "`round(frete / preco, 4)`"),
    ],
}
CATALOGO["gold.fato_pedido"] = {
    "camada": "gold",
    "descricao": "Tabela fato no **grão de pedido**: itens, pagamento, avaliação e entrega consolidados. Evita a contagem dupla de valores pagos e notas.",
    "colunas": [
        ("order_id", "string", "Pedido (PK).", "Hash de 32 caracteres, único", "`silver.pedidos.order_id`"),
        ("sk_data_compra", "int", "FK para `dim_data.sk_data`.", "`yyyyMMdd`", "`date_format(dt_compra, 'yyyyMMdd')`"),
        ("sk_cliente", "int", "FK para `dim_cliente`.", "-1 ou inteiro >= 1", "JOIN por `customer_id`; `coalesce(..., -1)`"),
        ("status_pedido", "string", "Situação do pedido.", "delivered, shipped, canceled, unavailable, invoiced, processing, created, approved", "`silver.pedidos.status`"),
        ("conta_receita", "boolean", "Verdadeiro se o pedido não está cancelado nem indisponível.", "true / false", "`status NOT IN ('canceled','unavailable')`"),
        ("qtd_itens", "bigint", "Medida: itens no pedido (0 se o pedido não tem itens).", ">= 0", "`count(*)` de `silver.itens_pedido` por pedido"),
        ("qtd_itens_moda", "bigint", "Medida: itens de moda no pedido.", ">= 0 e <= qtd_itens", "Soma de `eh_moda` (JOIN com `silver.produtos`)"),
        ("eh_pedido_moda", "boolean", "Verdadeiro se o pedido tem ao menos um item de moda.", "true / false", "`qtd_itens_moda > 0`"),
        ("receita_itens", "decimal(22,2)", "Medida: soma dos preços dos itens, sem frete.", ">= 0 ou nulo", "`sum(preco)` por pedido"),
        ("receita_moda", "decimal(22,2)", "Medida: soma dos preços apenas dos itens de moda.", ">= 0 ou nulo", "`sum(preco)` dos itens com `eh_moda`"),
        ("frete_total", "decimal(22,2)", "Medida: frete total do pedido.", ">= 0 ou nulo", "`sum(frete)` por pedido"),
        ("valor_pago_total", "decimal(22,2)", "Medida: total pago (soma de todos os pagamentos).", ">= 0 ou nulo", "`sum(valor_pago)` de `silver.pagamentos`"),
        ("parcelas_max", "int", "Maior número de parcelas entre os pagamentos do pedido.", ">= 1 ou nulo", "`max(parcelas)`"),
        ("qtd_meios_pagamento", "bigint", "Número de pagamentos (meios) usados no pedido.", ">= 1 ou nulo", "`count(*)` de `silver.pagamentos`"),
        ("tipo_pagamento_principal", "string", "Meio de pagamento de maior valor no pedido.", "credit_card, boleto, voucher, debit_card, nao_definido ou nulo", "Pagamento com maior `valor_pago` (`row_number`)"),
        ("nota_avaliacao", "int", "Nota de satisfação (1 avaliação por pedido).", "1 a 5 ou nulo", "`silver.avaliacoes.nota`"),
        ("dias_ate_entrega", "int", "Dias entre compra e entrega ao cliente.", ">= 0 ou nulo", "`silver.pedidos.dias_ate_entrega`"),
        ("atraso_dias", "int", "Dias de atraso frente ao prometido (positivo = atrasou).", "Inteiro ou nulo", "`silver.pedidos.atraso_dias`"),
        ("entregue_no_prazo", "boolean", "Verdadeiro se entregue até a data prometida.", "true / false / nulo", "`silver.pedidos.entregue_no_prazo`"),
    ],
}
CATALOGO["gold.agg_mensal_segmento"] = {
    "camada": "gold",
    "descricao": "Agregação mensal por segmento (moda e `Fora de moda`), apenas pedidos que contam receita. Alimenta a análise de sazonalidade.",
    "colunas": [
        ("ano_mes", "string", "Ano e mês.", "`yyyy-MM`", "`dim_data.ano_mes`"),
        ("ano", "int", "Ano.", "2016 a 2018", "`dim_data.ano`"),
        ("mes", "int", "Mês.", "1 a 12", "`dim_data.mes`"),
        ("segmento", "string", "Segmento do produto.", "Segmentos de moda ou `Fora de moda`", "`dim_produto.segmento`"),
        ("eh_moda", "boolean", "Verdadeiro se o segmento é de moda.", "true / false", "`dim_produto.eh_moda`"),
        ("pedidos", "bigint", "Pedidos distintos com ao menos um item do segmento no mês.", ">= 1", "`count(DISTINCT order_id)`"),
        ("itens", "bigint", "Itens vendidos.", ">= 1", "`count(*)`"),
        ("receita", "decimal(22,2)", "Receita (preço dos itens, sem frete) em R$.", "> 0", "`sum(preco)`"),
        ("frete_total", "decimal(22,2)", "Frete total em R$.", ">= 0", "`sum(frete)`"),
        ("preco_medio_item", "decimal(12,2)", "Preço médio do item em R$.", "> 0", "`round(avg(preco), 2)`"),
        ("preco_mediano_item", "decimal(12,2)", "Preço mediano (aproximado) do item em R$.", "> 0", "`round(percentile_approx(preco, 0.5), 2)`"),
    ],
}
CATALOGO["gold.agg_uf_moda"] = {
    "camada": "gold",
    "descricao": "Agregação por UF do cliente, somente itens de moda de pedidos que contam receita. Alimenta a análise geográfica e de frete.",
    "colunas": [
        ("uf", "string", "UF do cliente.", "27 UFs ou `NI`", "`dim_cliente.uf`"),
        ("regiao", "string", "Região do Brasil.", "Norte, Nordeste, Centro-Oeste, Sudeste, Sul, Não informada", "`dim_cliente.regiao`"),
        ("pedidos", "bigint", "Pedidos distintos com item de moda.", ">= 1", "`count(DISTINCT order_id)`"),
        ("clientes_unicos", "bigint", "Consumidores reais distintos.", ">= 1", "`count(DISTINCT cliente_unico_id)`"),
        ("itens", "bigint", "Itens de moda vendidos.", ">= 1", "`count(*)`"),
        ("receita", "decimal(22,2)", "Receita de moda em R$ (sem frete).", "> 0", "`sum(preco)`"),
        ("frete_total", "decimal(22,2)", "Frete dos itens de moda em R$.", ">= 0", "`sum(frete)`"),
        ("receita_por_pedido", "decimal(14,2)", "Receita média por pedido em R$.", "> 0", "`round(receita / pedidos, 2)`"),
        ("frete_pct_da_receita", "double", "Frete total dividido pela receita (0,20 = 20%).", ">= 0", "`round(frete_total / receita, 4)`"),
    ],
}
CATALOGO["gold.catalogo_dados"] = {
    "camada": "gold",
    "descricao": "Este catálogo em forma de tabela: uma linha por coluna de cada tabela do projeto, com tipo, descrição, domínio e linhagem.",
    "colunas": [
        ("camada", "string", "Camada da tabela.", "bronze, silver, gold", "`catalogo_metadados.py`"),
        ("tabela", "string", "Nome da tabela.", "Nome de tabela do projeto", "`catalogo_metadados.py`"),
        ("coluna", "string", "Nome da coluna.", "Nome de coluna", "`catalogo_metadados.py`"),
        ("tipo", "string", "Tipo de dado da coluna, lido do schema real da tabela.", "Tipo Spark", "Schema da tabela no momento da execução"),
        ("descricao", "string", "O que a coluna representa.", "Texto", "`catalogo_metadados.py`"),
        ("dominio", "string", "Valores válidos ou esperados.", "Texto", "`catalogo_metadados.py`"),
        ("linhagem", "string", "Origem do dado e transformação aplicada.", "Texto", "`catalogo_metadados.py`"),
    ],
}

CATALOGO["gold.resumo_analise"] = {
    "camada": "gold",
    "descricao": "Indicadores-chave gerados pelo notebook de análise (06), uma linha por indicador e por pergunta de negócio. Base para a discussão do README.",
    "colunas": [
        ("pergunta", "string", "Pergunta de negócio a que o indicador pertence.", "Base, P1, P2, P3, P4, P5, P6", "`06_analise_perguntas`"),
        ("indicador", "string", "Nome do indicador.", "Texto", "`06_analise_perguntas`"),
        ("valor", "string", "Valor já formatado (moeda, percentual ou texto).", "Texto", "Calculado por consultas SQL sobre as tabelas Gold"),
    ],
}
