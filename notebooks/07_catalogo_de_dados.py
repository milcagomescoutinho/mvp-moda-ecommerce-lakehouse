# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Catálogo de dados (Unity Catalog)
# MAGIC
# MAGIC **Etapa 4.3 (Modelagem: Catálogo de Dados)** — documenta cada tabela e cada coluna diretamente no **Unity Catalog**, de modo que
# MAGIC a descrição apareça no *Catalog Explorer* e em `information_schema`. Para cada coluna gravamos:
# MAGIC
# MAGIC 1. **descrição** do que ela representa; 2. **tipo** de dado (lido do schema real); 3. **domínio de valores**; 4. **linhagem** (de onde veio e que transformação sofreu).
# MAGIC
# MAGIC As descrições vêm de `catalogo_metadados.py` (fonte única, também usada para gerar o catálogo do README).
# MAGIC O notebook ainda **valida** o catálogo contra as tabelas reais: avisa se faltar coluna documentada, se sobrar coluna sem documentação ou se o tipo divergir.
# MAGIC
# MAGIC Além dos comentários, cria a tabela `gold.catalogo_dados` (uma linha por coluna) para consulta via SQL.
# MAGIC
# MAGIC **Rode por último (depois do notebook 06)**, quando todas as tabelas, inclusive `gold.resumo_analise`, já existem.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./catalogo_metadados

# COMMAND ----------

from pyspark.sql import functions as F


def existe(nome_completo):
    camada, tabela = nome_completo.split(".")
    return spark.catalog.tableExists(f"{CATALOG}.{camada}.{tabela}")


def schema_real(nome_completo):
    camada, tabela = nome_completo.split(".")
    return {f.name: f.dataType.simpleString() for f in spark.table(f"{CATALOG}.{camada}.{tabela}").schema.fields}


# --- 1) Tabela de catálogo (uma linha por coluna) ----------------------------------------------------
linhas, avisos = [], []
for nome, meta in CATALOGO.items():
    camada, tabela = nome.split(".")
   real = schema_real(nome) if existe(nome) else None
    if real is None and nome != "gold.catalogo_dados":
        avisos.append(f"Tabela documentada mas inexistente: {nome}")
    documentadas = set()
    for coluna, tipo, descricao, dominio, linhagem in meta["colunas"]:
        documentadas.add(coluna)
        tipo_final = real[coluna] if real and coluna in real else tipo
        if real and coluna not in real:
            avisos.append(f"{nome}.{coluna}: documentada mas não existe na tabela")
        elif real and real[coluna] != tipo:
            avisos.append(f"{nome}.{coluna}: tipo documentado '{tipo}' difere do real '{real[coluna]}'")
        linhas.append((camada, tabela, coluna, tipo_final, descricao, dominio, linhagem))
    if real:
        for coluna in real:
            if coluna not in documentadas:
                avisos.append(f"{nome}.{coluna}: existe na tabela mas não está documentada")

df_cat = spark.createDataFrame(linhas, "camada string, tabela string, coluna string, tipo string, descricao string, dominio string, linhagem string")
df_cat.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.catalogo_dados")
print(f"{GOLD}.catalogo_dados: {df_cat.count()} colunas documentadas em {len(CATALOGO)} tabelas")

print("\nValidação do catálogo contra as tabelas reais:")
if avisos:
    for a in avisos:
        print("  ⚠", a)
else:
    print("  OK: todas as tabelas e colunas estão documentadas e os tipos conferem.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravando as descrições no Unity Catalog
# MAGIC `COMMENT ON TABLE` para a tabela e `ALTER TABLE ... ALTER COLUMN ... COMMENT` para cada coluna.
# MAGIC O comentário da coluna junta descrição, domínio e linhagem em uma linha, que é o que o Catalog Explorer exibe.

# COMMAND ----------

def esc(texto):
    return texto.replace("\\", "\\\\").replace("'", "\\'")


falhas = 0
for nome, meta in CATALOGO.items():
    if not existe(nome):
        continue
    camada, tabela = nome.split(".")
    completo = f"{CATALOG}.{camada}.{tabela}"
    try:
        spark.sql(f"COMMENT ON TABLE {completo} IS '{esc(meta['descricao'])}'")
    except Exception as e:
        falhas += 1
        print(f"  (tabela {nome}) {str(e).splitlines()[0][:120]}")
    reais = schema_real(nome)
    for coluna, tipo, descricao, dominio, linhagem in meta["colunas"]:
        if coluna not in reais:
            continue
        comentario = f"{descricao} | Domínio: {dominio} | Origem: {linhagem}"
        try:
            spark.sql(f"ALTER TABLE {completo} ALTER COLUMN `{coluna}` COMMENT '{esc(comentario)}'")
        except Exception as e:
            falhas += 1
            print(f"  ({nome}.{coluna}) {str(e).splitlines()[0][:120]}")

print(f"Comentários gravados. Falhas: {falhas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultando o catálogo
# MAGIC Evidências para o README: (1) o catálogo como tabela e (2) o `information_schema` do próprio Unity Catalog.

# COMMAND ----------

display(spark.sql(f"""
    SELECT camada, tabela, COUNT(*) AS colunas
    FROM {GOLD}.catalogo_dados
    GROUP BY camada, tabela
    ORDER BY CASE camada WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 ELSE 3 END, tabela
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT tabela, coluna, tipo, descricao, dominio, linhagem
    FROM {GOLD}.catalogo_dados
    WHERE tabela = 'fato_vendas_item'
"""))

# COMMAND ----------

if IS_DATABRICKS:
    print("Comentários de coluna lidos direto do Unity Catalog (information_schema):")
    display(spark.sql(f"""
        SELECT table_name, column_name, data_type, comment
        FROM {CATALOG}.information_schema.columns
        WHERE table_schema = 'gold' AND table_name IN ('fato_vendas_item', 'dim_produto')
        ORDER BY table_name, ordinal_position
    """))
    print("Comentários de tabela:")
    display(spark.sql(f"""
        SELECT table_schema, table_name, comment
        FROM {CATALOG}.information_schema.tables
        WHERE table_schema IN ('bronze', 'silver', 'gold')
        ORDER BY table_schema, table_name
    """))
else:
    print("Execução local: information_schema do Unity Catalog não existe aqui.")
