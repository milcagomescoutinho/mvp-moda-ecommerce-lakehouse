"""Teste local do pipeline (Spark local + dados sintéticos).

Executa os notebooks 00 a 07 em ordem, exatamente como estão no repositório (as linhas `# MAGIC` e `# COMMAND` são
removidas para que o arquivo rode como Python puro; `%run` é substituído por herdar o namespace da configuração).
Valida também que os defeitos plantados foram detectados e tratados.

Uso (na raiz do repositório):
    pip install pyspark pandas matplotlib
    python tests/gerar_dados_teste.py /tmp/olist_teste
    python tests/teste_local.py /tmp/olist_teste

Não roda no Databricks: serve para validar a lógica antes de subir o projeto.
"""
import os
import shutil
import sys
import tempfile

os.environ.setdefault("MPLBACKEND", "Agg")
RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = sys.argv[1] if len(sys.argv) > 1 else "/tmp/olist_teste"
WAREHOUSE = tempfile.mkdtemp(prefix="wh_")
os.environ["MVP_RAW_DIR"] = RAW
os.environ["MVP_CATALOG"] = "spark_catalog"

from pyspark.sql import SparkSession  # noqa: E402

spark = (SparkSession.builder.master("local[2]").appName("teste_local")
         .config("spark.sql.warehouse.dir", WAREHOUSE).config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


if os.environ.get("SALVAR_GRAFICOS"):
    # Opcional: salva cada gráfico do notebook 06 em PNG (útil para conferir o visual sem abrir o Databricks)
    import matplotlib.pyplot as plt
    os.makedirs(os.environ["SALVAR_GRAFICOS"], exist_ok=True)
    _contador = [0]

    def _salvar(*a, **k):
        _contador[0] += 1
        plt.gcf().savefig(os.path.join(os.environ["SALVAR_GRAFICOS"], f"grafico_{_contador[0]}.png"))
        plt.close("all")

    plt.show = _salvar


def display(obj, *a, **k):
    try:
        n = obj.count() if hasattr(obj, "count") and callable(obj.count) and hasattr(obj, "limit") else len(obj)
    except Exception:
        n = "?"
    print(f"   [display] {n} linhas")


def executar(nome, ns):
    caminho = os.path.join(RAIZ, "notebooks", nome)
    linhas = [l for l in open(caminho, encoding="utf-8").read().splitlines() if not l.startswith("# MAGIC") and not l.startswith("# COMMAND")]
    exec(compile("\n".join(linhas), nome, "exec"), ns)
    return ns


print("=" * 90)
base = {"spark": spark, "display": display}
executar("00_config.py", base)
executar("catalogo_metadados.py", base)

resultados = {}
for nb in ["01_setup_catalogo_e_volume.py", "02_bronze_ingestao.py", "03_qualidade_de_dados.py", "04_silver_limpeza.py",
           "05_gold_modelagem.py", "06_analise_perguntas.py", "07_catalogo_de_dados.py"]:
    print("\n" + "=" * 90 + f"\n>>> {nb}\n" + "=" * 90)
    resultados[nb] = executar(nb, dict(base))

# ------------------------------------------------------------------------------------------------------
# Verificações
# ------------------------------------------------------------------------------------------------------
print("\n" + "=" * 90 + "\nVERIFICAÇÕES\n" + "=" * 90)
falhas = []


def checar(descricao, condicao):
    print(("  OK   " if condicao else "  FALHA ") + descricao)
    if not condicao:
        falhas.append(descricao)


def dq(tabela, regra_contem):
    r = spark.sql(f"SELECT registros_com_problema p FROM spark_catalog.silver.dq_resultados WHERE tabela='{tabela}' AND regra LIKE '%{regra_contem}%'").collect()
    return r[0]["p"] if r else None


checar("DQ detectou UF inválida em customers", (dq("olist_customers", "UF pertence") or 0) >= 1)
checar("DQ detectou CEP com 4 dígitos", (dq("olist_customers", "CEP com exatamente 5") or 0) >= 1)
checar("DQ detectou preço <= 0", (dq("olist_order_items", "Preço maior que zero") or 0) >= 1)
checar("DQ detectou parcelas < 1", (dq("olist_order_payments", "Parcelas maiores") or 0) >= 1)
checar("DQ detectou entrega antes da compra", (dq("olist_orders", "Entrega ocorre depois") or 0) >= 1)
checar("DQ detectou review_id repetido", (dq("olist_order_reviews", "Chave (review_id)") or 0) >= 1)
checar("DQ detectou mais de 1 avaliação por pedido", (dq("olist_order_reviews", "Chave (order_id)") or 0) >= 1)
checar("DQ detectou item órfão", (dq("olist_order_items", "order_id existe em olist_orders") or 0) >= 1)
checar("DQ detectou peso <= 0", (dq("olist_products", "Peso maior") or 0) >= 1)
checar("DQ detectou categoria nula", (dq("olist_products", "Categoria preenchida") or 0) >= 1)

q = spark.sql("SELECT motivo, count(*) n FROM spark_catalog.silver.quarentena_registros GROUP BY motivo").collect()
motivos = {r["motivo"]: r["n"] for r in q}
checar("Quarentena recebeu o preço zero", any("preço" in m for m in motivos))
checar("Quarentena recebeu o item de pedido inexistente", any("pedido inexistente" in m for m in motivos))

checar("Silver: CEP sempre com 5 dígitos", spark.sql("SELECT count(*) c FROM spark_catalog.silver.clientes WHERE length(cep_prefixo) <> 5").collect()[0]["c"] == 0)
checar("Silver: UF inválida virou NI", spark.sql("SELECT count(*) c FROM spark_catalog.silver.clientes WHERE uf = 'NI'").collect()[0]["c"] == 1)
checar("Silver: parcelas sempre >= 1", spark.sql("SELECT min(parcelas) m FROM spark_catalog.silver.pagamentos").collect()[0]["m"] >= 1)
checar("Silver: parcela corrigida sinalizada", spark.sql("SELECT count(*) c FROM spark_catalog.silver.pagamentos WHERE flag_parcelas_corrigida").collect()[0]["c"] >= 1)
checar("Silver: 1 avaliação por pedido", spark.sql("SELECT count(*) - count(DISTINCT order_id) d FROM spark_catalog.silver.avaliacoes").collect()[0]["d"] == 0)
checar("Silver: entrega inconsistente sinalizada",
       spark.sql("SELECT count(*) c FROM spark_catalog.silver.pedidos WHERE flag_datas_inconsistentes").collect()[0]["c"] >= 1)
checar("Silver: peso zero virou nulo", spark.sql("SELECT count(*) c FROM spark_catalog.silver.produtos WHERE peso_g <= 0").collect()[0]["c"] == 0)
checar("Silver: produtos de moda identificados", spark.sql("SELECT count(*) c FROM spark_catalog.silver.produtos WHERE eh_moda").collect()[0]["c"] > 0)
checar("Silver: malas de viagem classificadas como moda",
       spark.sql("SELECT count(*) c FROM spark_catalog.silver.produtos WHERE segmento_moda = 'Malas e acessórios de viagem'").collect()[0]["c"] > 0)

checar("Gold: nenhum item perdeu a chave de produto (sk = -1 só se órfão)",
       spark.sql("SELECT count(*) c FROM spark_catalog.gold.fato_vendas_item WHERE sk_produto = -1").collect()[0]["c"] == 0)
checar("Gold: fato_pedido tem 1 linha por pedido",
       spark.sql("SELECT count(*) - count(DISTINCT order_id) d FROM spark_catalog.gold.fato_pedido").collect()[0]["d"] == 0)
checar("Gold: Black Friday marcada na dim_data",
       spark.sql("SELECT count(*) c FROM spark_catalog.gold.dim_data WHERE data = DATE'2017-11-24' AND evento_comercial = 'Black Friday'").collect()[0]["c"] == 1)
checar("Gold: Dia das Mães 2017 = 14/05",
       spark.sql("SELECT count(*) c FROM spark_catalog.gold.dim_data WHERE data = DATE'2017-05-14' AND evento_comercial = 'Dia das Mães'").collect()[0]["c"] == 1)
checar("Gold: Dia dos Pais 2018 = 12/08",
       spark.sql("SELECT count(*) c FROM spark_catalog.gold.dim_data WHERE data = DATE'2018-08-12' AND evento_comercial = 'Dia dos Pais'").collect()[0]["c"] == 1)
checar("Análise: resumo_analise preenchido", spark.table("spark_catalog.gold.resumo_analise").count() > 20)

avisos = resultados["07_catalogo_de_dados.py"].get("avisos", ["?"])
for a in avisos:
    print("     aviso de catálogo:", a)
checar("Catálogo: todas as tabelas/colunas documentadas e tipos conferem (sem avisos)", len(avisos) == 0)
checar("Catálogo: falhas ao gravar comentários = 0", resultados["07_catalogo_de_dados.py"].get("falhas", -1) == 0)

shutil.rmtree(WAREHOUSE, ignore_errors=True)
print("\n" + ("TODOS OS TESTES PASSARAM" if not falhas else f"{len(falhas)} FALHA(S): {falhas}"))
sys.exit(1 if falhas else 0)
