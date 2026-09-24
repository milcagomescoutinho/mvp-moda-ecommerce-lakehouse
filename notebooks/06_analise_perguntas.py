# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Análise: respondendo às perguntas de negócio
# MAGIC
# MAGIC **Etapa 4.5 (Análise Final de Dados)** — cada pergunta definida no README (Etapa 2 / 4.1) tem aqui uma consulta SQL sobre as tabelas **Gold**,
# MAGIC um gráfico e uma **leitura automática dos números** (frases geradas a partir do resultado), para servir de base à discussão escrita no README.
# MAGIC
# MAGIC **Contexto:** o Olist não traz gasto com anúncios. As perguntas, portanto, olham a **demanda e a economia do pedido de moda** que orientam decisões de mídia paga:
# MAGIC quando concentrar verba (P1), onde geossegmentar (P2), quanto se pode pagar para adquirir um cliente (P3), o que atrapalha a conversão no checkout (P4, P5) e se há retorno depois da primeira compra (P6).
# MAGIC
# MAGIC **Regras comuns**
# MAGIC * *Moda* = `dim_produto.eh_moda` (vestuário, calçados, bolsas, moda íntima/praia, moda esportiva e **malas de viagem**).
# MAGIC * Receita = soma do **preço dos itens, sem frete**, só de pedidos que contam receita (`conta_receita`: exclui `canceled` e `unavailable`).
# MAGIC * P1 e P3 usam só **meses completos** (jan/2017 a ago/2018). Antes disso o volume é residual e depois de ago/2018 o dataset termina antes do mês fechar.
# MAGIC * Onde há valores extremos, mostramos a **mediana** ao lado da média.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

ANO_MES_INI, ANO_MES_FIM = "2017-01", "2018-08"
DATA_INI, DATA_FIM = "2017-01-01", "2018-08-31"

# Paleta: azul para magnitude, laranja só para destaque, cinzas para contexto
AZUL, LARANJA, CINZA, TEXTO, TEXTO2 = "#2a78d6", "#eb6834", "#c3c2b7", "#0b0b0b", "#52514e"
plt.rcParams.update({
    "figure.figsize": (9.5, 4.6), "figure.dpi": 110, "font.size": 10,
    "axes.spines.top": False, "axes.spines.right": False, "axes.edgecolor": CINZA,
    "axes.labelcolor": TEXTO2, "xtick.color": TEXTO2, "ytick.color": TEXTO2,
    "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlelocation": "left",
})
resumo = []   # (pergunta, indicador, valor) -> gold.resumo_analise


def brl(x):
    return "R$ " + f"{x:,.0f}".replace(",", ".")


def brl2(x):
    return "R$ " + f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def pct(x, casas=1):
    return f"{100 * x:.{casas}f}%".replace(".", ",")


def registrar(pergunta, indicador, valor):
    resumo.append((pergunta, indicador, str(valor)))
    print(f"  • {indicador}: {valor}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Visão de trabalho: itens de moda
# MAGIC View temporária com um item de moda por linha, já ligada às dimensões de data, produto e cliente (JOIN das três dimensões com `fato_vendas_item`).
# MAGIC Todas as perguntas de item usam esta view, o que garante a mesma definição de "moda" e "receita" em toda a análise.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_itens_moda AS
SELECT f.order_id, f.item_seq, f.preco, f.frete, f.frete_pct_preco,
       p.segmento, p.categoria_pt,
       d.data, d.ano, d.mes, d.ano_mes, d.evento_comercial,
       c.uf, c.regiao, c.cliente_unico_id
FROM {GOLD}.fato_vendas_item f
JOIN {GOLD}.dim_produto p ON f.sk_produto      = p.sk_produto
JOIN {GOLD}.dim_data    d ON f.sk_data_compra  = d.sk_data
JOIN {GOLD}.dim_cliente c ON f.sk_cliente      = c.sk_cliente
WHERE f.conta_receita AND p.eh_moda
""")

base = spark.sql("""
SELECT COUNT(DISTINCT order_id) AS pedidos_moda, COUNT(*) AS itens_moda,
       CAST(SUM(preco) AS DOUBLE) AS receita_moda
FROM v_itens_moda
""").toPandas().iloc[0]
print("Universo de moda no dataset completo:")
registrar("Base", "Pedidos com item de moda", f"{int(base.pedidos_moda):,}".replace(",", "."))
registrar("Base", "Itens de moda vendidos", f"{int(base.itens_moda):,}".replace(",", "."))
registrar("Base", "Receita de moda (sem frete)", brl(base.receita_moda))

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1 · Sazonalidade: em que meses e datas comemorativas a demanda de moda se concentra?
# MAGIC **Decisão de mídia:** distribuição de verba ao longo do ano (*flighting*), calendário de picos e reforço em datas específicas.
# MAGIC A análise tem duas partes: a **receita mensal** e o **índice de receita diária** de cada janela comemorativa contra os dias sem evento
# MAGIC (`dim_data.evento_comercial`).

# COMMAND ----------

mensal = spark.sql(f"""
SELECT ano_mes, COUNT(DISTINCT order_id) AS pedidos, CAST(SUM(preco) AS DOUBLE) AS receita
FROM v_itens_moda
WHERE data BETWEEN '{DATA_INI}' AND '{DATA_FIM}'
GROUP BY ano_mes ORDER BY ano_mes
""").toPandas()
display(mensal)

media = mensal.receita.mean()
pico = mensal.loc[mensal.receita.idxmax()]
vale = mensal.loc[mensal.receita.idxmin()]
ano17 = mensal[mensal.ano_mes.between("2017-01", "2017-08")].receita.sum()
ano18 = mensal[mensal.ano_mes.between("2018-01", "2018-08")].receita.sum()

print("Leitura dos números (P1, receita mensal):")
registrar("P1", "Mês de maior receita de moda", f"{pico.ano_mes} ({brl(pico.receita)}, {pct(pico.receita / media - 1)} acima da média mensal)")
registrar("P1", "Mês de menor receita (período)", f"{vale.ano_mes} ({brl(vale.receita)})")
registrar("P1", "Média mensal de receita de moda", brl(media))
registrar("P1", "Crescimento jan-ago/2018 vs jan-ago/2017", pct(ano18 / ano17 - 1))

fig, ax = plt.subplots()
cores = [LARANJA if m == pico.ano_mes else AZUL for m in mensal.ano_mes]
ax.bar(mensal.ano_mes, mensal.receita / 1000, color=cores, width=0.7)
ax.axhline(media / 1000, color=TEXTO2, lw=1, ls="--")
ax.text(len(mensal) - 0.5, media / 1000, "  média", va="center", color=TEXTO2, fontsize=9)
ax.set_title(f"Receita de moda por mês: pico em {pico.ano_mes}, {pct(pico.receita / media - 1, 0)} acima da média")
ax.set_ylabel("Receita (R$ mil, sem frete)")
ax.tick_params(axis="x", rotation=60)
ax.grid(axis="y", alpha=0.25)
plt.tight_layout()
plt.show()

# COMMAND ----------

eventos = spark.sql(f"""
WITH rec AS (
  SELECT data, CAST(SUM(preco) AS DOUBLE) AS receita, COUNT(DISTINCT order_id) AS pedidos
  FROM v_itens_moda WHERE data BETWEEN '{DATA_INI}' AND '{DATA_FIM}' GROUP BY data
), dias AS (
  SELECT data, evento_comercial FROM {GOLD}.dim_data WHERE data BETWEEN '{DATA_INI}' AND '{DATA_FIM}'
)
SELECT d.evento_comercial, COUNT(*) AS dias,
       SUM(COALESCE(r.receita, 0)) AS receita, CAST(SUM(COALESCE(r.pedidos, 0)) AS DOUBLE) AS pedidos
FROM dias d LEFT JOIN rec r ON d.data = r.data
GROUP BY d.evento_comercial
""").toPandas()
eventos["receita_por_dia"] = eventos.receita / eventos.dias
base_dia = eventos.loc[eventos.evento_comercial == "Sem evento", "receita_por_dia"].iloc[0]
eventos["indice_vs_dia_normal"] = eventos.receita_por_dia / base_dia * 100
eventos = eventos.sort_values("indice_vs_dia_normal", ascending=False)
display(eventos)

print("Leitura dos números (P1, datas comemorativas):")
for _, r in eventos[eventos.evento_comercial != "Sem evento"].iterrows():
    registrar("P1", f"Índice de receita diária: {r.evento_comercial}", f"{r.indice_vs_dia_normal:.0f} (dia normal = 100; {int(r.dias)} dias na janela)")

plot = eventos.sort_values("indice_vs_dia_normal")
fig, ax = plt.subplots()
ax.barh(plot.evento_comercial, plot.indice_vs_dia_normal, color=[CINZA if e == "Sem evento" else AZUL for e in plot.evento_comercial], height=0.6)
ax.axvline(100, color=TEXTO2, lw=1, ls="--")
for y, v in enumerate(plot.indice_vs_dia_normal):
    ax.text(v, y, f"  {v:.0f}", va="center", color=TEXTO, fontsize=9)
top = eventos[eventos.evento_comercial != "Sem evento"].iloc[0]
ax.set_title(f"Receita diária por janela: {top.evento_comercial} lidera ({top.indice_vs_dia_normal:.0f} vs 100 em dia normal)")
ax.set_xlabel("Índice de receita diária de moda (dia sem evento = 100)")
ax.grid(axis="x", alpha=0.25)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2 · Geografia: quais UFs concentram a demanda de moda, com que ticket e com que peso de frete?
# MAGIC **Decisão de mídia:** priorização de praças na segmentação geográfica, lances por região e onde o frete pode inviabilizar a conversão.
# MAGIC Usa `gold.agg_uf_moda` (todo o período do dataset).

# COMMAND ----------

uf = spark.sql(f"""
SELECT uf, regiao, pedidos, clientes_unicos,
       CAST(receita AS DOUBLE) AS receita,
       CAST(receita_por_pedido AS DOUBLE) AS receita_por_pedido,
       frete_pct_da_receita
FROM {GOLD}.agg_uf_moda ORDER BY receita DESC
""").toPandas()
uf["pct_receita"] = uf.receita / uf.receita.sum()
uf["pct_acumulado"] = uf.pct_receita.cumsum()
display(uf)

reg = uf.groupby("regiao", as_index=False).agg(receita=("receita", "sum")).sort_values("receita", ascending=False)
reg["pct"] = reg.receita / reg.receita.sum()
relevantes = uf[uf.pct_receita >= 0.01]

print("Leitura dos números (P2):")
registrar("P2", "UF nº 1 em receita de moda", f"{uf.uf.iloc[0]} ({pct(uf.pct_receita.iloc[0])} da receita)")
registrar("P2", "Top 3 UFs concentram", f"{', '.join(uf.uf.head(3))}: {pct(uf.pct_acumulado.iloc[2])} da receita")
registrar("P2", "Top 5 UFs concentram", pct(uf.pct_acumulado.iloc[4]))
registrar("P2", "Região nº 1", f"{reg.regiao.iloc[0]} ({pct(reg.pct.iloc[0])} da receita)")
mais_caro = relevantes.sort_values("receita_por_pedido", ascending=False).iloc[0]
registrar("P2", "Maior ticket entre UFs com >= 1% da receita", f"{mais_caro.uf} ({brl2(mais_caro.receita_por_pedido)} por pedido)")
frete_alto = relevantes.sort_values("frete_pct_da_receita", ascending=False).iloc[0]
registrar("P2", "Maior peso de frete entre UFs com >= 1% da receita", f"{frete_alto.uf} (frete = {pct(frete_alto.frete_pct_da_receita)} da receita)")
frete_baixo = relevantes.sort_values("frete_pct_da_receita").iloc[0]
registrar("P2", "Menor peso de frete entre UFs com >= 1% da receita", f"{frete_baixo.uf} (frete = {pct(frete_baixo.frete_pct_da_receita)} da receita)")

top10 = uf.head(10).iloc[::-1]
fig, ax = plt.subplots()
ax.barh(top10.uf, top10.receita / 1000, color=AZUL, height=0.65)
for y, (v, p) in enumerate(zip(top10.receita / 1000, top10.pct_receita)):
    ax.text(v, y, f"  {pct(p)}", va="center", fontsize=9, color=TEXTO)
ax.set_title(f"Top 3 UFs ({', '.join(uf.uf.head(3))}) concentram {pct(uf.pct_acumulado.iloc[2], 0)} da receita de moda")
ax.set_xlabel("Receita de moda (R$ mil, sem frete)")
ax.grid(axis="x", alpha=0.25)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3 · Mix e ticket: quais segmentos de moda geram mais receita e qual teto de CPA cada um comporta?
# MAGIC **Decisão de mídia:** quais segmentos priorizar em campanhas de catálogo/Shopping e **quanto se pode pagar por um pedido** em cada um.
# MAGIC O **teto de CPA** é o ticket por pedido vezes a margem de contribuição. O Olist não informa margem, então os percentuais
# MAGIC (10%, 20%, 30%) são **hipóteses ilustrativas** para sensibilidade; o cliente deve trocar pela margem real.

# COMMAND ----------

seg = spark.sql(f"""
SELECT segmento, SUM(pedidos) AS pedidos, SUM(itens) AS itens,
       CAST(SUM(receita) AS DOUBLE) AS receita, CAST(SUM(frete_total) AS DOUBLE) AS frete
FROM {GOLD}.agg_mensal_segmento
WHERE ano_mes BETWEEN '{ANO_MES_INI}' AND '{ANO_MES_FIM}'
GROUP BY segmento
""").toPandas()

mediano = spark.sql(f"""
SELECT segmento, CAST(percentile_approx(preco, 0.5) AS DOUBLE) AS preco_mediano_item
FROM v_itens_moda WHERE data BETWEEN '{DATA_INI}' AND '{DATA_FIM}' GROUP BY segmento
""").toPandas()

total_mkt = seg.receita.sum()
moda = seg[seg.segmento != "Fora de moda"].merge(mediano, on="segmento", how="left")
moda["pct_receita_moda"] = moda.receita / moda.receita.sum()
moda["ticket_por_pedido"] = moda.receita / moda.pedidos
moda["preco_medio_item"] = moda.receita / moda.itens
for m in (0.10, 0.20, 0.30):
    moda[f"teto_cpa_{int(m * 100)}pct"] = moda.ticket_por_pedido * m
moda = moda.sort_values("receita", ascending=False)
display(moda)

share_moda = moda.receita.sum() / total_mkt
print("Leitura dos números (P3):")
registrar("P3", "Moda no marketplace (receita, jan/2017-ago/2018)", pct(share_moda))
registrar("P3", "Segmento nº 1 de moda", f"{moda.segmento.iloc[0]} ({pct(moda.pct_receita_moda.iloc[0])} da receita de moda)")
registrar("P3", "Top 3 segmentos concentram", pct(moda.pct_receita_moda.head(3).sum()))
maior_ticket = moda.sort_values("ticket_por_pedido", ascending=False).iloc[0]
menor_ticket = moda.sort_values("ticket_por_pedido").iloc[0]
registrar("P3", "Maior ticket por pedido", f"{maior_ticket.segmento} ({brl2(maior_ticket.ticket_por_pedido)})")
registrar("P3", "Menor ticket por pedido", f"{menor_ticket.segmento} ({brl2(menor_ticket.ticket_por_pedido)})")
registrar("P3", f"Teto de CPA a 20% de margem: {maior_ticket.segmento}", brl2(maior_ticket.teto_cpa_20pct))
registrar("P3", f"Teto de CPA a 20% de margem: {menor_ticket.segmento}", brl2(menor_ticket.teto_cpa_20pct))

plot = moda.iloc[::-1]
fig, ax = plt.subplots()
ax.barh(plot.segmento, plot.receita / 1000, color=AZUL, height=0.65)
for y, (v, t) in enumerate(zip(plot.receita / 1000, plot.ticket_por_pedido)):
    ax.text(v, y, f"  ticket {brl(t)}", va="center", fontsize=9, color=TEXTO)
# "$" precisa de escape: o Matplotlib interpreta um par de "$" como fórmula matemática
ax.set_title((f"{moda.segmento.iloc[0]} lidera a receita de moda ({pct(moda.pct_receita_moda.iloc[0], 0)})\n"
              f"ticket por pedido varia de {brl(menor_ticket.ticket_por_pedido)} a {brl(maior_ticket.ticket_por_pedido)}").replace("$", r"\$"))
ax.set_xlabel("Receita jan/2017-ago/2018 (R$ mil, sem frete)")
ax.set_xlim(0, plot.receita.max() / 1000 * 1.3)
ax.grid(axis="x", alpha=0.25)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4 · Frete: quanto o frete pesa sobre o pedido e isso coincide com pior experiência?
# MAGIC **Decisão de mídia:** frete é a principal **fricção de checkout**. Se o frete pesa muito no pedido, a mensagem de "frete grátis" e a segmentação por região passam a ser prioridade.
# MAGIC Unidade: **pedido** com ao menos um item de moda (`fato_pedido`), peso do frete = `frete_total / receita_itens`.
# MAGIC Atenção: a análise mostra **associação**, não causa (frete alto costuma vir de distância, que também aumenta atraso).

# COMMAND ----------

faixas = spark.sql(f"""
WITH p AS (
  SELECT fp.order_id, fp.nota_avaliacao, fp.entregue_no_prazo,
         CAST(fp.frete_total AS DOUBLE) / CAST(fp.receita_itens AS DOUBLE) AS frete_pct
  FROM {GOLD}.fato_pedido fp
  WHERE fp.conta_receita AND fp.eh_pedido_moda AND fp.receita_itens > 0
)
SELECT CASE WHEN frete_pct < 0.10 THEN '1. até 10%'
            WHEN frete_pct < 0.25 THEN '2. de 10% a 25%'
            WHEN frete_pct < 0.50 THEN '3. de 25% a 50%'
            ELSE '4. acima de 50%' END AS faixa_frete_sobre_preco,
       COUNT(*) AS pedidos,
       AVG(nota_avaliacao) AS nota_media,
       AVG(CASE WHEN nota_avaliacao <= 2 THEN 1.0 WHEN nota_avaliacao IS NOT NULL THEN 0.0 END) AS pct_nota_baixa,
       AVG(CASE WHEN entregue_no_prazo THEN 1.0 WHEN entregue_no_prazo IS NOT NULL THEN 0.0 END) AS pct_no_prazo
FROM p GROUP BY 1 ORDER BY 1
""").toPandas()
faixas["pct_pedidos"] = faixas.pedidos / faixas.pedidos.sum()
display(faixas)

por_regiao = spark.sql("""
SELECT regiao, COUNT(DISTINCT order_id) AS pedidos,
       CAST(percentile_approx(frete_pct_preco, 0.5) AS DOUBLE) AS frete_pct_mediano_item
FROM v_itens_moda GROUP BY regiao ORDER BY frete_pct_mediano_item DESC
""").toPandas()
display(por_regiao)

pesado = faixas[faixas.faixa_frete_sobre_preco.isin(["3. de 25% a 50%", "4. acima de 50%"])]
leve = faixas[faixas.faixa_frete_sobre_preco == "1. até 10%"]
print("Leitura dos números (P4):")
registrar("P4", "Pedidos de moda com frete acima de 25% do valor dos itens", pct(pesado.pedidos.sum() / faixas.pedidos.sum()))
for _, r in faixas.iterrows():
    registrar("P4", f"Nota média, frete {r.faixa_frete_sobre_preco[3:]}", f"{r.nota_media:.2f} ({int(r.pedidos):,} pedidos; {pct(r.pct_no_prazo)} no prazo)".replace(",", "."))
if len(leve) and len(faixas):
    registrar("P4", "Diferença de nota: frete acima de 50% vs até 10%",
              f"{faixas.nota_media.iloc[-1] - leve.nota_media.iloc[0]:+.2f} pontos".replace(".", ","))
registrar("P4", "Região com maior frete mediano sobre o preço do item", f"{por_regiao.regiao.iloc[0]} ({pct(por_regiao.frete_pct_mediano_item.iloc[0])})")
registrar("P4", "Região com menor frete mediano sobre o preço do item", f"{por_regiao.regiao.iloc[-1]} ({pct(por_regiao.frete_pct_mediano_item.iloc[-1])})")

fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.2))
rot = [f[3:] for f in faixas.faixa_frete_sobre_preco]
a1.bar(rot, faixas.pct_pedidos * 100, color=AZUL, width=0.6)
for x, v in enumerate(faixas.pct_pedidos * 100):
    a1.text(x, v, f"{v:.0f}%", ha="center", va="bottom", fontsize=9)
a1.set_title(f"{pct(pesado.pedidos.sum() / faixas.pedidos.sum(), 0)} dos pedidos têm frete > 25% dos itens")
a1.set_ylabel("% dos pedidos de moda")
a1.set_xlabel("Frete sobre o valor dos itens")
a2.bar(rot, faixas.nota_media, color=AZUL, width=0.6)
for x, v in enumerate(faixas.nota_media):
    a2.text(x, v, f"{v:.2f}".replace(".", ","), ha="center", va="bottom", fontsize=9)
a2.set_ylim(0, 5)
a2.set_title("Nota média por faixa de frete")
a2.set_ylabel("Nota (1 a 5)")
a2.set_xlabel("Frete sobre o valor dos itens")
for a in (a1, a2):
    a.grid(axis="y", alpha=0.25)
    a.tick_params(axis="x", labelsize=8)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5 · Pagamento: como os clientes de moda pagam e quanto o parcelamento se associa a ticket maior?
# MAGIC **Decisão de mídia:** mensagem de oferta ("em até Nx sem juros"), públicos de maior ticket e desenho de campanhas para cartão vs boleto.
# MAGIC Unidade: pedido com item de moda (`fato_pedido`). O parcelamento só existe para **cartão de crédito**, então as faixas de parcelas usam só esses pedidos.

# COMMAND ----------

meios = spark.sql(f"""
SELECT COALESCE(tipo_pagamento_principal, 'sem pagamento registrado') AS meio_principal,
       COUNT(*) AS pedidos, CAST(SUM(valor_pago_total) AS DOUBLE) AS valor
FROM {GOLD}.fato_pedido
WHERE conta_receita AND eh_pedido_moda
GROUP BY 1 ORDER BY pedidos DESC
""").toPandas()
meios["pct_pedidos"] = meios.pedidos / meios.pedidos.sum()
display(meios)

parc = spark.sql(f"""
SELECT CASE WHEN parcelas_max = 1 THEN '1x (à vista)'
            WHEN parcelas_max BETWEEN 2 AND 3 THEN '2 a 3x'
            WHEN parcelas_max BETWEEN 4 AND 6 THEN '4 a 6x'
            WHEN parcelas_max BETWEEN 7 AND 10 THEN '7 a 10x'
            ELSE '11x ou mais' END AS faixa_parcelas,
       CASE WHEN parcelas_max = 1 THEN 1 WHEN parcelas_max <= 3 THEN 2 WHEN parcelas_max <= 6 THEN 3
            WHEN parcelas_max <= 10 THEN 4 ELSE 5 END AS ordem,
       COUNT(*) AS pedidos,
       CAST(percentile_approx(CAST(valor_pago_total AS DOUBLE), 0.5) AS DOUBLE) AS ticket_mediano,
       CAST(AVG(CAST(valor_pago_total AS DOUBLE)) AS DOUBLE) AS ticket_medio
FROM {GOLD}.fato_pedido
WHERE conta_receita AND eh_pedido_moda AND tipo_pagamento_principal = 'credit_card' AND parcelas_max IS NOT NULL
GROUP BY 1, 2 ORDER BY ordem
""").toPandas()
parc["pct_pedidos_cartao"] = parc.pedidos / parc.pedidos.sum()
display(parc)

cartao = meios.loc[meios.meio_principal == "credit_card", "pct_pedidos"]
cartao = float(cartao.iloc[0]) if len(cartao) else 0.0
parcelado = parc[parc.faixa_parcelas != "1x (à vista)"].pedidos.sum() / parc.pedidos.sum()

print("Leitura dos números (P5):")
registrar("P5", "Meio de pagamento principal nº 1", f"{meios.meio_principal.iloc[0]} ({pct(meios.pct_pedidos.iloc[0])} dos pedidos de moda)")
registrar("P5", "Pedidos pagos principalmente no cartão de crédito", pct(cartao))
registrar("P5", "Pedidos no cartão com 2 ou mais parcelas", pct(parcelado))
registrar("P5", "Ticket mediano, cartão à vista (1x)", brl2(parc.ticket_mediano.iloc[0]))
registrar("P5", "Ticket mediano, cartão em 11x ou mais", brl2(parc.ticket_mediano.iloc[-1]))
registrar("P5", "Faixa de parcelas mais frequente", f"{parc.loc[parc.pedidos.idxmax(), 'faixa_parcelas']} ({pct(parc.pct_pedidos_cartao.max())} dos pedidos no cartão)")

fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.2), gridspec_kw={"width_ratios": [1, 1.3]})
m = meios.iloc[::-1]
a1.barh(m.meio_principal, m.pct_pedidos * 100, color=AZUL, height=0.6)
for y, v in enumerate(m.pct_pedidos * 100):
    a1.text(v, y, f"  {v:.1f}%".replace(".", ","), va="center", fontsize=9)
a1.set_title("Meio de pagamento principal")
a1.set_xlabel("% dos pedidos de moda")
a1.set_xlim(0, m.pct_pedidos.max() * 100 * 1.2)
a2.bar(parc.faixa_parcelas, parc.ticket_mediano, color=AZUL, width=0.6)
for x, v in enumerate(parc.ticket_mediano):
    a2.text(x, v, brl(v), ha="center", va="bottom", fontsize=9)
a2.set_title("Ticket mediano do pedido por nº de parcelas (cartão)")
a2.set_ylabel("Valor pago mediano (R$)")
for a in (a1, a2):
    a.grid(axis="y" if a is a2 else "x", alpha=0.25)
    a.tick_params(axis="x", labelsize=8)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P6 · Recompra: que parcela dos clientes de moda compra mais de uma vez?
# MAGIC **Decisão de mídia:** define se o CPA precisa se pagar **na primeira compra** (recompra baixa) ou se vale investir em aquisição e remarketing pensando em LTV (recompra alta).
# MAGIC Cliente = `cliente_unico_id` (o `customer_id` muda a cada pedido, então não serve para medir recompra). Compara moda com o marketplace inteiro.

# COMMAND ----------

def distribuicao_recompra(origem):
    return spark.sql(f"""
    WITH cli AS (
      SELECT cliente_unico_id, COUNT(DISTINCT order_id) AS n, SUM(preco) AS receita
      FROM {origem} GROUP BY cliente_unico_id
    )
    SELECT CASE WHEN n = 1 THEN '1 pedido' WHEN n = 2 THEN '2 pedidos' ELSE '3 ou mais' END AS faixa,
           COUNT(*) AS clientes, CAST(SUM(receita) AS DOUBLE) AS receita
    FROM cli GROUP BY 1 ORDER BY 1
    """).toPandas()


spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_itens_todos AS
SELECT f.order_id, f.preco, c.cliente_unico_id
FROM {GOLD}.fato_vendas_item f JOIN {GOLD}.dim_cliente c ON f.sk_cliente = c.sk_cliente
WHERE f.conta_receita
""")

rec_moda = distribuicao_recompra("v_itens_moda")
rec_todos = distribuicao_recompra("v_itens_todos")
for d in (rec_moda, rec_todos):
    d["pct_clientes"] = d.clientes / d.clientes.sum()
    d["pct_receita"] = d.receita / d.receita.sum()
display(rec_moda)
display(rec_todos)

taxa_moda = 1 - rec_moda.loc[rec_moda.faixa == "1 pedido", "pct_clientes"].sum()
taxa_todos = 1 - rec_todos.loc[rec_todos.faixa == "1 pedido", "pct_clientes"].sum()
receita_recorrente = 1 - rec_moda.loc[rec_moda.faixa == "1 pedido", "pct_receita"].sum()

print("Leitura dos números (P6):")
registrar("P6", "Clientes de moda com 2 ou mais pedidos de moda", pct(taxa_moda, 2))
registrar("P6", "Clientes do marketplace com 2 ou mais pedidos (todas as categorias)", pct(taxa_todos, 2))
registrar("P6", "Receita de moda vinda de clientes recorrentes", pct(receita_recorrente, 2))
registrar("P6", "Clientes de moda com 1 único pedido", pct(1 - taxa_moda, 2))

fig, ax = plt.subplots()
ordem = ["1 pedido", "2 pedidos", "3 ou mais"]
x = range(len(ordem))
vm = [rec_moda.set_index("faixa").pct_clientes.get(o, 0) * 100 for o in ordem]
vt = [rec_todos.set_index("faixa").pct_clientes.get(o, 0) * 100 for o in ordem]
ax.bar([i - 0.2 for i in x], vm, width=0.38, color=AZUL, label="Moda")
ax.bar([i + 0.2 for i in x], vt, width=0.38, color=CINZA, label="Marketplace inteiro")
for i, (a, b) in enumerate(zip(vm, vt)):
    ax.text(i - 0.2, a, f"{a:.1f}%".replace(".", ","), ha="center", va="bottom", fontsize=9)
    ax.text(i + 0.2, b, f"{b:.1f}%".replace(".", ","), ha="center", va="bottom", fontsize=9, color=TEXTO2)
ax.set_xticks(list(x))
ax.set_xticklabels(ordem)
ax.set_ylabel("% dos clientes")
ax.set_title(f"{pct(1 - taxa_moda, 0)} dos clientes de moda compram uma única vez")
ax.legend(frameon=False)
ax.grid(axis="y", alpha=0.25)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo dos indicadores
# MAGIC Todos os números destacados acima em uma única tabela persistida (`gold.resumo_analise`). É a base para escrever a discussão no README sem se perder entre os gráficos.

# COMMAND ----------

df_resumo = spark.createDataFrame(resumo, "pergunta string, indicador string, valor string")
df_resumo.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.resumo_analise")
display(spark.table(f"{GOLD}.resumo_analise"))
