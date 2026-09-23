-- Consultas do dashboard "Mídia Paga: moda no Olist" (Databricks AI/BI Dashboard).
--
-- Isto é a referência/Plano B: se o arquivo dashboard_midia_paga.lvdash.json não importar de primeira
-- (Create > Dashboard > "..." > Import dashboard from file), copie cada consulta abaixo para um novo
-- dataset do dashboard (botão "Add data" > "Create from SQL"), na ordem, e monte os gráficos indicados.
--
-- Se o seu catálogo caiu para `workspace` em vez de `mvp_moda` (ver notebook 01), troque
-- "mvp_moda" por "workspace" em todas as consultas abaixo antes de rodar (Ctrl+F / find & replace).

-- ============================================================================
-- 1) ds_kpis_moda  →  4 KPIs (widget: counter, um para cada coluna)
-- ============================================================================
WITH item_moda AS (
  SELECT f.order_id, f.preco
  FROM mvp_moda.gold.fato_vendas_item f
  JOIN mvp_moda.gold.dim_produto p ON f.sk_produto = p.sk_produto
  WHERE f.conta_receita AND p.eh_moda
),
item_todos AS (
  SELECT f.order_id, f.preco
  FROM mvp_moda.gold.fato_vendas_item f
  WHERE f.conta_receita
)
SELECT
  (SELECT COUNT(DISTINCT order_id) FROM item_moda) AS pedidos_moda,
  (SELECT CAST(SUM(preco) AS DOUBLE) FROM item_moda) AS receita_moda,
  (SELECT CAST(SUM(preco) AS DOUBLE) FROM item_moda) / (SELECT COUNT(DISTINCT order_id) FROM item_moda) AS ticket_medio_moda,
  (SELECT CAST(SUM(preco) AS DOUBLE) FROM item_moda) / (SELECT CAST(SUM(preco) AS DOUBLE) FROM item_todos) AS pct_moda_marketplace;

-- ============================================================================
-- 2) ds_receita_mensal  →  bar chart: x = ano_mes, y = receita  (P1)
-- ============================================================================
SELECT d.ano_mes,
       COUNT(DISTINCT f.order_id) AS pedidos,
       CAST(SUM(f.preco) AS DOUBLE) AS receita
FROM mvp_moda.gold.fato_vendas_item f
JOIN mvp_moda.gold.dim_produto p ON f.sk_produto = p.sk_produto
JOIN mvp_moda.gold.dim_data d ON f.sk_data_compra = d.sk_data
WHERE f.conta_receita AND p.eh_moda
  AND d.data BETWEEN '2017-01-01' AND '2018-08-31'
GROUP BY d.ano_mes
ORDER BY d.ano_mes;

-- ============================================================================
-- 3) ds_eventos  →  table (ou bar horizontal): evento_comercial x índice de receita diária  (P1)
-- ============================================================================
WITH rec AS (
  SELECT d.data, CAST(SUM(f.preco) AS DOUBLE) AS receita
  FROM mvp_moda.gold.fato_vendas_item f
  JOIN mvp_moda.gold.dim_produto p ON f.sk_produto = p.sk_produto
  JOIN mvp_moda.gold.dim_data d ON f.sk_data_compra = d.sk_data
  WHERE f.conta_receita AND p.eh_moda AND d.data BETWEEN '2017-01-01' AND '2018-08-31'
  GROUP BY d.data
),
dias AS (
  SELECT data, evento_comercial FROM mvp_moda.gold.dim_data WHERE data BETWEEN '2017-01-01' AND '2018-08-31'
),
eventos AS (
  SELECT d.evento_comercial,
         COUNT(*) AS dias,
         SUM(COALESCE(r.receita, 0)) AS receita,
         SUM(COALESCE(r.receita, 0)) / COUNT(*) AS receita_por_dia
  FROM dias d LEFT JOIN rec r ON d.data = r.data
  GROUP BY d.evento_comercial
),
base AS (
  SELECT receita_por_dia AS base_dia FROM eventos WHERE evento_comercial = 'Sem evento'
)
SELECT e.evento_comercial, e.dias, e.receita, e.receita_por_dia,
       e.receita_por_dia / b.base_dia * 100 AS indice_vs_dia_normal
FROM eventos e CROSS JOIN base b
ORDER BY indice_vs_dia_normal DESC;

-- ============================================================================
-- 4) ds_uf  →  table: todas as UFs, ordenadas por receita  (P2)
-- ============================================================================
SELECT uf, regiao, pedidos, clientes_unicos,
       CAST(receita AS DOUBLE) AS receita,
       CAST(receita_por_pedido AS DOUBLE) AS receita_por_pedido,
       frete_pct_da_receita,
       CAST(receita AS DOUBLE) / SUM(CAST(receita AS DOUBLE)) OVER () AS pct_receita
FROM mvp_moda.gold.agg_uf_moda
ORDER BY receita DESC;

-- ============================================================================
-- 5) ds_uf_top10  →  bar chart: x = uf, y = receita  (top 10, P2)
-- ============================================================================
SELECT uf, CAST(receita AS DOUBLE) AS receita
FROM mvp_moda.gold.agg_uf_moda
ORDER BY receita DESC
LIMIT 10;

-- ============================================================================
-- 6) ds_frete_regiao  →  bar chart: x = regiao, y = frete_pct_mediano_item  (P4)
-- ============================================================================
SELECT c.regiao,
       COUNT(DISTINCT f.order_id) AS pedidos,
       CAST(percentile_approx(f.frete_pct_preco, 0.5) AS DOUBLE) AS frete_pct_mediano_item
FROM mvp_moda.gold.fato_vendas_item f
JOIN mvp_moda.gold.dim_produto p ON f.sk_produto = p.sk_produto
JOIN mvp_moda.gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
WHERE f.conta_receita AND p.eh_moda
GROUP BY c.regiao
ORDER BY frete_pct_mediano_item DESC;

-- ============================================================================
-- 7) ds_segmentos  →  bar chart (x=segmento, y=receita) + table (com teto de CPA)  (P3)
-- ============================================================================
WITH seg AS (
  SELECT segmento, SUM(pedidos) AS pedidos, SUM(itens) AS itens,
         CAST(SUM(receita) AS DOUBLE) AS receita
  FROM mvp_moda.gold.agg_mensal_segmento
  WHERE ano_mes BETWEEN '2017-01' AND '2018-08' AND segmento <> 'Fora de moda'
  GROUP BY segmento
)
SELECT segmento, pedidos, itens, receita,
       receita / pedidos AS ticket_por_pedido,
       receita / SUM(receita) OVER () AS pct_receita_moda,
       (receita / pedidos) * 0.10 AS teto_cpa_10pct,
       (receita / pedidos) * 0.20 AS teto_cpa_20pct,
       (receita / pedidos) * 0.30 AS teto_cpa_30pct
FROM seg
ORDER BY receita DESC;

-- ============================================================================
-- 8) ds_frete_faixas  →  2 bar charts (x=faixa_frete_sobre_preco; y=pedidos e y=nota_media)  (P4)
-- ============================================================================
WITH p AS (
  SELECT fp.order_id, fp.nota_avaliacao, fp.entregue_no_prazo,
         CAST(fp.frete_total AS DOUBLE) / CAST(fp.receita_itens AS DOUBLE) AS frete_pct
  FROM mvp_moda.gold.fato_pedido fp
  WHERE fp.conta_receita AND fp.eh_pedido_moda AND fp.receita_itens > 0
)
SELECT CASE WHEN frete_pct < 0.10 THEN '1. até 10%'
            WHEN frete_pct < 0.25 THEN '2. de 10% a 25%'
            WHEN frete_pct < 0.50 THEN '3. de 25% a 50%'
            ELSE '4. acima de 50%' END AS faixa_frete_sobre_preco,
       COUNT(*) AS pedidos,
       AVG(nota_avaliacao) AS nota_media,
       AVG(CASE WHEN entregue_no_prazo THEN 1.0 WHEN entregue_no_prazo IS NOT NULL THEN 0.0 END) AS pct_no_prazo
FROM p GROUP BY 1 ORDER BY 1;

-- ============================================================================
-- 9) ds_pagamento  →  bar chart: x = meio_principal, y = pedidos  (P5)
-- ============================================================================
SELECT COALESCE(tipo_pagamento_principal, 'sem pagamento registrado') AS meio_principal,
       COUNT(*) AS pedidos,
       CAST(SUM(valor_pago_total) AS DOUBLE) AS valor,
       COUNT(*) / SUM(COUNT(*)) OVER () AS pct_pedidos
FROM mvp_moda.gold.fato_pedido
WHERE conta_receita AND eh_pedido_moda
GROUP BY 1
ORDER BY pedidos DESC;

-- ============================================================================
-- 10) ds_parcelas  →  bar chart: x = faixa_parcelas, y = ticket_mediano  (P5)
-- ============================================================================
SELECT CONCAT(CAST(CASE WHEN parcelas_max = 1 THEN 1 WHEN parcelas_max <= 3 THEN 2 WHEN parcelas_max <= 6 THEN 3
            WHEN parcelas_max <= 10 THEN 4 ELSE 5 END AS STRING), '. ',
       CASE WHEN parcelas_max = 1 THEN '1x (à vista)'
            WHEN parcelas_max BETWEEN 2 AND 3 THEN '2 a 3x'
            WHEN parcelas_max BETWEEN 4 AND 6 THEN '4 a 6x'
            WHEN parcelas_max BETWEEN 7 AND 10 THEN '7 a 10x'
            ELSE '11x ou mais' END) AS faixa_parcelas,
       COUNT(*) AS pedidos,
       CAST(percentile_approx(CAST(valor_pago_total AS DOUBLE), 0.5) AS DOUBLE) AS ticket_mediano
FROM mvp_moda.gold.fato_pedido
WHERE conta_receita AND eh_pedido_moda AND tipo_pagamento_principal = 'credit_card' AND parcelas_max IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- ============================================================================
-- 11) ds_recompra  →  table: universo x faixa x % de clientes  (P6)
-- ============================================================================
WITH cli_moda AS (
  SELECT c.cliente_unico_id, COUNT(DISTINCT f.order_id) AS n
  FROM mvp_moda.gold.fato_vendas_item f
  JOIN mvp_moda.gold.dim_produto p ON f.sk_produto = p.sk_produto
  JOIN mvp_moda.gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
  WHERE f.conta_receita AND p.eh_moda
  GROUP BY c.cliente_unico_id
),
cli_todos AS (
  SELECT c.cliente_unico_id, COUNT(DISTINCT f.order_id) AS n
  FROM mvp_moda.gold.fato_vendas_item f
  JOIN mvp_moda.gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
  WHERE f.conta_receita
  GROUP BY c.cliente_unico_id
),
uniao AS (
  SELECT 'Moda' AS universo,
         CASE WHEN n = 1 THEN '1 pedido' WHEN n = 2 THEN '2 pedidos' ELSE '3 ou mais' END AS faixa,
         COUNT(*) AS clientes
  FROM cli_moda GROUP BY 2
  UNION ALL
  SELECT 'Marketplace inteiro' AS universo,
         CASE WHEN n = 1 THEN '1 pedido' WHEN n = 2 THEN '2 pedidos' ELSE '3 ou mais' END AS faixa,
         COUNT(*) AS clientes
  FROM cli_todos GROUP BY 2
)
SELECT universo, faixa, clientes,
       clientes / SUM(clientes) OVER (PARTITION BY universo) AS pct_clientes
FROM uniao
ORDER BY universo, faixa;
