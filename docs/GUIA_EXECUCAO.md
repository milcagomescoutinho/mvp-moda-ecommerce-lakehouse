# Guia de execução: do zero ao README final

Tempo estimado: **1h30 a 2h30**, a maior parte na conferência de resultados e na escrita das discussões.

> **Importante (trabalho individual).** O enunciado zera a nota de trabalhos iguais. O código e a estrutura deste repositório são o esqueleto; as **discussões, a autoavaliação, o nome e os screenshots têm que ser os seus**. Reescreva os trechos marcados como `[PREENCHER]` e `[ajuste aos seus números]` com as suas conclusões, a partir dos números que **a sua execução** produzir. Nada nesses trechos foi calculado com o dataset real.

---

## 1. Baixar os dados (Kaggle)

1. Entre em https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce (é preciso estar logada no Kaggle).
2. **Licença:** conferida na página do Kaggle em 21/09/2026: **CC BY-NC-SA 4.0**. O README já está de acordo (tire um print da caixa "License" se quiser anexar como evidência).
3. Clique em **Download** e extraia o `.zip`.
4. Você vai usar **8 arquivos**:
   `olist_customers_dataset.csv`, `olist_orders_dataset.csv`, `olist_order_items_dataset.csv`, `olist_order_payments_dataset.csv`, `olist_order_reviews_dataset.csv`, `olist_products_dataset.csv`, `olist_sellers_dataset.csv`, `product_category_name_translation.csv`.
   O `olist_geolocation_dataset.csv` **não** é usado (não precisa enviar).

## 2. Publicar o código no GitHub (repositório público)

1. No GitHub, crie um repositório **público** (ex.: `mvp-moda-ecommerce-lakehouse`).
2. (Repositório já criado: https://github.com/milcagomescoutinho/mvp-moda-ecommerce-lakehouse.) Na pasta do projeto, no terminal:
   ```bash
   git init
   git add .
   git commit -m "MVP de engenharia de dados: moda e e-commerce (Olist)"
   git branch -M main
   git remote add origin https://github.com/milcagomescoutinho/mvp-moda-ecommerce-lakehouse.git
   git push -u origin main
   ```
   (Alternativa sem terminal: no GitHub, **Add file → Upload files** e arraste as pastas.)
3. Os CSVs **não** devem ir para o GitHub: o `.gitignore` já bloqueia `*.csv`. O enunciado dispensa a entrega dos dados.

## 3. Conectar o repositório ao Databricks (Git folder)

1. No Databricks: **Workspace → Create → Git folder**.
2. Cole a URL do repositório, escolha **GitHub** como provedor e crie. (Repositório público não exige credenciais.)
3. Abra a pasta `notebooks`: os arquivos `.py` aparecem como notebooks.
4. Screenshot **`22_git_folder.png`**: a pasta com os notebooks dentro do Databricks, mostrando o vínculo com o GitHub.

## 4. Executar o pipeline

Em cada notebook, conecte a **Serverless** (canto superior direito) e use **Run all**. Se o Databricks pedir **Verify identity** para liberar a computação, faça a verificação pela própria interface.

| Ordem | Notebook | O que fazer / o que esperar |
|---|---|---|
| 1 | `01_setup_catalogo_e_volume` | Cria catálogo `mvp_moda`, schemas e o volume. **Se aparecer erro de permissão ao criar o catálogo**, o notebook usa o catálogo `workspace` sozinho e avisa na saída. A última célula ainda mostrará arquivos faltando: normal. |
| — | **Upload dos CSVs** | **Catalog → `mvp_moda` (ou `workspace`) → `bronze` → `raw_files` → Upload to this volume** e selecione os 8 CSVs. Reexecute a última célula do notebook 01: deve dizer "OK: todos os arquivos esperados estão no Volume". |
| 2 | `02_bronze_ingestao` | Grava 8 tabelas Bronze. |
| 3 | `03_qualidade_de_dados` | Mede a qualidade. Cria `silver.dq_*`. Se a célula "Categorias de moda encontradas" falhar no `assert`, veja o próximo quadro. |
| 4 | `04_silver_limpeza` | Silver e quarentena. Termina com o balanço Bronze → Silver. |
| 5 | `05_gold_modelagem` | Dimensões, fatos, agregações e chaves PK/FK. Termina com a reconciliação (deve dar `ok = true`). |
| 6 | `06_analise_perguntas` | Consultas, gráficos e `gold.resumo_analise`. |
| 7 | `07_catalogo_de_dados` | Grava o catálogo no Unity Catalog. Deve imprimir "OK: todas as tabelas e colunas estão documentadas e os tipos conferem". |

**Se o `assert` de categorias de moda falhar** ou o mapeamento não bater com o dataset: abra a tabela mostrada na célula (categorias que começam com `fashion_` ou `malas_acessorios`), e ajuste o dicionário `SEGMENTOS_MODA` em `notebooks/00_config.py`. Depois, reexecute a partir do notebook 04.

**Se o notebook 07 avisar de divergência de tipo ou de coluna** (linhas com `⚠`), atualize `notebooks/catalogo_metadados.py` para refletir a coluna/tipo real, e reexecute o 07. (Os nomes das colunas de comentário em `olist_order_reviews` são a parte mais sujeita a variação entre versões do dataset.)

Depois de qualquer ajuste, faça `git commit` e `git push`, e no Databricks use **Pull** no Git folder.

## 5. Screenshots de evidência (salvar em `docs/images/` com **estes nomes**)

O README já aponta para estes arquivos: ao salvar com o nome certo, a imagem aparece sozinha.

| Arquivo | O que capturar | Onde |
|---|---|---|
| `01_volume_com_csvs.png` | Volume `raw_files` com os 8 CSVs | Catalog → volume |
| `02_bronze_execucao.png` | Saída do notebook 02: tabelas Bronze com linhas e colunas | Notebook 02 |
| `03_catalogo_arvore.png` | Catálogo do projeto com schemas `bronze`, `silver` e `gold` e suas tabelas | Catalog Explorer |
| `04_catalogo_colunas_fato.png` | Aba **Columns** de `gold.fato_vendas_item`, com os comentários visíveis | Catalog Explorer |
| `05_catalogo_relacionamentos.png` | Chaves primárias/estrangeiras (aba de constraints/relacionamentos) de `gold.fato_vendas_item` | Catalog Explorer |
| `06_catalogo_lineage.png` | Aba **Lineage** de `gold.fato_vendas_item` (grafo até a Bronze) | Catalog Explorer |
| `07_dq_resultados.png` | Regras com `registros_com_problema > 0` | Notebook 03 |
| `08_dq_resumo_dimensao.png` | Resumo por dimensão de qualidade | Notebook 03 |
| `09_dq_outliers.png` | Tabela de outliers (IQR) | Notebook 03 |
| `10_silver_balanco.png` | Balanço Bronze → Silver | Notebook 04 |
| `11_quarentena.png` | Quarentena por tabela e motivo (nos dados reais a tabela sai **vazia**: capture mesmo assim, é a evidência de que nada violou as regras duras) | Notebook 04 |
| `12_gold_tabelas_persistidas.png` | Contagem de linhas das tabelas Gold | Notebook 05 |
| `13_reconciliacao.png` | Checagem de reconciliação Silver × Gold | Notebook 05 |
| `14_p1_mensal.png` | Gráfico de receita mensal | Notebook 06 |
| `15_p1_eventos.png` | Gráfico do índice por janela comemorativa | Notebook 06 |
| `16_p2_uf.png` | Gráfico das UFs | Notebook 06 |
| `17_p3_segmentos.png` | Gráfico dos segmentos | Notebook 06 |
| `18_p4_frete.png` | Gráficos de frete e nota | Notebook 06 |
| `19_p5_pagamento.png` | Gráficos de pagamento e parcelas | Notebook 06 |
| `20_p6_recompra.png` | Gráfico de recompra | Notebook 06 |
| `21_resumo_analise.png` | Tabela `gold.resumo_analise` | Notebook 06 |
| `22_git_folder.png` | Git folder no Databricks | Workspace |
| `23_dashboard_midia.png` | O dashboard de mídia paga aberto, com pelo menos a página de Sazonalidade visível | Dashboard (seção 5.1 abaixo) |

Dica: além dos gráficos, vale um screenshot da **saída em texto** ("Leitura dos números") de cada pergunta, que mostra os números exatos citados na discussão.

## 5.1. (Bônus) Importar o dashboard de mídia paga

1. No Databricks: **Workspace → Create → Dashboard**.
2. No dashboard vazio, clique no menu **"..."** (canto superior direito) → **Import dashboard from file** → selecione `dashboard/dashboard_midia_paga.lvdash.json` (dentro da pasta do Git folder).
3. Se a importação falhar ou vier com erro, é a variação de versão do formato de dashboard entre releases do Databricks — não precisa insistir. Abra um dashboard vazio, use **Add data → Create from SQL** e cole cada consulta de `dashboard/consultas_dashboard_midia.sql`, na ordem, criando um gráfico de barra, contador ou tabela conforme a anotação de cada bloco do arquivo.
4. **Se o catálogo do seu workspace caiu para `workspace`** (mensagem do notebook 01), abra os dois arquivos (`.lvdash.json` e `.sql`) num editor de texto e troque `mvp_moda` por `workspace` antes de importar/colar.
5. Publique o dashboard (botão **Publish**) para poder tirar o screenshot com os dados carregados, e salve como `23_dashboard_midia.png`.

## 6. Completar o README

Os números, as tabelas de resultado e as discussões de P1 a P6 já estão preenchidos com o resultado de uma execução dos notebooks sobre os CSVs reais do Olist. O que **falta e é seu**:

1. Substitua `[seu nome completo]` e `[nome da disciplina e turma]` no topo.
2. **Confira os números contra a sua execução no Databricks.** As contagens (99.441 pedidos, 112.650 itens, R$ 13.591.643,70 de receita nos itens) e os indicadores do `gold.resumo_analise` devem coincidir. Se algum diferir, use o seu. Os valores de outliers (IQR) podem variar um pouco, porque usam `percentile_approx`.
3. **Reescreva as discussões (P1 a P6 e a Discussão geral) com as suas palavras.** O enunciado zera trabalhos iguais, e o texto atual é um rascunho: mantenha os números, mude a redação e acrescente a sua experiência de mídia (o que você faria com cada achado).
4. Escreva a **Autoavaliação** (marque as caixas, comente cada objetivo) e as **Dificuldades encontradas**, com o que de fato aconteceu no seu Databricks.
5. Se você alterar `notebooks/catalogo_metadados.py`, regenere o catálogo do README com `python docs/gerar_catalogo_md.py`.
6. Confira que nenhuma imagem do README aparece quebrada no GitHub, e que a busca por `PREENCHER` e `REDIJA` não retorna nada.

---

## 7. Checklist final por critério de avaliação

| Critério (pontos) | Onde está | Conferido |
|---|---|---|
| **Objetivo (1,0)**: problema e perguntas claros, definidos antes | README → Contexto de Negócios e Perguntas | [ ] |
| **Coleta (0,5)**: como coletou, contexto e licença | README → Contexto (dados brutos e licença) e Carga dos Dados | [ ] |
| **Modelagem (2,0)**: tabelas e colunas (1,0); catálogo (1,0) | README → Modelagem e Catálogo; screenshots 03–06 | [ ] |
| **Carga e Pipeline (1,0)**: upload, scripts e ETLs | README → Carga e Pipeline; notebooks; screenshots 01, 02, 10–13, 22 | [ ] |
| **Qualidade (1,0)**: problemas, tratamento no pipeline | README → Qualidade; screenshots 07–09 | [ ] |
| **Análise (2,0)**: resposta correta (1,0) e discussão (1,0) | README → Análise P1–P6; screenshots 14–21 | [ ] |
| **Autoavaliação (0,5)** | README → Autoavaliação | [ ] |
| **Capricho (2,0)** | Índice, diagramas, tabelas, imagens legíveis, sem `[PREENCHER]` sobrando, dashboard bônus importado e com screenshot | [ ] |
| Repositório **público** no GitHub, com o código | Link do repositório | [ ] |
| Sem vídeo/áudio; sem Google Colab | Tudo no Databricks | [ ] |

## 8. Problemas comuns

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| Erro de permissão em `CREATE CATALOG` | O Free Edition pode limitar a criação de catálogos | O notebook 01 cai para o catálogo `workspace` sozinho. Se não cair, escreva `workspace` no widget `catalogo` de cada notebook. |
| `FileNotFoundException` / pasta vazia ao ler os CSVs | CSVs no lugar errado ou nome diferente | Confira o caminho `/Volumes/<catálogo>/bronze/raw_files/` e os nomes dos 8 arquivos. |
| `try_to_timestamp` ou `try_cast` não encontrado | Computação antiga (não serverless) | Use **Serverless**. |
| Colunas de avaliações desalinhadas ou texto na coluna `review_score` | Comentários com quebra de linha mal lidos | Já tratado com `multiLine` e `escape`. Se persistir, aparecerá na quarentena, com o motivo. |
| Aviso ⚠ no notebook 07 | O catálogo documenta uma coluna que não existe (ou o contrário) | Ajuste `catalogo_metadados.py` e reexecute o 07. |
| PK/FK "(ignorado)" no notebook 05 | Constraints já existem de uma execução anterior, ou o recurso não está disponível | Não afeta os dados. Se a aba de relacionamentos não aparecer, use a de Lineage como evidência. |
| Gráfico não aparece | Célula de gráfico não executada, ou saída recolhida | Reexecute a célula e expanda a saída. |
