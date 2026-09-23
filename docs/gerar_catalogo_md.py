"""Gera o catálogo de dados em Markdown a partir de notebooks/catalogo_metadados.py (fonte única).

Saídas:
  * docs/CATALOGO_DE_DADOS.md  → catálogo completo (Bronze, Silver e Gold)
  * README.md                  → substitui os trechos entre os marcadores
        <!-- CATALOGO_BRONZE:INICIO --> ... <!-- CATALOGO_BRONZE:FIM -->
        <!-- CATALOGO_SILVER:INICIO --> ... <!-- CATALOGO_SILVER:FIM -->
        <!-- CATALOGO_GOLD:INICIO -->   ... <!-- CATALOGO_GOLD:FIM -->

Uso (na raiz do repositório):  python docs/gerar_catalogo_md.py
"""
import os
import re

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ns = {}
exec(compile("\n".join(l for l in open(os.path.join(RAIZ, "notebooks", "catalogo_metadados.py"), encoding="utf-8").read().splitlines()
                       if not l.startswith("# MAGIC") and not l.startswith("# COMMAND")), "catalogo_metadados.py", "exec"), ns)
CATALOGO = ns["CATALOGO"]


def esc(t):
    return str(t).replace("|", "\\|").replace("\n", " ")


def tabela_md(nome, meta, nivel="####"):
    linhas = [f"{nivel} `{nome}`", "", meta["descricao"], "",
              "| Coluna | Tipo | Descrição | Domínio de valores | Linhagem |", "|---|---|---|---|---|"]
    for coluna, tipo, desc, dom, lin in meta["colunas"]:
        linhas.append(f"| `{coluna}` | `{tipo}` | {esc(desc)} | {esc(dom)} | {esc(lin)} |")
    return "\n".join(linhas) + "\n"


def camada_md(camada, nivel="####"):
    return "\n".join(tabela_md(n, m, nivel) for n, m in CATALOGO.items() if m["camada"] == camada)


def bronze_resumo():
    linhas = ["| Tabela Bronze | O que contém | Colunas (todas `string`, mais `_ingested_at` e `_source_file`) |", "|---|---|---|"]
    for nome, meta in CATALOGO.items():
        if meta["camada"] != "bronze":
            continue
        cols = ", ".join(f"`{c[0]}`" for c in meta["colunas"] if not c[0].startswith("_"))
        linhas.append(f"| `{nome}` | {esc(meta['descricao'])} | {cols} |")
    return "\n".join(linhas) + "\n"


# --- catálogo completo em docs/ ---------------------------------------------------------------------
n_tab = len(CATALOGO)
n_col = sum(len(m["colunas"]) for m in CATALOGO.values())
completo = [
    "# Catálogo de dados",
    "",
    f"Documentação de **{n_tab} tabelas e {n_col} colunas** do projeto (Bronze, Silver e Gold). "
    "Gerado automaticamente a partir de `notebooks/catalogo_metadados.py`; o mesmo conteúdo é gravado no Unity Catalog pelo notebook `07_catalogo_de_dados.py`.",
    "",
    "Para cada coluna: **tipo**, **descrição**, **domínio de valores** e **linhagem** (origem e transformação).",
    "",
    "## Camada Bronze", "", camada_md("bronze", "###"),
    "## Camada Silver", "", camada_md("silver", "###"),
    "## Camada Gold", "", camada_md("gold", "###"),
]
with open(os.path.join(RAIZ, "docs", "CATALOGO_DE_DADOS.md"), "w", encoding="utf-8") as f:
    f.write("\n".join(completo))

# --- README: substitui os trechos marcados ----------------------------------------------------------
caminho_readme = os.path.join(RAIZ, "README.md")
readme = open(caminho_readme, encoding="utf-8").read()
for marcador, conteudo in [("BRONZE", bronze_resumo()), ("SILVER", camada_md("silver")), ("GOLD", camada_md("gold"))]:
    padrao = re.compile(rf"(<!-- CATALOGO_{marcador}:INICIO -->\n).*?(<!-- CATALOGO_{marcador}:FIM -->)", re.S)
    if not padrao.search(readme):
        raise SystemExit(f"Marcador CATALOGO_{marcador} não encontrado no README.md")
    readme = padrao.sub(lambda m: m.group(1) + "\n" + conteudo + "\n" + m.group(2), readme)
with open(caminho_readme, "w", encoding="utf-8") as f:
    f.write(readme)

print(f"Catálogo gerado: {n_tab} tabelas, {n_col} colunas → docs/CATALOGO_DE_DADOS.md e README.md")
