"""Gera CSVs SINTÉTICOS no formato do dataset Olist, só para testar o código do pipeline localmente.

⚠ Estes dados são inventados. Não representam o Olist real e NUNCA devem ser usados em análise ou no README.
Eles trazem defeitos propositais (preço zero, parcelas 0, UF inválida, avaliações duplicadas, entrega antes da compra,
item órfão, CEP com 4 dígitos, peso zero, categoria nula) para comprovar que a Qualidade de Dados e a Silver os tratam.

Uso: python tests/gerar_dados_teste.py <pasta_de_saida>
"""
import csv
import datetime as dt
import os
import random
import sys

random.seed(7)
saida = sys.argv[1] if len(sys.argv) > 1 else "./raw_files"
os.makedirs(saida, exist_ok=True)


def gravar(nome, cabecalho, linhas):
    with open(os.path.join(saida, nome), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(cabecalho)
        w.writerows(linhas)


def hid(prefixo, n):
    return f"{prefixo}{n:030d}"[-32:]


UFS = ["SP"] * 8 + ["RJ"] * 3 + ["MG"] * 3 + ["RS", "PR", "SC", "BA", "PE", "DF", "GO", "CE", "AM", "PA"]

# --- customers: 300 pedidos -> ~240 consumidores (alguns recompram) ---------------------------------
clientes = []
for i in range(300):
    unico = hid("u", random.randint(1, 240))
    uf = random.choice(UFS)
    cep = str(random.randint(1000, 99999))          # sem lpad: alguns ficam com 4 dígitos
    if i == 5:
        uf = "XX"                                    # defeito: UF inválida
    cidade = "sao paulo " if i == 7 else random.choice(["campinas", "recife", "curitiba", "salvador"])  # defeito: espaço sobrando
    clientes.append([hid("c", i), unico, cep, cidade, uf])
gravar("olist_customers_dataset.csv", ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"], clientes)

# --- sellers ----------------------------------------------------------------------------------------
vend = [[hid("s", i), str(random.randint(1000, 99999)), "sao paulo", random.choice(UFS)] for i in range(20)]
gravar("olist_sellers_dataset.csv", ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"], vend)

# --- products ---------------------------------------------------------------------------------------
cats = (["fashion_roupa_feminina"] * 20 + ["fashion_calcados"] * 15 + ["fashion_bolsas_e_acessorios"] * 12 + ["fashion_roupa_masculina"] * 10 +
        ["fashion_underwear_e_moda_praia"] * 6 + ["fashion_esporte"] * 3 + ["malas_acessorios"] * 6 + ["fashion_roupa_infanto_juvenil"] * 3 +
        ["cool_stuff"] * 15 + ["pcs"] * 5 + ["cama_mesa_banho"] * 20 + ["informatica_acessorios"] * 10 + [""] * 4)
prods = []
for i, c in enumerate(cats):
    peso = 0 if i == 3 else random.randint(80, 3000)   # defeito: peso zero
    prods.append([hid("p", i), c, random.randint(20, 60), random.randint(50, 900), random.randint(1, 6), peso,
                  random.randint(10, 60), random.randint(2, 40), random.randint(8, 50)])
gravar("olist_products_dataset.csv", ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght",
                                      "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"], prods)

# --- translation (sem 'pcs' de propósito: categoria sem tradução) ---------------------------------
trad = [["fashion_roupa_feminina", "fashion_female_clothing"], ["fashion_calcados", "fashion_shoes"],
        ["fashion_bolsas_e_acessorios", "fashion_bags_accessories"], ["fashion_roupa_masculina", "fashion_male_clothing"],
        ["fashion_underwear_e_moda_praia", "fashion_underwear_beach"], ["fashion_esporte", "fashion_sport"],
        ["malas_acessorios", "luggage_accessories"], ["fashion_roupa_infanto_juvenil", "fashion_childrens_clothes"],
        ["cool_stuff", "cool_stuff"], ["cama_mesa_banho", "bed_bath_table"], ["informatica_acessorios", "computers_accessories"]]
gravar("product_category_name_translation.csv", ["product_category_name", "product_category_name_english"], trad)

# --- orders / itens / payments / reviews ------------------------------------------------------------
inicio, fim = dt.date(2017, 1, 1), dt.date(2018, 8, 31)
dias = (fim - inicio).days
pedidos, itens, pags, avs = [], [], [], []
for i in range(600):
    d = inicio + dt.timedelta(days=random.randint(0, dias))
    if i % 9 == 0:
        d = dt.date(2017, 11, random.randint(22, 26))            # pico de Black Friday
    compra = dt.datetime(d.year, d.month, d.day, random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
    status = random.choices(["delivered", "shipped", "canceled", "unavailable", "invoiced"], [88, 4, 4, 2, 2])[0]
    aprov = compra + dt.timedelta(hours=2)
    posta = compra + dt.timedelta(days=random.randint(1, 4))
    est = compra + dt.timedelta(days=random.randint(12, 28))
    entrega = compra + dt.timedelta(days=random.randint(4, 35)) if status == "delivered" else None
    if i == 11:
        entrega = compra - dt.timedelta(days=3)                   # defeito: entrega antes da compra
    fmt = lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if x else ""
    pedidos.append([hid("o", i), hid("c", i % 300), status, fmt(compra), fmt(aprov), fmt(posta), fmt(entrega), fmt(est)])

    n_it = random.choices([1, 2, 3], [70, 22, 8])[0]
    total = 0.0
    for k in range(1, n_it + 1):
        p = random.choice(prods)
        base = 40 + 60 * random.random() * (3 if "malas" in p[1] else 1)
        preco = round(random.lognormvariate(4.3, 0.6) if random.random() < 0.9 else base * 8, 2)
        if i == 20 and k == 1:
            preco = 0.0                                            # defeito: preço zero
        frete = round(random.uniform(0, 45), 2) if random.random() > 0.08 else 0.0
        total += preco + frete
        itens.append([hid("o", i), k, p[0], random.choice(vend)[0], fmt(posta), preco, frete])
    tipo = random.choices(["credit_card", "boleto", "voucher", "debit_card"], [75, 18, 4, 3])[0]
    parc = random.choice([1, 1, 2, 3, 4, 6, 10, 12]) if tipo == "credit_card" else 1
    if i == 30:
        parc = 0                                                   # defeito: parcelas zero
    pags.append([hid("o", i), 1, tipo, parc, round(total, 2)])
    if random.random() < 0.06:
        pags.append([hid("o", i), 2, "voucher", 1, round(random.uniform(5, 30), 2)])
    if random.random() < 0.9:
        nota = random.choices([1, 2, 3, 4, 5], [8, 4, 9, 25, 54])[0]
        cm = 'Produto "ótimo",\nchegou rápido' if random.random() < 0.15 else ""
        avs.append([hid("r", i), hid("o", i), nota, "", cm, fmt(compra + dt.timedelta(days=20)), fmt(compra + dt.timedelta(days=22))])
        if i in (40, 41, 42):                                      # defeito: 2ª avaliação do mesmo pedido
            avs.append([hid("r", 900 + i), hid("o", i), 2, "", "", fmt(compra + dt.timedelta(days=25)), fmt(compra + dt.timedelta(days=27))])
        if i == 43:                                                # defeito: mesmo review_id em pedido diferente
            avs.append([hid("r", i), hid("o", 44), 3, "", "", fmt(compra + dt.timedelta(days=21)), fmt(compra + dt.timedelta(days=23))])

itens.append([hid("o", 9999), 1, prods[0][0], vend[0][0], "2018-01-01 10:00:00", 99.9, 10.0])   # defeito: item de pedido inexistente

gravar("olist_orders_dataset.csv", ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
                                    "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"], pedidos)
gravar("olist_order_items_dataset.csv", ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"], itens)
gravar("olist_order_payments_dataset.csv", ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"], pags)
gravar("olist_order_reviews_dataset.csv", ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
                                           "review_creation_date", "review_answer_timestamp"], avs)
print(f"CSVs sintéticos de teste gravados em {saida}")
