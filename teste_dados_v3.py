import requests
from bs4 import BeautifulSoup
import pandas as pd
import re # Importamos a biblioteca de "Expressões Regulares" para limpar o texto

# --- Nosso "Molho Secreto" V3 ---

lista_de_fiis = [
    "MXRF11", 
    "HGLG11", 
    "BCFF11",
    "XPML11",
    "KNCR11",
    "VISC11",
    "IRDM11"
]

print(f"Iniciando busca V3 por {len(lista_de_fiis)} FIIs no Status Invest...")

dados_fiis = []

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

for ticker in lista_de_fiis:
    try:
        url = f"https://statusinvest.com.br/fundos-imobiliarios/{ticker}"
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"   [FALHA] {ticker}: Site fora do ar ou bloqueou (Código {response.status_code})")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')

        # --- LÓGICA DE BUSCA V3 (Atualizada) ---

        pvp_final = 0.0
        dy_final = 0.0

        # 1. Encontramos TODOS os "indicadores"
        # O site agora agrupa os indicadores em divs
        indicators = soup.find_all('div', class_='indicator-container')

        for item in indicators:
            # Dentro de cada 'item', procuramos o título
            titulo_element = item.find('h3', class_='title')
            if not titulo_element:
                continue

            titulo = titulo_element.text.strip() # Limpa o texto do título

            # Procuramos o valor
            valor_element = item.find('strong', class_='value')
            if not valor_element:
                continue

            valor_str = valor_element.text.strip() # Limpa o texto do valor

            # 2. Capturamos os dados que queremos
            if 'P/VP' in titulo:
                # Limpa o valor (ex: "1,02" -> 1.02)
                pvp_final = float(valor_str.replace(",", "."))

            if 'Dividend Yield' in titulo:
                # Limpa o valor (ex: "12,05 %" -> 12.05)
                # Usamos regex para pegar SÓ os números e a vírgula
                dy_limpo = re.sub(r'[^\d,]', '', valor_str) 
                dy_final = float(dy_limpo.replace(",", "."))

        # 3. Adiciona na nossa lista
        dados_fiis.append({
            "Ticker": ticker,
            "P/VP": pvp_final,
            "DY (12M)": dy_final
        })

        print(f"   [OK] {ticker} processado. (P/VP: {pvp_final}, DY: {dy_final}%)")

    except Exception as e:
        print(f"   [ERRO] {ticker}: Erro inesperado. {e}")

# --- Organizando com Pandas ---
df = pd.DataFrame(dados_fiis)

print("\n--- RESULTADO DA ANÁLISE V3 (Correção P/VP) ---")
print(df)