import requests
from bs4 import BeautifulSoup
import pandas as pd

# --- Nosso "Molho Secreto" V2 ---

# Lista de FIIs que queremos analisar (AGORA SEM O .SA)
lista_de_fiis = [
    "MXRF11", 
    "HGLG11", 
    "BCFF11",
    "XPML11",
    "KNCR11",
    "VISC11",
    "IRDM11"
]

print(f"Iniciando busca por {len(lista_de_fiis)} FIIs no Status Invest...")

dados_fiis = [] # Lista para guardar os resultados

# IMPORTANTE: Simulamos ser um navegador para o site não nos bloquear
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Loop: "Para cada ticker na nossa lista..."
for ticker in lista_de_fiis:
    try:
        url = f"https://statusinvest.com.br/fundos-imobiliarios/{ticker}"

        # 1. "Abre" a página do FII
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"   [FALHA] {ticker}: Site fora do ar ou bloqueou (Código {response.status_code})")
            continue # Pula para o próximo FII

        # 2. "Lê" o conteúdo da página
        soup = BeautifulSoup(response.text, 'html.parser')

        # 3. Encontra os dados que queremos (esta é a parte "mágica" do scraping)

        # --- P/VP ---
        # Procuramos um <strong> com o título "P/VP"
        pvp_element = soup.find('strong', class_='value', string='P/VP')
        # Se encontrar, pegamos o <strong> "irmão" dele, que contém o valor
        pvp_valor_str = pvp_element.find_next_sibling('strong').text if pvp_element else '0'

        # --- DY (12M) ---
        # Procuramos um <h3> com o título "Dividend Yield"
        dy_element = soup.find(lambda tag: tag.name == 'h3' and 'Dividend Yield' in tag.text)
        # Se encontrar, pegamos o <strong> "irmão" dele, que contém o valor
        dy_valor_str = dy_element.find_next_sibling('strong').text if dy_element else '0'

        # 4. Limpa os dados (troca "," por "." e remove "%")
        pvp_final = float(pvp_valor_str.replace(",", "."))
        dy_final = float(dy_valor_str.replace(",", ".").replace("%", ""))

        # 5. Adiciona na nossa lista
        dados_fiis.append({
            "Ticker": ticker,
            "P/VP": pvp_final,
            "DY (12M)": dy_final
        })

        print(f"   [OK] {ticker} processado. (P/VP: {pvp_final}, DY: {dy_final}%)")

    except Exception as e:
        print(f"   [ERRO] {ticker}: Erro inesperado ao processar. {e}")

# --- Organizando com Pandas ---
df = pd.DataFrame(dados_fiis)

print("\n--- RESULTADO DA ANÁLISE V2 (via Status Invest) ---")
print(df)