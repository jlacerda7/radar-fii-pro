import requests
from bs4 import BeautifulSoup
import pandas as pd
import re # Para limpeza de texto

# --- Nosso "Molho Secreto" V4 ---

lista_de_fiis = [
    "MXRF11", 
    "HGLG11", 
    "BCFF11",
    "XPML11",
    "KNCR11",
    "VISC11",
    "IRDM11"
]

print(f"Iniciando busca V4 por {len(lista_de_fiis)} FIIs no Status Invest...")

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

        # --- LÓGICA DE BUSCA V4 (Atualizada) ---

        pvp_final = 0.0
        dy_final = 0.0

        # 1. Encontramos o "container" principal que agrupa os indicadores
        container_principal = soup.find('div', class_='indicator-container')

        # 2. Se o container existir, procuramos TODOS os "cards" de indicadores dentro dele
        if container_principal:
            cards_indicadores = container_principal.find_all('div', class_='indicator-card')

            # 3. "Para cada card encontrado..."
            for card in cards_indicadores:
                # 4. Pegamos o Título (ex: "P/VP")
                titulo_element = card.find('h3', class_='title')
                # 5. Pegamos o Valor (ex: "1,02")
                valor_element = card.find('strong', class_='value')

                if titulo_element and valor_element:
                    titulo = titulo_element.text.strip()
                    valor_str = valor_element.text.strip()

                    # 6. Verificamos se é o card que queremos
                    if 'P/VP' in titulo:
                        pvp_final = float(valor_str.replace(",", "."))

                    if 'Dividend Yield' in titulo:
                        # Limpa o valor (ex: "12,05 %" -> 12.05)
                        dy_limpo = re.sub(r'[^\d,]', '', valor_str) 
                        dy_final = float(dy_limpo.replace(",", "."))

        # 7. Adiciona na nossa lista (mesmo que tenha falhado, adiciona 0.0)
        dados_fiis.append({
            "Ticker": ticker,
            "P/VP": pvp_final,
            "DY (12M)": dy_final
        })

        if pvp_final == 0.0 or dy_final == 0.0:
             print(f"   [AVISO] {ticker} processado com dados faltantes. (P/VP: {pvp_final}, DY: {dy_final}%)")
        else:
            print(f"   [OK] {ticker} processado. (P/VP: {pvp_final}, DY: {dy_final}%)")

    except Exception as e:
        print(f"   [ERRO] {ticker}: Erro inesperado. {e}")

# --- Organizando com Pandas ---
df = pd.DataFrame(dados_fiis)

print("\n--- RESULTADO DA ANÁLISE V4 (Lógica Precisa) ---")
print(df)