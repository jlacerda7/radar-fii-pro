import yfinance as yf
import pandas as pd

# --- Nosso "Molho Secreto" Começa Aqui ---

# Lista de FIIs que queremos analisar. 
# Note o ".SA" no final, que diz ao Yahoo que é da B3 (São Paulo).
lista_de_fiis = [
    "MXRF11.SA", 
    "HGLG11.SA", 
    "BCFF11.SA",
    "XPML11.SA",
    "KNCR11.SA"
]

print(f"Iniciando busca por {len(lista_de_fiis)} FIIs...")

dados_fiis = [] # Uma lista vazia para guardar nossos resultados

# Loop: "Para cada ticker na nossa lista_de_fiis..."
for ticker_str in lista_de_fiis:
    try:
        # 1. Cria o objeto do ticker
        ticker = yf.Ticker(ticker_str)

        # 2. Busca as informações (aqui está a mágica)
        info = ticker.info

        # 3. Pegamos SÓ o que interessa para o nosso MVP
        dados = {
            "Ticker": ticker_str.replace(".SA", ""),
            "Nome": info.get('shortName'),
            "P/VP": info.get('priceToBook'),
            "DY (12M)": info.get('yield', 0) * 100 # 'yield' é o DY
        }

        # 4. Adiciona os dados encontrados na nossa lista
        dados_fiis.append(dados)
        print(f"   [OK] {ticker_str} processado.")

    except Exception as e:
        # Se der erro (ex: FII não encontrado), nós pulamos
        print(f"   [FALHA] {ticker_str}: {e}")

# --- Organizando a Bagunça com Pandas ---

# Converte a lista de dados em uma tabela bonita
df = pd.DataFrame(dados_fiis)

print("\n--- RESULTADO DA ANÁLISE (via Pandas) ---")
print(df)