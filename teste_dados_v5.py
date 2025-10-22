import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Nosso "Molho Secreto" V5 (Selenium) ---

# Define a lista de FIIs
lista_de_fiis = [
    "MXRF11", 
    "HGLG11", 
    "BCFF11",
    "XPML11",
    "KNCR11",
    "VISC11",
    "IRDM11"
]

print(f"Iniciando busca V5 (Selenium) por {len(lista_de_fiis)} FIIs...")
dados_fiis = []

# --- Configuração do Selenium ---
# Isso diz ao Selenium para usar o "chromedriver.exe" que está NA MESMA PASTA
servico = Service() 
opcoes = webdriver.ChromeOptions()
# Opcional: Faz o navegador rodar "escondido" (sem abrir a janela)
# Para depurar, comente a linha abaixo (deixe-a como #opcoes.add_argument...):
opcoes.add_argument('--headless') 
opcoes.add_argument('--disable-gpu') # Necessário para o modo headless
opcoes.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

# Inicializa o navegador
try:
    driver = webdriver.Chrome(service=servico, options=opcoes)
except Exception as e:
    print(f"\n[ERRO FATAL] Não foi possível iniciar o Selenium (WebDriver).")
    print("Verifique se o 'chromedriver.exe' está na mesma pasta do script.")
    print(f"Erro: {e}")
    exit() # Para o script

# Loop: "Para cada ticker na nossa lista..."
for ticker in lista_de_fiis:
    try:
        url = f"https://statusinvest.com.br/fundos-imobiliarios/{ticker}"
        
        # 1. "Abre" a página (o robô abre o Chrome)
        driver.get(url)
        
        # 2. "Espera" (O Pulo do Gato)
        # Espera ATÉ 10 segundos para o P/VP aparecer na tela
        # Esta é a "mágica" que faltava: esperar o JavaScript carregar
        wait = WebDriverWait(driver, 10)
        
        # XPath: O "caminho" exato para o dado na tela
        pvp_xpath = "//h3[contains(text(), 'P/VP')]/following-sibling::strong"
        dy_xpath = "//h3[contains(text(), 'Dividend Yield')]/following-sibling::strong"
        
        # 3. "Lê" os dados da tela
        pvp_element = wait.until(EC.presence_of_element_located((By.XPATH, pvp_xpath)))
        pvp_str = pvp_element.text
        
        dy_element = driver.find_element(By.XPATH, dy_xpath)
        dy_str = dy_element.text
        
        # 4. Limpa os dados
        pvp_final = float(pvp_str.replace(",", "."))
        dy_final = float(dy_str.replace(",", ".").replace("%", ""))
        
        # 5. Adiciona na nossa lista
        dados_fiis.append({
            "Ticker": ticker,
            "P/VP": pvp_final,
            "DY (12M)": dy_final
        })
        
        print(f"   [OK] {ticker} processado. (P/VP: {pvp_final}, DY: {dy_final}%)")
        
    except Exception as e:
        print(f"   [ERRO] {ticker}: Não foi possível processar. {e}")

# Fecha o navegador
driver.quit()

# --- Organizando com Pandas ---
df = pd.DataFrame(dados_fiis)
print("\n--- RESULTADO DA ANÁLISE V5 (Selenium) ---")
print(df)