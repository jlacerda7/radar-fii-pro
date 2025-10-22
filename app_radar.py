import pandas as pd
import time
import streamlit as st # <<< A GRANDE NOVIDADE
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re

# --- Configuração da Página do App ---
# Isso deve ser a PRIMEIRA linha de comando do streamlit
st.set_page_config(layout="wide", page_title="Radar FII Pro")

# --- Nosso "Molho Secreto" (O Scraper V5) ---

# @st.cache_data: Esta é a "mágica" do Streamlit.
# Ele "salva" o resultado da busca. 
# Assim, o usuário pode mexer nos filtros sem que o robô 
# (selenium) tenha que rodar TUDO de novo.
# O 'ttl=3600' diz: "guarde esses dados por 1 hora (3600s)".
@st.cache_data(ttl=3600)
def buscar_dados_fiis():
    lista_de_fiis = [
        "MXRF11", "HGLG11", "BCFF11", "XPML11", "KNCR11", 
        "VISC11", "IRDM11", "BTCI11", "CPTS11", "MCCI11",
        "RECR11", "XPLG11", "BRCO11", "PVBI11", "BTLG11"
    ] # <<< AUMENTEI A LISTA PARA O RADAR FICAR BOM

    # st.info: Mostra uma barra de status no app
    with st.spinner(f"Iniciando busca por {len(lista_de_fiis)} FIIs... Isso pode levar alguns minutos."):
        dados_fiis = []

        servico = Service() 
        opcoes = webdriver.ChromeOptions()
        opcoes.add_argument('--headless') 
        opcoes.add_argument('--disable-gpu')
        opcoes.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

        try:
            driver = webdriver.Chrome(service=servico, options=opcoes)
        except Exception as e:
            st.error("Erro ao iniciar o WebDriver. Verifique o 'chromedriver.exe'.")
            return pd.DataFrame() # Retorna uma tabela vazia

        for ticker in lista_de_fiis:
            try:
                url = f"https://statusinvest.com.br/fundos-imobiliarios/{ticker}"
                driver.get(url)

                wait = WebDriverWait(driver, 10)
                pvp_xpath = "//h3[contains(text(), 'P/VP')]/following-sibling::strong"
                dy_xpath = "//h3[contains(text(), 'Dividend Yield')]/following-sibling::strong"

                pvp_element = wait.until(EC.presence_of_element_located((By.XPATH, pvp_xpath)))
                pvp_str = pvp_element.text

                dy_element = driver.find_element(By.XPATH, dy_xpath)
                dy_str = dy_element.text

                pvp_final = float(pvp_str.replace(",", "."))
                dy_final = float(dy_str.replace(",", ".").replace("%", "").replace("N/A", "0"))

                dados_fiis.append({
                    "Ticker": ticker,
                    "P/VP": pvp_final,
                    "DY (12M)": dy_final
                })

            except Exception as e:
                # Não vamos poluir o app, mas avisamos no terminal
                print(f"[ERRO] Falha ao processar {ticker}: {e}")

        driver.quit()

        # Avisa no app que terminou
        st.success(f"Busca finalizada! {len(dados_fiis)} FIIs carregados.")
        return pd.DataFrame(dados_fiis)

# --- Início do Aplicativo Web ---
st.title("🛰️ Radar FII Pro (MVP V1)")
st.subheader("Encontrando as melhores oportunidades em Fundos Imobiliários")
st.caption("Desenvolvido por: [Seu Nome/Nome da Startup]")

# 1. Carrega os dados (usando o cache)
try:
    df_base = buscar_dados_fiis()
except Exception as e:
    st.error(f"Ocorreu um erro fatal ao buscar os dados: {e}")
    st.stop() # Para a execução do app se não houver dados

if df_base.empty:
    st.warning("Nenhum dado foi carregado. Tente recarregar a página (F5).")
    st.stop()

# --- 2. Filtros (A parte interativa) na Barra Lateral ---
st.sidebar.header("Filtros Avançados")

preco_teto_pvp = st.sidebar.slider(
    "P/VP Máximo:",
    min_value=0.5,
    max_value=2.0,
    value=1.1,  # Valor padrão
    step=0.01
)

dy_minimo = st.sidebar.slider(
    "Dividend Yield (12M) Mínimo (%):",
    min_value=0.0,
    max_value=20.0,
    value=8.0, # Valor padrão
    step=0.5
)

# --- 3. Lógica de Filtragem (Pandas) ---
df_filtrado = df_base[
    (df_base['P/VP'] <= preco_teto_pvp) &
    (df_base['DY (12M)'] >= dy_minimo)
]

# --- 4. Exibição dos Resultados ---
st.header(f"Resultados Encontrados: {len(df_filtrado)}")
st.dataframe(
    df_filtrado
    .sort_values(by='DY (12M)', ascending=False) # Ordena pelos maiores DY
    .style.format({
        'P/VP': '{:.2f}',
        'DY (12M)': '{:.2f}%'
    }),
    use_container_width=True
)

st.caption("Disclaimer: Isso não é uma recomendação de compra ou venda.")

# Expansor para ver os dados brutos
with st.expander("Ver todos os dados brutos (antes do filtro)"):
    st.dataframe(df_base.style.format({'P/VP': '{:.2f}', 'DY (12M)': '{:.2f}%'}), use_container_width=True)