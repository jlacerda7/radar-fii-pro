# --- VERSÃO DE PRODUÇÃO (CLOUD) v7 ---
# --- ROBÔ MAIS PACIENTE E COM 3 TENTATIVAS ---

import pandas as pd
import time # <<< Importamos o 'time' para as pausas
import streamlit as st
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import sqlite3 
import os 
import math 

# --- Configuração da Página do App ---
st.set_page_config(layout="wide", page_title="Radar FII Pro")

# --- PARTE 1: O "ROBÔ" (SCRAPER) E O BANCO DE DADOS ---
DB_FILE = "fiis_data.db"
FII_LIST = [
    "MXRF11", "HGLG11", "BCFF11", "XPML11", "KNCR11", 
    "VISC11", "IRDM11", "BTCI11", "CPTS11", "MCCI11",
    "RECR11", "XPLG11", "BRCO11", "PVBI11", "BTLG11",
    "RBRR11", "JSRE11", "VILG11", "GGRC11", "TGAR11"
]
MAX_TENTATIVAS = 3 # <<< Quantas vezes tentar antes de desistir

def inicializar_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fiis (
        Ticker TEXT PRIMARY KEY,
        P_VP REAL,
        DY_12M REAL,
        data_coleta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    conn.close()

@st.cache_resource(show_spinner=False)
def get_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)
    return driver

# --- FUNÇÃO ATUALIZAR_DADOS (V7 - MAIS ROBUSTA) ---
def atualizar_dados_fiis():
    status_placeholder = st.empty()
    status_placeholder.info(f"Iniciando busca por {len(FII_LIST)} FIIs... (Robô V7 com {MAX_TENTATIVAS} tentativas)")
    
    dados_fiis_lista = [] 
    
    try:
        driver = get_driver()
    except Exception as e:
        st.error(f"Erro ao iniciar o WebDriver na nuvem: {e}")
        return False

    progress_bar = st.progress(0)
    
    for i, ticker in enumerate(FII_LIST):
        ticker_data = None # Reseta o dado do ticker
        
        # --- LÓGICA DE TENTATIVAS (RETRY) ---
        for tentativa in range(MAX_TENTATIVAS):
            try:
                url = f"https://statusinvest.com.br/fundos-imobiliarios/{ticker}"
                driver.get(url)
                
                # Timeout maior: 15 segundos
                wait = WebDriverWait(driver, 15) 
                
                pvp_xpath = "//h3[contains(text(), 'P/VP')]/following-sibling::strong"
                dy_xpath = "//h3[contains(text(), 'Dividend Yield')]/following-sibling::strong"
                
                # Espera o P/VP carregar
                pvp_element = wait.until(EC.presence_of_element_located((By.XPATH, pvp_xpath)))
                pvp_str = pvp_element.text
                
                # Acha o DY (geralmente já carregou se o P/VP carregou)
                dy_element = driver.find_element(By.XPATH, dy_xpath)
                dy_str = dy_element.text
                
                pvp_final = float(pvp_str.replace(",", "."))
                dy_final = float(dy_str.replace(",", ".").replace("%", "").replace("N/A", "0"))
                
                # Se chegou aqui, deu certo!
                ticker_data = (ticker, pvp_final, dy_final)
                
                progress_bar.progress((i + 1) / len(FII_LIST), text=f"Buscando {ticker}... [OK]")
                
                time.sleep(0.5) # Pausa de 0.5s para parecer mais humano
                break # Sai do loop de tentativas (pois deu certo)
                
            except Exception as e:
                # Se falhou, registra o erro e tenta de novo
                print(f"[ERRO Tentativa {tentativa+1}/{MAX_TENTATIVAS}] Falha ao processar {ticker}: {e}")
                time.sleep(1) # Espera 1s antes de tentar de novo
        
        # Depois de 3 tentativas, verifica se o 'ticker_data' foi salvo
        if ticker_data:
            dados_fiis_lista.append(ticker_data)
        else:
            # Se 'ticker_data' ainda é None, falhou 3x
            print(f"[FALHA TOTAL] {ticker} descartado após {MAX_TENTATIVAS} tentativas.")
            progress_bar.progress((i + 1) / len(FII_LIST), text=f"Buscando {ticker}... [FALHA]")

    # --- FIM DO LOOP PRINCIPAL ---

    progress_bar.empty()
    status_placeholder.empty()

    if not dados_fiis_lista:
        st.error("A coleta de dados falhou. Nenhum FII foi processado.")
        return False

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.executemany("""
    REPLACE INTO fiis (Ticker, P_VP, DY_12M, data_coleta) 
    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, dados_fiis_lista)
    conn.commit()
    conn.close()
    
    st.success(f"Busca finalizada! {len(dados_fiis_lista)} FIIs atualizados.")
    return True

# --- O RESTANTE DO CÓDIGO (PARTE 2) É IDÊNTICO ---
# (Colado abaixo para garantir que o arquivo esteja 100% correto)

def carregar_dados_do_db():
    if not os.path.exists(DB_FILE):
        return pd.DataFrame()
    conn = sqlite3.connect(DB_FILE)
    try:
        df = pd.read_sql_query("SELECT * FROM fiis", conn)
    except pd.io.sql.DatabaseError:
        df = pd.DataFrame()
    conn.close()
    return df

def calcular_score_pro(df):
    if df.empty:
        df['Score Pro'] = pd.Series(dtype='int')
        return df
    df_filtrado = df[(df['P_VP'] > 0) & (df['P_VP'] < 1.5) & (df['DY_12M'] > 0)].copy()
    if df_filtrado.empty:
        df['Score Pro'] = 0
        return df
    if (df_filtrado['DY_12M'].max() - df_filtrado['DY_12M'].min()) == 0 or (df_filtrado['P_VP'].max() - df_filtrado['P_VP'].min()) == 0:
        df_filtrado['dy_norm'] = 50
        df_filtrado['pvp_norm'] = 50
    else:
        df_filtrado['dy_norm'] = 100 * (df_filtrado['DY_12M'] - df_filtrado['DY_12M'].min()) / (df_filtrado['DY_12M'].max() - df_filtrado['DY_12M'].min())
        df_filtrado['pvp_norm'] = 100 * (df_filtrado['P_VP'].max() - df_filtrado['P_VP']) / (df_filtrado['P_VP'].max() - df_filtrado['P_VP'].min())
    df_filtrado['Score Pro'] = (df_filtrado['dy_norm'] * 0.6) + (df_filtrado['pvp_norm'] * 0.4)
    df_final = df.merge(df_filtrado[['Ticker', 'Score Pro']], on='Ticker', how='left')
    df_final['Score Pro'] = df_final['Score Pro'].fillna(0).astype(int)
    return df_final

st.title("🛰️ Radar FII Pro (Cloud V7)") # Mudei o Título para sabermos que é o V7
st.subheader("Encontrando as melhores oportunidades em Fundos Imobiliários")

inicializar_db()
df_base = carregar_dados_do_db()
data_atualizacao = None
dados_expirados = True

if not df_base.empty:
    try:
        data_atualizacao = pd.to_datetime(df_base['data_coleta'].iloc[0])
        st.caption(f"Dados atualizados em: {data_atualizacao.strftime('%d/%m/%Y às %H:%M:%S')}")
        dados_expirados = (pd.Timestamp.now() - data_atualizacao > pd.Timedelta(hours=4))
    except (KeyError, IndexError):
        df_base = pd.DataFrame()

st.sidebar.header("Controles")
if st.sidebar.button("Forçar Atualização Agora (Lento)"):
    atualizar_dados_fiis()
    df_base = carregar_dados_do_db()
    st.experimental_rerun() 

elif dados_expirados or df_base.empty:
    if df_base.empty:
        st.info("Banco de dados local vazio. Iniciando o robô de coleta...")
    else:
        st.info("Os dados estão desatualizados. Iniciando o robô de coleta...")
    atualizar_dados_fiis()
    df_base = carregar_dados_do_db()
else:
    st.write("Dados carregados do cache local (banco de dados).")

if df_base.empty:
    st.error("A coleta de dados falhou. Não há FIIs para exibir. Tente 'Forçar Atualização'.")
    st.stop() 

df_com_score = calcular_score_pro(df_base)

st.sidebar.header("Filtros Avançados")
preco_teto_pvp = st.sidebar.slider("P/VP Máximo:", 0.5, 2.0, 1.2, 0.01)
dy_minimo = st.sidebar.slider("Dividend Yield (12M) Mínimo (%):", 0.0, 20.0, 5.0, 0.5)
score_minimo = st.sidebar.slider("Score Pro Mínimo (de 0 a 100):", 0, 100, 30, 5)

df_filtrado = df_com_score[
    (df_com_score['P_VP'] <= preco_teto_pvp) &
    (df_com_score['DY_12M'] >= dy_minimo) &
    (df_com_score['Score Pro'] >= score_minimo)
]

st.header(f"Resultados Encontrados: {len(df_filtrado)}")
colunas_para_exibir = {'Ticker': 'Ticker', 'Score Pro': 'Score Pro 🔥', 'P_VP': 'P/VP', 'DY_12M': 'DY (12M) %'}

st.dataframe(
    df_filtrado[colunas_para_exibir.keys()]
    .sort_values(by='Score Pro', ascending=False)
    .style.format({'Score Pro': '{:d} pts', 'P/VP': '{:.2f}', 'DY_12M': '{:.2f}%'})
    .map(lambda x: 'background-color: #3C5C3C' if x > 80 else '', subset=['Score Pro'])
    .hide(axis="index"),
    use_container_width=True
)

st.caption("Disclaimer: Isso não é uma recomendação de compra ou venda.")

with st.expander("Ver todos os dados brutos (antes do filtro)"):
    st.dataframe(df_com_score.sort_values(by='Score Pro', ascending=False), use_container_width=True)