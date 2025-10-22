import pandas as pd
import time
import streamlit as st
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import sqlite3 # <<< Para o banco de dados
import os # <<< Para verificar a data do arquivo
import math # <<< Para o cálculo do Score

# --- Configuração da Página do App ---
st.set_page_config(layout="wide", page_title="Radar FII Pro V1")

# --- PARTE 1: O "ROBÔ" (SCRAPER) E O BANCO DE DADOS ---
# Nome do nosso "banco de dados"
DB_FILE = "fiis_data.db"
# Lista de FIIs (podemos aumentar muito mais agora)
FII_LIST = [
    "MXRF11", "HGLG11", "BCFF11", "XPML11", "KNCR11", 
    "VISC11", "IRDM11", "BTCI11", "CPTS11", "MCCI11",
    "RECR11", "XPLG11", "BRCO11", "PVBI11", "BTLG11",
    "RBRR11", "JSRE11", "VILG11", "GGRC11", "TGAR11"
]

def inicializar_db():
    """Cria a tabela no banco de dados se ela não existir."""
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

def atualizar_dados_fiis():
    """Função do robô (Selenium) para buscar e salvar os dados no DB."""
    st.write(f"Iniciando busca por {len(FII_LIST)} FIIs... Isso pode levar alguns minutos.")
    
    dados_fiis_lista = [] # Lista de tuplas para o DB
    
    servico = Service() 
    opcoes = webdriver.ChromeOptions()
    opcoes.add_argument('--headless') 
    opcoes.add_argument('--disable-gpu')
    opcoes.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    
    try:
        driver = webdriver.Chrome(service=servico, options=opcoes)
    except Exception as e:
        st.error(f"Erro ao iniciar o WebDriver: {e}")
        return

    progress_bar = st.progress(0)
    
    for i, ticker in enumerate(FII_LIST):
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
            
            dados_fiis_lista.append((ticker, pvp_final, dy_final))
            
            progress_bar.progress((i + 1) / len(FII_LIST), text=f"Buscando {ticker}...")
            
        except Exception as e:
            print(f"[ERRO] Falha ao processar {ticker}: {e}")
            
    driver.quit()
    
    # Salvar no Banco de Dados
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # "REPLACE INTO" atualiza o FII se ele já existir (pelo Ticker)
    cursor.executemany("""
    REPLACE INTO fiis (Ticker, P_VP, DY_12M, data_coleta) 
    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, dados_fiis_lista)
    conn.commit()
    conn.close()
    
    progress_bar.empty() # Limpa a barra de progresso
    st.success(f"Busca finalizada! {len(dados_fiis_lista)} FIIs atualizados no banco de dados.")

def carregar_dados_do_db():
    """Lê os dados do banco de dados e transforma em um DataFrame Pandas."""
    conn = sqlite3.connect(DB_FILE)
    # Lê os dados da tabela 'fiis' e os coloca em um DataFrame
    df = pd.read_sql_query("SELECT * FROM fiis", conn)
    conn.close()
    return df

def calcular_score_pro(df):
    """Calcula nosso "Molho Secreto"."""
    # Filtramos FIIs "ruins" (P/VP muito alto ou DY zero)
    df_filtrado = df[(df['P_VP'] > 0) & (df['P_VP'] < 1.5) & (df['DY_12M'] > 0)].copy()

    # Normalização (coloca DY e P/VP na mesma escala 0-100)
    # Para DY: quanto maior, melhor
    df_filtrado['dy_norm'] = 100 * (df_filtrado['DY_12M'] - df_filtrado['DY_12M'].min()) / (df_filtrado['DY_12M'].max() - df_filtrado['DY_12M'].min())
    # Para P/VP: quanto menor, melhor (por isso invertemos max - valor)
    df_filtrado['pvp_norm'] = 100 * (df_filtrado['P_VP'].max() - df_filtrado['P_VP']) / (df_filtrado['P_VP'].max() - df_filtrado['P_VP'].min())
    
    # Nosso Score (60% peso no DY, 40% no P/VP)
    df_filtrado['Score Pro'] = (df_filtrado['dy_norm'] * 0.6) + (df_filtrado['pvp_norm'] * 0.4)
    
    # Juntar o Score de volta no DF original
    df_final = df.merge(df_filtrado[['Ticker', 'Score Pro']], on='Ticker', how='left')
    # Preenche FIIs que não entraram no cálculo (ex: DY=0) com Score 0
    df_final['Score Pro'] = df_final['Score Pro'].fillna(0).astype(int)
    
    return df_final

# --- PARTE 2: O APLICATIVO WEB (STREAMLIT) ---

st.title("🛰️ Radar FII Pro (V1)")
st.subheader("Encontrando as melhores oportunidades em Fundos Imobiliários")

# 1. Cria o DB (se não existir)
inicializar_db()

# 2. Verifica quando o DB foi atualizado
try:
    timestamp = os.path.getmtime(DB_FILE)
    data_atualizacao = pd.to_datetime(timestamp, unit='s')
    st.caption(f"Dados atualizados em: {data_atualizacao.strftime('%d/%m/%Y às %H:%M:%S')}")
except FileNotFoundError:
    st.warning("Banco de dados não encontrado. Iniciando busca...")
    data_atualizacao = None

# 3. Lógica de atualização
# Se o DB não existir ou tiver mais de 4 horas, roda o robô
dados_expirados = (data_atualizacao is None) or (pd.Timestamp.now() - data_atualizacao > pd.Timedelta(hours=4))

if dados_expirados:
    st.info("Os dados estão desatualizados. Iniciando o robô de coleta...")
    atualizar_dados_fiis()
else:
    st.write("Dados carregados do cache local (banco de dados).")

# 4. Carrega os dados do DB e calcula o Score
df_base = carregar_dados_do_db()
if not df_base.empty:
    df_base = calcular_score_pro(df_base)
else:
    st.error("Nenhum dado para exibir. Tente atualizar manualmente.")

# Botão para forçar a atualização (nosso cliente PRO gosta disso)
st.sidebar.header("Controles")
if st.sidebar.button("Forçar Atualização Agora (Lento)"):
    atualizar_dados_fiis()
    df_base = carregar_dados_do_db()
    if not df_base.empty:
        df_base = calcular_score_pro(df_base)

# --- 5. Filtros Interativos ---
st.sidebar.header("Filtros Avançados")

preco_teto_pvp = st.sidebar.slider(
    "P/VP Máximo:",
    min_value=0.5, max_value=2.0, value=1.2, step=0.01
)

dy_minimo = st.sidebar.slider(
    "Dividend Yield (12M) Mínimo (%):",
    min_value=0.0, max_value=20.0, value=5.0, step=0.5
)

score_minimo = st.sidebar.slider(
    "Score Pro Mínimo (de 0 a 100):",
    min_value=0, max_value=100, value=30, step=5
)

# --- 6. Lógica de Filtragem (Pandas) ---
df_filtrado = df_base[
    (df_base['P_VP'] <= preco_teto_pvp) &
    (df_base['DY_12M'] >= dy_minimo) &
    (df_base['Score Pro'] >= score_minimo)
]

# --- 7. Exibição dos Resultados ---
st.header(f"Resultados Encontrados: {len(df_filtrado)}")

# Colunas que queremos exibir e seus nomes "bonitos"
colunas_para_exibir = {
    'Ticker': 'Ticker',
    'Score Pro': 'Score Pro 🔥',
    'P_VP': 'P/VP',
    'DY_12M': 'DY (12M) %'
}

st.dataframe(
    df_filtrado[colunas_para_exibir.keys()] # Pega só as colunas que queremos
    .sort_values(by='Score Pro', ascending=False) # Ordena pelo Score
    .style.format({
        'Score Pro': '{:d} pts',
        'P_VP': '{:.2f}',
        'DY_12M': '{:.2f}%'
    })
    .map(lambda x: 'background-color: #3C5C3C' if x > 80 else '', subset=['Score Pro']) # Destaca Scores altos
    .hide(axis="index"), # Esconde o índice (0, 1, 2...)
    use_container_width=True
)

st.caption("Disclaimer: Isso não é uma recomendação de compra ou venda.")

with st.expander("Ver todos os dados brutos (antes do filtro)"):
    st.dataframe(df_base.sort_values(by='Score Pro', ascending=False), use_container_width=True)