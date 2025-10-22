import pandas as pd
import time
import streamlit as st
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import sqlite3 
import os 
import math 

# --- Configuração da Página do App ---
st.set_page_config(layout="wide", page_title="Radar FII Pro V2")

# --- PARTE 1: O "ROBÔ" (SCRAPER) E O BANCO DE DADOS ---
DB_FILE = "fiis_data.db"
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
    status_placeholder = st.empty() # Cria um "espaço" para o status
    
    status_placeholder.info(f"Iniciando busca por {len(FII_LIST)} FIIs... Isso pode levar alguns minutos.")
    
    dados_fiis_lista = [] 
    
    servico = Service() 
    opcoes = webdriver.ChromeOptions()
    opcoes.add_argument('--headless') 
    opcoes.add_argument('--disable-gpu')
    opcoes.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    
    try:
        driver = webdriver.Chrome(service=servico, options=opcoes)
    except Exception as e:
        st.error(f"Erro ao iniciar o WebDriver: {e}. Verifique o chromedriver.exe.")
        return False # Retorna falha

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
    
    # Limpa a UI
    progress_bar.empty()
    status_placeholder.empty()

    if not dados_fiis_lista:
        st.error("A coleta de dados falhou. Nenhum FII foi processado.")
        return False # Retorna falha

    # Salvar no Banco de Dados
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.executemany("""
    REPLACE INTO fiis (Ticker, P_VP, DY_12M, data_coleta) 
    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, dados_fiis_lista)
    conn.commit()
    conn.close()
    
    st.success(f"Busca finalizada! {len(dados_fiis_lista)} FIIs atualizados.")
    return True # Retorna sucesso

def carregar_dados_do_db():
    """Lê os dados do banco de dados e transforma em um DataFrame Pandas."""
    if not os.path.exists(DB_FILE):
        return pd.DataFrame() # Retorna DF vazio se o DB não existe
        
    conn = sqlite3.connect(DB_FILE)
    try:
        df = pd.read_sql_query("SELECT * FROM fiis", conn)
    except pd.io.sql.DatabaseError:
        df = pd.DataFrame() # Retorna DF vazio se a tabela não existir
    conn.close()
    return df

# --- CORREÇÃO DE BUG (V2) ---
# Tornamos a função segura para tabelas vazias
def calcular_score_pro(df):
    """Calcula nosso "Molho Secreto"."""
    
    # Se a tabela estiver vazia, apenas adiciona a coluna Score e retorna
    if df.empty:
        df['Score Pro'] = pd.Series(dtype='int')
        return df

    # Filtramos FIIs "ruins" (P/VP muito alto ou DY zero)
    df_filtrado = df[(df['P_VP'] > 0) & (df['P_VP'] < 1.5) & (df['DY_12M'] > 0)].copy()
    
    # Se *nenhum* FII passar no filtro, damos Score 0 para todos
    if df_filtrado.empty:
        df['Score Pro'] = 0
        return df

    # Normalização
    df_filtrado['dy_norm'] = 100 * (df_filtrado['DY_12M'] - df_filtrado['DY_12M'].min()) / (df_filtrado['DY_12M'].max() - df_filtrado['DY_12M'].min())
    df_filtrado['pvp_norm'] = 100 * (df_filtrado['P_VP'].max() - df_filtrado['P_VP']) / (df_filtrado['P_VP'].max() - df_filtrado['P_VP'].min())
    
    # Cálculo do Score
    df_filtrado['Score Pro'] = (df_filtrado['dy_norm'] * 0.6) + (df_filtrado['pvp_norm'] * 0.4)
    
    # Juntar o Score de volta no DF original
    df_final = df.merge(df_filtrado[['Ticker', 'Score Pro']], on='Ticker', how='left')
    df_final['Score Pro'] = df_final['Score Pro'].fillna(0).astype(int)
    
    return df_final

# --- PARTE 2: O APLICATIVO WEB (STREAMLIT) ---

st.title("🛰️ Radar FII Pro (V2 - Resiliente)")
st.subheader("Encontrando as melhores oportunidades em Fundos Imobiliários")

inicializar_db()

# --- LÓGICA DE CARREGAMENTO V2 (CORRIGIDA) ---

df_base = carregar_dados_do_db()
data_atualizacao = None
dados_expirados = True

if not df_base.empty:
    try:
        # Pega a data de atualização do primeiro item (todos são atualizados juntos)
        data_atualizacao = pd.to_datetime(df_base['data_coleta'].iloc[0])
        st.caption(f"Dados atualizados em: {data_atualizacao.strftime('%d/%m/%Y às %H:%M:%S')}")
        dados_expirados = (pd.Timestamp.now() - data_atualizacao > pd.Timedelta(hours=4))
    except (KeyError, IndexError):
        df_base = pd.DataFrame() # Força o esvaziamento se os dados estiverem corrompidos

# Botão para forçar a atualização (nosso cliente PRO gosta disso)
st.sidebar.header("Controles")
if st.sidebar.button("Forçar Atualização Agora (Lento)"):
    atualizar_dados_fiis()
    df_base = carregar_dados_do_db() # Recarrega do DB

# Se os dados expiraram OU se a tabela está vazia, rodamos o robô
elif dados_expirados or df_base.empty:
    if df_base.empty:
        st.info("Banco de dados local vazio. Iniciando o robô de coleta...")
    else:
        st.info("Os dados estão desatualizados. Iniciando o robô de coleta...")
    
    atualizar_dados_fiis()
    df_base = carregar_dados_do_db() # Recarrega do DB
else:
    st.write("Dados carregados do cache local (banco de dados).")

# --- PONTO DE CHECAGEM V2 (CORRIGIDO) ---
# Se, depois de tudo, a tabela AINDA estiver vazia, nós paramos o app.
if df_base.empty:
    st.error("A coleta de dados falhou. Não há FIIs para exibir. Tente 'Forçar Atualização' ou verifique sua conexão.")
    st.stop() # <<< ESTE COMANDO IMPEDE O ERRO

# Se chegamos aqui, df_base TEM dados. Agora calculamos o score.
df_com_score = calcular_score_pro(df_base)

# --- 5. Filtros Interativos ---
st.sidebar.header("Filtros Avançados")
preco_teto_pvp = st.sidebar.slider("P/VP Máximo:", 0.5, 2.0, 1.2, 0.01)
dy_minimo = st.sidebar.slider("Dividend Yield (12M) Mínimo (%):", 0.0, 20.0, 5.0, 0.5)
score_minimo = st.sidebar.slider("Score Pro Mínimo (de 0 a 100):", 0, 100, 30, 5)

# --- 6. Lógica de Filtragem (Pandas) ---
df_filtrado = df_com_score[
    (df_com_score['P_VP'] <= preco_teto_pvp) &
    (df_com_score['DY_12M'] >= dy_minimo) &
    (df_com_score['Score Pro'] >= score_minimo)
]

# --- 7. Exibição dos Resultados ---
st.header(f"Resultados Encontrados: {len(df_filtrado)}")
colunas_para_exibir = {'Ticker': 'Ticker', 'Score Pro': 'Score Pro 🔥', 'P_VP': 'P/VP', 'DY_12M': 'DY (12M) %'}

st.dataframe(
    df_filtrado[colunas_para_exibir.keys()]
    .sort_values(by='Score Pro', ascending=False)
    .style.format({'Score Pro': '{:d} pts', 'P_VP': '{:.2f}', 'DY_12M': '{:.2f}%'})
    .map(lambda x: 'background-color: #3C5C3C' if x > 80 else '', subset=['Score Pro'])
    .hide(axis="index"),
    use_container_width=True
)

st.caption("Disclaimer: Isso não é uma recomendação de compra ou venda.")

with st.expander("Ver todos os dados brutos (antes do filtro)"):
    st.dataframe(df_com_score.sort_values(by='Score Pro', ascending=False), use_container_width=True)