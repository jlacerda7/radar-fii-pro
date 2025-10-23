# --- VERSÃO DE PRODUÇÃO (CLOUD) v13 ---
# --- MOTOR API (BRAPI.DEV) - CORREÇÃO FINAL (USA QUOTE.LIST) ---

import pandas as pd
import streamlit as st
from brapi import Brapi # <<< NOSSO MOTOR API
import sqlite3 
import os 
import math 

st.set_page_config(layout="wide", page_title="Radar FII Pro")

# --- PARTE 1: O "ROBÔ" (AGORA API) E O BANCO DE DADOS ---
DB_FILE = "fiis_data.db"

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

# --- FUNÇÃO ATUALIZAR_DADOS (V13 - CORRIGIDA) ---
def atualizar_dados_fiis():
    status_placeholder = st.empty()
    status_placeholder.info("Conectando à API Brapi para buscar TODOS os FIIs (V13)...")
    
    try:
        api_key = st.secrets["BRAPI_API_KEY"]
        brapi = Brapi(api_key=api_key)
        
        # --- ESTA É A CORREÇÃO (V13) ---
        # 1. Pedimos a lista de TODOS os FIIs (fund)
        fii_list_response = brapi.quote.list(type="fund")
        
        # 2. Extraímos os tickers (ex: "MXRF11") da resposta
        # A API Brapi chama a lista de 'stocks', mesmo sendo FIIs
        fii_tickers = [fii.stock for fii in fii_list_response.stocks]
        # --- FIM DA CORREÇÃO ---
        
        if not fii_tickers:
            st.error("API Brapi não retornou nenhuma lista de FIIs.")
            return False
            
        status_placeholder.info(f"Buscando dados de {len(fii_tickers)} FIIs...")
        
        # 3. Buscamos os dados detalhados de todos os FIIs
        # A biblioteca 'brapi' cuida de buscar em lotes
        fiis_data = brapi.get_stocks(stock=fii_tickers)
        
        dados_para_db = []
        
        # 4. Processamos os dados
        for fii in fiis_data:
            pvp = fii.regular_market_price / fii.book_value if fii.book_value and fii.book_value > 0 else 0
            dy = fii.dividend_yield * 100 if fii.dividend_yield else 0
            ticker = fii.stock
            
            # Só salvamos dados válidos
            if pvp > 0 and dy > 0: 
                dados_para_db.append((ticker, pvp, dy))
        
    except Exception as e:
        st.error(f"Erro ao conectar ou processar dados da API Brapi: {e}")
        print(f"Erro V13: {e}") # Log para nós
        return False
        
    status_placeholder.empty()

    if not dados_para_db:
        st.error("A coleta de dados da API falhou. Nenhum FII foi processado.")
        return False

    # Salva no Banco de Dados
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.executemany("""
    REPLACE INTO fiis (Ticker, P_VP, DY_12M, data_coleta) 
    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, dados_para_db)
    conn.commit()
    conn.close()
    
    st.success(f"Busca finalizada! {len(dados_para_db)} FIIs (TODOS) atualizados via API.")
    return True

# --- O RESTANTE DO CÓDIGO (PARTE 2) É IDÊNTICO ---

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
    df_filtrado = df[(df['P_VP'] > 0.1) & (df['P_VP'] < 1.5) & (df['DY_12M'] > 0)].copy()
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

st.title("🛰️ Radar FII Pro (Cloud V13 - API Engine)")
st.subheader("Encontrando as melhores oportunidades em Fundos Imobiliários")

inicializar_db()
df_base = carregar_dados_do_db()
data_atualizacao = None
dados_expirados = True

if not df_base.empty:
    try:
        data_atualizacao = pd.to_datetime(df_base['data_coleta'].iloc[0])
        st.caption(f"Dados atualizados em: {data_atualizacao.strftime('%d/%m/%Y às %H:%M:%S')}")
        dados_expirados = (pd.Timestamp.now() - data_atualizacao > pd.Timedelta(hours=1)) 
    except (KeyError, IndexError):
        df_base = pd.DataFrame()

st.sidebar.header("Controles")
if st.sidebar.button("Forçar Atualização Agora (API Rápida)"): 
    atualizar_dados_fiis()
    df_base = carregar_dados_do_db()
    st.rerun()

elif dados_expirados or df_base.empty:
    if df_base.empty:
        st.info("Banco de dados local vazio. Iniciando o robô de coleta...")
    else:
        st.info("Os dados estão desatualizados. Iniciando o robO de coleta...")
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

st.header(f"Resultados Encontrados: {len(df_filtrado)} (de {len(df_base)} FIIs analisados)")
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