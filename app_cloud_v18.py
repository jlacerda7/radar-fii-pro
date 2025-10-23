# --- VERSÃO DE PRODUÇÃO (CLOUD) v18 ---
# --- PIVOT DE INDICADOR: Score Pro V2 (DY x MarketCap) ---

import pandas as pd
import streamlit as st
from brapi import Brapi # <<< NOSSO MOTOR API
import sqlite3 
import os 
import math 
import time 

st.set_page_config(layout="wide", page_title="Radar FII Pro")

# --- PARTE 1: O "ROBÔ" (AGORA API) E O BANCO DE DADOS ---
DB_FILE = "fiis_data.db"
TAMANHO_DO_LOTE = 10 

def inicializar_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # PIVOT V18: Mudamos a coluna 'P_VP' para 'Market_Cap'
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fiis (
        Ticker TEXT PRIMARY KEY,
        Market_Cap REAL, 
        DY_12M REAL,
        data_coleta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    conn.close()

# --- FUNÇÃO ATUALIZAR_DADOS (V18 - Pega MarketCap) ---
def atualizar_dados_fiis():
    status_placeholder = st.empty()
    status_placeholder.info("Conectando à API Brapi para buscar TODOS os FIIs (V18)...")
    
    try:
        api_key = st.secrets["BRAPI_API_KEY"]
        brapi = Brapi(api_key=api_key)
        
        fii_list_response = brapi.quote.list(type="fund")
        fii_tickers = [fii.stock for fii in fii_list_response.stocks]
        
        if not fii_tickers:
            st.error("API Brapi não retornou nenhuma lista de FIIs.")
            return False
            
        status_placeholder.info(f"Lista de {len(fii_tickers)} FIIs recebida. Fatiando em lotes de {TAMANHO_DO_LOTE}...")
        
        lotes_de_fiis = [fii_tickers[i:i + TAMANHO_DO_LOTE] for i in range(0, len(fii_tickers), TAMANHO_DO_LOTE)]
        
        todos_os_dados = [] 
        progress_bar = st.progress(0)

        for i, lote in enumerate(lotes_de_fiis):
            try:
                fiis_data_response = brapi.quote.retrieve(tickers=lote)
                todos_os_dados.extend(fiis_data_response.results)
                percentual = (i + 1) / len(lotes_de_fiis)
                progress_bar.progress(percentual, text=f"Buscando Lote {i+1}/{len(lotes_de_fiis)}...")
                time.sleep(0.1) 
            except Exception as e_lote:
                st.warning(f"Falha ao buscar o lote {i+1} ({lote}). Erro: {e_lote}")
        
        progress_bar.empty()
        status_placeholder.info("Todos os lotes foram processados. Formatando dados...")

        # --- PARTE 4: PROCESSAMENTO (LÓGICA "BULLETPROOF" V18) ---
        dados_para_db = []
        for fii in todos_os_dados:
            
            # 4.1 Verificamos os atributos que REALMENTE existem
            ticker = getattr(fii, 'stock', None)
            market_cap = getattr(fii, 'marketCap', None)
            dy_val = getattr(fii, 'dividend_yield', None)
            
            # 4.2 Se os dados essenciais (Ticker, MarketCap) existirem...
            if ticker and market_cap and market_cap > 0:
                dy = dy_val * 100 if dy_val else 0 
                dados_para_db.append((ticker, market_cap, dy))
            
            # FIIs sem MarketCap ou Ticker serão ignorados.
        
    except Exception as e:
        st.error(f"Erro ao conectar ou processar dados da API Brapi: {e}")
        print(f"Erro V18: {e}")
        return False
        
    status_placeholder.empty()

    if not dados_para_db:
        st.error("A coleta de dados da API falhou ou nenhum FII retornou dados completos.")
        return False

    # 5. Salva no Banco de Dados
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # PIVOT V18: Salvando Market_Cap e DY_12M
    cursor.executemany("""
    REPLACE INTO fiis (Ticker, Market_Cap, DY_12M, data_coleta) 
    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, dados_para_db)
    conn.commit()
    conn.close()
    
    st.success(f"Busca finalizada! {len(dados_para_db)} FIIs com dados válidos foram atualizados.")
    return True

# --- PARTE 2: APP WEB (LÓGICA DO SCORE V2) ---

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

# --- PIVOT V18: NOVO CÁLCULO DE SCORE (DY x MarketCap) ---
def calcular_score_pro(df):
    if df.empty:
        df['Score Pro'] = pd.Series(dtype='int')
        return df
    
    # Filtro de qualidade: Queremos FIIs que pagam dividendos
    df_filtrado = df[df['DY_12M'] > 0].copy()
    
    if df_filtrado.empty:
        df['Score Pro'] = 0
        return df
        
    # Evita divisão por zero se todos os valores forem iguais
    if (df_filtrado['DY_12M'].max() - df_filtrado['DY_12M'].min()) == 0 or (df_filtrado['Market_Cap'].max() - df_filtrado['Market_Cap'].min()) == 0:
        df_filtrado['dy_norm'] = 50
        df_filtrado['mkt_cap_norm'] = 50
    else:
        # Nota DY (quanto maior, melhor)
        df_filtrado['dy_norm'] = 100 * (df_filtrado['DY_12M'] - df_filtrado['DY_12M'].min()) / (df_filtrado['DY_12M'].max() - df_filtrado['DY_12M'].min())
        # Nota MarketCap (quanto maior, melhor)
        df_filtrado['mkt_cap_norm'] = 100 * (df_filtrado['Market_Cap'] - df_filtrado['Market_Cap'].min()) / (df_filtrado['Market_Cap'].max() - df_filtrado['Market_Cap'].min())
    
    # Score Pro V2: 60% Renda (DY), 40% Segurança (MarketCap)
    df_filtrado['Score Pro'] = (df_filtrado['dy_norm'] * 0.6) + (df_filtrado['mkt_cap_norm'] * 0.4)
    
    df_final = df.merge(df_filtrado[['Ticker', 'Score Pro']], on='Ticker', how='left')
    df_final['Score Pro'] = df_final['Score Pro'].fillna(0).astype(int)
    return df_final

st.title("🛰️ Radar FII Pro (Cloud V18 - Score V2)")
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
        st.info("Os dados estão desatualizados. Iniciando o robô de coleta...")
    atualizar_dados_fiis()
    df_base = carregar_dados_do_db()
else:
    st.write("Dados carregados do cache local (banco de dados).")

if df_base.empty:
    st.error("A coleta de dados falhou. Não há FIIs para exibir. Tente 'Forçar Atualização'.")
    st.stop() 

df_com_score = calcular_score_pro(df_base)

# --- PIVOT V18: Novos Filtros ---
st.sidebar.header("Filtros Avançados")

# Vamos formatar o Market Cap para Bilhões/Milhões
min_mkt_cap_milhoes = math.floor(df_com_score['Market_Cap'].min() / 1_000_000)
max_mkt_cap_milhoes = math.ceil(df_com_score['Market_Cap'].max() / 1_000_000)

mkt_cap_minimo_milhoes = st.sidebar.slider(
    "Valor de Mercado Mínimo (Milhões R$):", 
    min_value=min_mkt_cap_milhoes, 
    max_value=max_mkt_cap_milhoes, 
    value=min_mkt_cap_milhoes, 
    step=100
)

dy_minimo = st.sidebar.slider("Dividend Yield (12M) Mínimo (%):", 0.0, 20.0, 5.0, 0.5)
score_minimo = st.sidebar.slider("Score Pro Mínimo (de 0 a 100):", 0, 100, 30, 5)

# --- PIVOT V18: Lógica de Filtragem ---
df_filtrado = df_com_score[
    (df_com_score['Market_Cap'] >= (mkt_cap_minimo_milhoes * 1_000_000)) &
    (df_com_score['DY_12M'] >= dy_minimo) &
    (df_com_score['Score Pro'] >= score_minimo)
]

# --- PIVOT V18: Nova Tabela de Exibição ---
st.header(f"Resultados Encontrados: {len(df_filtrado)} (de {len(df_base)} FIIs analisados)")
colunas_para_exibir = {
    'Ticker': 'Ticker', 
    'Score Pro': 'Score Pro 🔥', 
    'Market_Cap': 'Valor de Mercado', 
    'DY_12M': 'DY (12M) %'
}

st.dataframe(
    df_filtrado[colunas_para_exibir.keys()]
    .sort_values(by='Score Pro', ascending=False)
    .style.format({
        'Score Pro': '{:d} pts', 
        'Market_Cap': 'R$ {:,.0f}', # Formata como R$ 1.000.000
        'DY_12M': '{:.2f}%'
    })
    .map(lambda x: 'background-color: #3C5C3C' if x > 80 else '', subset=['Score Pro'])
    .hide(axis="index"),
    use_container_width=True
)
st.caption("Disclaimer: Isso não é uma recomendação de compra ou venda.")
with st.expander("Ver todos os dados brutos (antes do filtro)"):
    st.dataframe(
        df_com_score.sort_values(by='Score Pro', ascending=False),
        use_container_width=True
    )