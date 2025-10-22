# --- VERSÃO DE PRODUÇÃO (CLOUD) v8 ---
# --- PIVOT: FUNDAMENTUS (RÁPIDO) ---
# --- USA REQUESTS (NÃO PRECISA DE SELENIUM/CHROMIUM) ---

import pandas as pd
import time
import streamlit as st
import requests # <<< NOSSO NOVO MOTOR (RÁPIDO)
from bs4 import BeautifulSoup # <<< NOSSO NOVO LEITOR
import re
import sqlite3 
import os 
import math 

# --- Configuração da Página do App ---
st.set_page_config(layout="wide", page_title="Radar FII Pro")

# --- PARTE 1: O "ROBÔ" (SCRAPER) E O BANCO DE DADOS ---
DB_FILE = "fiis_data.db"
# URL MÁGICA: Esta URL do Fundamentus já lista TODOS os FIIs da bolsa!
URL_FUNDAMENTUS_FIIS = "https://www.fundamentus.com.br/fii_resultado.php"

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

# --- FUNÇÃO ATUALIZAR_DADOS (V8 - RÁPIDA, USANDO REQUESTS) ---
def atualizar_dados_fiis():
    status_placeholder = st.empty()
    status_placeholder.info("Iniciando busca por TODOS os FIIs no Fundamentus (Robô V8)...")
    
    try:
        # 1. Simula ser um navegador (obrigatório para o Fundamentus)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # 2. Baixa a página (instantâneo)
        response = requests.get(URL_FUNDAMENTUS_FIIS, headers=headers)
        if response.status_code != 200:
            st.error("Falha ao acessar o Fundamentus. O site pode estar fora do ar.")
            return False
            
        # 3. "Lê" a tabela de FIIs
        # O 'lxml' é um leitor de HTML super rápido
        soup = BeautifulSoup(response.text, 'lxml')
        tabela_fiis = soup.find('table', {'id': 'resultado'})
        
        if not tabela_fiis:
            st.error("Não foi possível encontrar a tabela de dados no Fundamentus.")
            return False

        # 4. Converte a tabela HTML em um DataFrame Pandas (mágica!)
        # O Pandas lê o HTML e já cria a tabela para nós
        df = pd.read_html(str(tabela_fiis), decimal=',', thousands='.')[0]
        
        # 5. Limpeza e Renomeação dos Dados
        # O Fundamentus usa nomes de colunas diferentes
        df.rename(columns={
            'Papel': 'Ticker',
            'P/VP': 'P_VP',
            'Dividend Yield': 'DY_12M'
        }, inplace=True)
        
        # Remove o símbolo '%' e converte para número
        df['DY_12M'] = df['DY_12M'].str.replace('%', '').str.replace(',', '.').astype(float)
        
        # Pega apenas os dados que nos interessam
        df_limpo = df[['Ticker', 'P_VP', 'DY_12M']].copy()
        
        # Transforma o DataFrame em uma lista de tuplas para o DB
        dados_fiis_lista = [tuple(x) for x in df_limpo.to_numpy()]

    except Exception as e:
        st.error(f"Erro inesperado ao processar dados do Fundamentus: {e}")
        return False
        
    status_placeholder.empty()

    if not dados_fiis_lista:
        st.error("A coleta de dados falhou. Nenhum FII foi processado.")
        return False

    # Salva no Banco de Dados
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.executemany("""
    REPLACE INTO fiis (Ticker, P_VP, DY_12M, data_coleta) 
    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, dados_fiis_lista)
    conn.commit()
    conn.close()
    
    st.success(f"Busca finalizada! {len(dados_fiis_lista)} FIIs (TODOS) atualizados.")
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
    # Ajustamos o filtro de P/VP, pois o Fundamentus tem dados '0'
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

st.title("🛰️ Radar FII Pro (Cloud V8 - Fundamentus)")
st.subheader("Encontrando as melhores oportunidades em Fundos Imobiliários")

inicializar_db()
df_base = carregar_dados_do_db()
data_atualizacao = None
dados_expirados = True

if not df_base.empty:
    try:
        data_atualizacao = pd.to_datetime(df_base['data_coleta'].iloc[0])
        st.caption(f"Dados atualizados em: {data_atualizacao.strftime('%d/%m/%Y às %H:%M:%S')}")
        # Agora o cache dura 1 hora. Fundamentus não atualiza tão rápido.
        dados_expirados = (pd.Timestamp.now() - data_atualizacao > pd.Timedelta(hours=1)) 
    except (KeyError, IndexError):
        df_base = pd.DataFrame()

st.sidebar.header("Controles")
if st.sidebar.button("Forçar Atualização Agora (Rápido)"): # Mudei o texto!
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