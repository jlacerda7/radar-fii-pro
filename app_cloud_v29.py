# --- VERSÃO DE PRODUÇÃO (CLOUD) v29 ---
# --- MOTOR API (BRAPI.DEV) - COMUNICAÇÃO DIRETA COM REQUESTS ---

import pandas as pd
import streamlit as st
import requests # <<< NOSSA FERRAMENTA DIRETA E ROBUSTA
import json
import sqlite3
import os
import math
import time
import re

st.set_page_config(layout="wide", page_title="Radar FII Pro")

# --- PARTE 1: API DIRETA E BANCO DE DADOS ---
DB_FILE = "fiis_data.db"
TAMANHO_DO_LOTE = 10
BRAPI_BASE_URL = "https://brapi.dev/api" # URL base da API

def inicializar_db():
    # ... (código idêntico)
    conn = sqlite3.connect(DB_FILE); cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS fiis (Ticker TEXT PRIMARY KEY, P_VP REAL, DY_12M REAL, data_coleta TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    conn.commit(); conn.close()

# --- FUNÇÃO ATUALIZAR_DADOS (V29 - USANDO REQUESTS DIRETAMENTE) ---
def atualizar_dados_fiis():
    status_placeholder = st.empty()
    status_placeholder.info("Conectando diretamente à API Brapi (V29)...")

    try:
        api_key = st.secrets["BRAPI_API_KEY"]
        headers = {'Authorization': f'Bearer {api_key}'} # Autenticação via Header

        # 1. Pedimos a lista de TODOS os FIIs diretamente
        list_url = f"{BRAPI_BASE_URL}/quote/list?type=fund&token={api_key}" # Incluir token aqui também por segurança
        response_list = requests.get(list_url, headers=headers, timeout=20)
        response_list.raise_for_status() # Levanta erro se status != 200
        fii_list_data = response_list.json()

        # Extraímos os tickers da resposta JSON
        fii_tickers_brutos = [item.get('stock') for item in fii_list_data.get('stocks', [])]
        regex_fii_valid = re.compile(r"^[A-Z]{4}11$")
        fii_tickers = [ticker for ticker in fii_tickers_brutos if isinstance(ticker, str) and regex_fii_valid.match(ticker)]

        if not fii_tickers:
            st.error("API Brapi não retornou nenhuma lista de FIIs válidos via requests.")
            return False

        status_placeholder.info(f"Lista de {len(fii_tickers)} FIIs válidos recebida. Fatiando em lotes de {TAMANHO_DO_LOTE}...")
        lotes_de_fiis = [fii_tickers[i:i + TAMANHO_DO_LOTE] for i in range(0, len(fii_tickers), TAMANHO_DO_LOTE)]

        todos_os_dados_processados = [] # Lista para guardar os dados já processados
        progress_bar = st.progress(0)
        erros_lote = 0

        # 2. Fazemos um loop e pedimos os dados de CADA LOTE diretamente
        for i, lote in enumerate(lotes_de_fiis):
            lote_bem_sucedido = False
            try:
                lote_limpo = [str(t).strip() for t in lote if isinstance(t, str)]
                if not lote_limpo: continue

                # Montamos a URL para buscar o lote com o módulo
                tickers_param = ",".join(lote_limpo)
                quote_url = f"{BRAPI_BASE_URL}/quote/{tickers_param}?modules=defaultKeyStatistics&token={api_key}"

                response_quote = requests.get(quote_url, headers=headers, timeout=30) # Timeout maior para múltiplos tickers
                response_quote.raise_for_status() # Levanta erro se status != 200
                quote_data = response_quote.json()

                # --- PARTE 3: PROCESSAMENTO DIRETO DO JSON (V29) ---
                if 'results' in quote_data:
                    for fii_result in quote_data['results']:
                        ticker = fii_result.get('symbol') # API usa 'symbol' aqui
                        stats_module = fii_result.get('defaultKeyStatistics') # Módulo é um sub-dicionário

                        if ticker and isinstance(stats_module, dict): # Verifica se é um dicionário
                            pvp_direto = stats_module.get('priceToBook')
                            dy_decimal = stats_module.get('dividendYield')

                            if pvp_direto is not None and isinstance(pvp_direto, (int, float)) and pvp_direto > 0:
                                pvp = float(pvp_direto)
                                dy = (float(dy_decimal) * 100) if dy_decimal is not None and isinstance(dy_decimal, (int, float)) else 0.0

                                # Adiciona diretamente à lista final
                                todos_os_dados_processados.append((ticker, pvp, dy))
                        else:
                            print(f"[AVISO V29] FII {ticker}: Módulo 'defaultKeyStatistics' ausente ou inválido.")
                    lote_bem_sucedido = True
                else:
                     print(f"[ERRO V29] Lote {i+1}: Chave 'results' não encontrada na resposta JSON.")
                     erros_lote += 1


            except requests.exceptions.HTTPError as http_err:
                erros_lote += 1
                status_code = http_err.response.status_code if http_err.response else 'N/A'
                st.warning(f"Lote {i+1} ({lote_limpo}) falhou com erro HTTP {status_code}. Pulando lote.")
                print(f"[AVISO V29] Lote {i+1} erro HTTP {status_code}: {lote_limpo}. Erro: {http_err}")
            except json.JSONDecodeError as json_err:
                erros_lote += 1
                st.warning(f"Lote {i+1} ({lote_limpo}) retornou JSON inválido. Pulando lote. Erro: {json_err}")
                print(f"[ERRO V29] JSON inválido lote {i+1}: {lote_limpo}. Erro: {json_err}")
            except Exception as e_lote:
                erros_lote += 1
                st.warning(f"Falha genérica ao buscar/processar o lote {i+1} ({lote_limpo}). Erro: {e_lote}. Pulando lote.")
                print(f"[ERRO V29] Falha genérica lote {i+1}: {lote_limpo}. Erro: {e_lote}")

            percentual = (i + 1) / len(lotes_de_fiis)
            status_texto = f"Buscando Lote {i+1}/{len(lotes_de_fiis)}..."
            if not lote_bem_sucedido: status_texto += " [ERRO]"
            progress_bar.progress(percentual, text=status_texto)
            time.sleep(0.1) # Pausa continua sendo boa prática


        progress_bar.empty()
        status_placeholder.info(f"Todos os {len(lotes_de_fiis)} lotes foram processados ({erros_lote} falharam). Dados válidos coletados: {len(todos_os_dados_processados)}.")

        # --- PARTE 4: VERIFICAÇÃO PÓS-PROCESSAMENTO ---
        if not todos_os_dados_processados:
             st.error("Nenhum FII com dados válidos (P/VP e DY) foi processado com sucesso.")
             print("[ERRO V29] Lista 'todos_os_dados_processados' está vazia.")
             return False # Indica falha na função

    except requests.exceptions.RequestException as req_err:
        st.error(f"Erro de conexão ao tentar acessar a API Brapi: {req_err}")
        print(f"Erro CRÍTICO V29 (Conexão): {req_err}")
        return False
    except Exception as e:
        st.error(f"Erro CRÍTICO inesperado durante a execução da coleta: {e}")
        print(f"Erro CRÍTICO V29: {e}")
        return False

    status_placeholder.empty()

    # 5. Salva no Banco de Dados
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.executemany("""
    REPLACE INTO fiis (Ticker, P_VP, DY_12M, data_coleta)
    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, todos_os_dados_processados)
    conn.commit()
    conn.close()

    st.success(f"Busca finalizada! {len(todos_os_dados_processados)} FIIs com dados válidos (P/VP e DY) foram atualizados via API direta.")
    return True

# --- O RESTANTE DO CÓDIGO (PARTE 2) É IDÊNTICO AO V19/V20/V21 ---

def carregar_dados_do_db():
    # ... (código idêntico)
    if not os.path.exists(DB_FILE): return pd.DataFrame()
    conn = sqlite3.connect(DB_FILE);
    try: df = pd.read_sql_query("SELECT Ticker, P_VP, DY_12M, data_coleta FROM fiis", conn)
    except pd.io.sql.DatabaseError: df = pd.DataFrame()
    conn.close(); return df

def calcular_score_pro(df):
    # ... (código idêntico - usa P_VP e DY_12M)
    if df.empty: df['Score Pro'] = pd.Series(dtype='int'); return df
    df_filtrado = df[(df['P_VP'] > 0.1) & (df['P_VP'] < 1.5)].copy() # Permitimos DY 0 aqui
    if df_filtrado.empty: df['Score Pro'] = 0; return df
    df_filtrado['DY_12M'] = df_filtrado['DY_12M'].fillna(0) # Trata DY nulo como 0
    dy_range = df_filtrado['DY_12M'].max() - df_filtrado['DY_12M'].min()
    pvp_range = df_filtrado['P_VP'].max() - df_filtrado['P_VP'].min()
    if dy_range == 0 : df_filtrado['dy_norm'] = 50
    else: df_filtrado['dy_norm'] = 100 * (df_filtrado['DY_12M'] - df_filtrado['DY_12M'].min()) / dy_range
    if pvp_range == 0: df_filtrado['pvp_norm'] = 50
    else: df_filtrado['pvp_norm'] = 100 * (df_filtrado['P_VP'].max() - df_filtrado['P_VP']) / pvp_range
    df_filtrado['Score Pro'] = (df_filtrado['dy_norm'] * 0.6) + (df_filtrado['pvp_norm'] * 0.4)
    df_final = df.merge(df_filtrado[['Ticker', 'Score Pro']], on='Ticker', how='left')
    df_final['Score Pro'] = df_final['Score Pro'].fillna(0).astype(int)
    return df_final

st.title("🛰️ Radar FII Pro (Cloud V29 - Final)")
st.subheader("Encontrando as melhores oportunidades em Fundos Imobiliários")

inicializar_db()
df_base = carregar_dados_do_db()
data_atualizacao = None
dados_expirados = True

# ... (restante do código da UI idêntico)
if not df_base.empty:
    try: data_atualizacao = pd.to_datetime(df_base['data_coleta'].iloc[0]); st.caption(f"Dados atualizados em: {data_atualizacao.strftime('%d/%m/%Y às %H:%M:%S')}")
    except (KeyError, IndexError): df_base = pd.DataFrame() # Reseta se dados corrompidos
if data_atualizacao: dados_expirados = (pd.Timestamp.now() - data_atualizacao > pd.Timedelta(hours=1))
else: dados_expirados = True

st.sidebar.header("Controles"); update_button_pressed = st.sidebar.button("Forçar Atualização Agora (API Rápida)")
atualizacao_chamada = False
if update_button_pressed: atualizacao_chamada = atualizar_dados_fiis(); df_base = carregar_dados_do_db(); st.rerun()
elif dados_expirados or df_base.empty:
    if df_base.empty: st.info("Banco de dados local vazio. Iniciando o robô de coleta...")
    else: st.info("Os dados estão desatualizados. Iniciando o robô de coleta...")
    atualizacao_chamada = atualizar_dados_fiis(); df_base = carregar_dados_do_db()
else: st.write("Dados carregados do cache local (banco de dados).")

if df_base.empty:
    if not atualizacao_chamada: st.error("Não há dados no cache local. Tente 'Forçar Atualização'.")
    # Mensagens de erro específicas já são mostradas pela função atualizar_dados_fiis()
    st.stop()
df_com_score = calcular_score_pro(df_base)

st.sidebar.header("Filtros Avançados"); preco_teto_pvp = st.sidebar.slider("P/VP Máximo:", 0.5, 2.0, 1.2, 0.01)
dy_minimo = st.sidebar.slider("Dividend Yield (12M) Mínimo (%):", 0.0, 25.0, 0.0, 0.5) # Aumentei limite DY
score_minimo = st.sidebar.slider("Score Pro Mínimo (de 0 a 100):", 0, 100, 0, 5)

# Filtro permite DY=0, o Score Pro cuidará da ordenação
df_filtrado = df_com_score[(df_com_score['P_VP'] <= preco_teto_pvp) & (df_com_score['DY_12M'] >= dy_minimo) & (df_com_score['Score Pro'] >= score_minimo)]

st.header(f"Resultados Encontrados: {len(df_filtrado)} (de {len(df_base)} FIIs analisados)")
colunas_para_exibir = {'Ticker': 'Ticker', 'Score Pro': 'Score Pro 🔥', 'P_VP': 'P/VP', 'DY_12M': 'DY (12M) %'}
st.dataframe(df_filtrado[colunas_para_exibir.keys()].sort_values(by='Score Pro', ascending=False).style.format({'Score Pro': '{:d} pts', 'P_VP': '{:.2f}', 'DY_12M': '{:.2f}%'}).map(lambda x: 'background-color: #3C5C3C' if x > 80 else '', subset=['Score Pro']).hide(axis="index"), use_container_width=True)
st.caption("Disclaimer: Isso não é uma recomendação de compra ou venda.")
with st.expander("Ver todos os dados brutos (antes do filtro)"): st.dataframe(df_com_score.sort_values(by='Score Pro', ascending=False), use_container_width=True)