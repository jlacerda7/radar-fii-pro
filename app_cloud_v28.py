# --- VERSÃO DE PRODUÇÃO (CLOUD) v28 ---
# --- MOTOR API (BRAPI.DEV) - PARSE MANUAL DA STRING defaultKeyStatistics ---

import pandas as pd
import streamlit as st
from brapi import Brapi
import sqlite3
import os
import math
import time
import re
import requests
import ast # <<< Ferramenta para "traduzir" a string

st.set_page_config(layout="wide", page_title="Radar FII Pro")

# --- PARTE 1: O "ROBÔ" (AGORA API) E O BANCO DE DADOS ---
DB_FILE = "fiis_data.db"
TAMANHO_DO_LOTE = 10

def inicializar_db():
    # ... (código idêntico)
    conn = sqlite3.connect(DB_FILE); cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS fiis (Ticker TEXT PRIMARY KEY, P_VP REAL, DY_12M REAL, data_coleta TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    conn.commit(); conn.close()

# --- FUNÇÃO ATUALIZAR_DADOS (V28 - PARSE MANUAL) ---
def atualizar_dados_fiis():
    status_placeholder = st.empty()
    status_placeholder.info("Conectando à API Brapi para buscar TODOS os FIIs (V28)...")

    try:
        api_key = st.secrets["BRAPI_API_KEY"]
        brapi = Brapi(api_key=api_key)

        fii_list_response = brapi.quote.list(type="fund")
        fii_tickers_brutos = [fii.stock for fii in fii_list_response.stocks]
        regex_fii_valid = re.compile(r"^[A-Z]{4}11$")
        fii_tickers = [ticker for ticker in fii_tickers_brutos if isinstance(ticker, str) and regex_fii_valid.match(ticker)]

        if not fii_tickers:
            st.error("API Brapi não retornou nenhuma lista de FIIs válidos.")
            return False

        status_placeholder.info(f"Lista de {len(fii_tickers)} FIIs válidos recebida. Fatiando em lotes de {TAMANHO_DO_LOTE}...")
        lotes_de_fiis = [fii_tickers[i:i + TAMANHO_DO_LOTE] for i in range(0, len(fii_tickers), TAMANHO_DO_LOTE)]

        todos_os_dados_api = [] # Renomeado para clareza
        progress_bar = st.progress(0)
        erros_lote = 0

        # 3. Fazemos um loop e pedimos os dados de CADA LOTE
        for i, lote in enumerate(lotes_de_fiis):
            lote_bem_sucedido = False
            try:
                lote_limpo = [str(t).strip() for t in lote if isinstance(t, str)]
                if not lote_limpo: continue

                # Pedimos os dados COM o módulo defaultKeyStatistics
                fiis_data_response = brapi.quote.retrieve(tickers=lote_limpo, modules="defaultKeyStatistics")
                todos_os_dados_api.extend(fiis_data_response.results) # Guarda a resposta BRUTA da API
                lote_bem_sucedido = True

            except requests.exceptions.HTTPError as http_err:
                erros_lote += 1; status_code = http_err.response.status_code if http_err.response else 'N/A'
                print(f"[AVISO V28] Lote {i+1} erro HTTP {status_code}: {lote_limpo}. Erro: {http_err}")
            except Exception as e_lote:
                erros_lote += 1
                print(f"[ERRO V28] Falha genérica lote {i+1}: {lote_limpo}. Erro: {e_lote}")

            percentual = (i + 1) / len(lotes_de_fiis)
            status_texto = f"Buscando Lote {i+1}/{len(lotes_de_fiis)}..."
            if not lote_bem_sucedido: status_texto += " [ERRO]"
            progress_bar.progress(percentual, text=status_texto)
            time.sleep(0.1)

        progress_bar.empty()
        status_placeholder.info(f"Todos os {len(lotes_de_fiis)} lotes foram processados ({erros_lote} falharam). Formatando dados coletados...")

        # --- PARTE 4: PROCESSAMENTO (LÓGICA "TRADUTOR MANUAL" V28) ---
        dados_para_db = []
        if not todos_os_dados_api:
             st.error("Nenhum dado foi coletado com sucesso após processar todos os lotes.")
             print("[ERRO V28] Lista 'todos_os_dados_api' está vazia.")
             return False

        for fii_api_result in todos_os_dados_api:
            ticker = getattr(fii_api_result, 'stock', None)
            # Pegamos a STRING do módulo
            stats_string_raw = getattr(fii_api_result, 'default_key_statistics', None)

            # Só processa se tiver ticker e a string do módulo
            if ticker and stats_string_raw:
                pvp = 0.0
                dy = 0.0
                stats_dict = {} # Dicionário onde guardaremos os dados traduzidos

                try:
                    # --- O TRADUTOR V28 ---
                    # ast.literal_eval converte a string "{'key': val}" em um dict {'key': val}
                    # Usamos str() para garantir que é uma string
                    stats_dict = ast.literal_eval(str(stats_string_raw))
                    # --- FIM DO TRADUTOR ---

                    # Agora acessamos o DICIONÁRIO traduzido com segurança
                    pvp_direto = stats_dict.get('priceToBook')
                    dy_decimal = stats_dict.get('dividendYield')

                    # Verificamos se os valores existem e são válidos
                    if pvp_direto is not None and isinstance(pvp_direto, (int, float)) and pvp_direto > 0:
                        pvp = float(pvp_direto)

                        # DY pode ser None, tratamos isso
                        if dy_decimal is not None and isinstance(dy_decimal, (int, float)):
                             dy = float(dy_decimal) * 100 # Converte para percentual
                        else:
                             dy = 0.0 # Se for None ou inválido, considera 0

                        # Adiciona ao DB apenas se tivermos P/VP válido (DY pode ser 0)
                        dados_para_db.append((ticker, pvp, dy))
                    else:
                         print(f"[AVISO V28] FII {ticker}: P/VP inválido ou não encontrado no dict: {pvp_direto}")

                except (ValueError, SyntaxError, TypeError) as eval_err:
                    # Se ast.literal_eval falhar (string mal formada)
                    print(f"[ERRO V28] Falha ao traduzir string 'defaultKeyStatistics' para FII {ticker}. Erro: {eval_err}")
                    print(f"String recebida: {stats_string_raw}")
                except Exception as proc_err:
                     # Captura outros erros inesperados no processamento
                    print(f"[ERRO V28] Erro inesperado ao processar FII {ticker}. Erro: {proc_err}")

            # Se não encontrar a string do módulo ou o ticker, ignora o FII.

    except Exception as e:
        st.error(f"Erro CRÍTICO durante a execução da coleta: {e}")
        print(f"Erro CRÍTICO V28: {e}")
        return False

    status_placeholder.empty()

    if not dados_para_db:
        st.error("Dados foram coletados, mas nenhum FII continha P/VP e DY válidos após o processamento.")
        print("[ERRO V28] Lista 'dados_para_db' está vazia após o processamento com ast.literal_eval.")
        return False

    # 5. Salva no Banco de Dados
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.executemany("""
    REPLACE INTO fiis (Ticker, P_VP, DY_12M, data_coleta)
    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, dados_para_db)
    conn.commit()
    conn.close()

    st.success(f"Busca finalizada! {len(dados_para_db)} FIIs com dados válidos (P/VP e DY) foram atualizados.")
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
    # Ajuste: Permitimos DY=0 no cálculo do Score, mas eles terão Score baixo
    df_filtrado = df[(df['P_VP'] > 0.1) & (df['P_VP'] < 1.5)].copy()
    if df_filtrado.empty: df['Score Pro'] = 0; return df

    # Trata caso onde DY_12M pode ser 0 ou None (fillna(0))
    df_filtrado['DY_12M'] = df_filtrado['DY_12M'].fillna(0)

    # Verifica se há variação nos dados para evitar divisão por zero
    dy_range = df_filtrado['DY_12M'].max() - df_filtrado['DY_12M'].min()
    pvp_range = df_filtrado['P_VP'].max() - df_filtrado['P_VP'].min()

    if dy_range == 0 : df_filtrado['dy_norm'] = 50 # Se todos DYs iguais, nota média
    else: df_filtrado['dy_norm'] = 100 * (df_filtrado['DY_12M'] - df_filtrado['DY_12M'].min()) / dy_range

    if pvp_range == 0: df_filtrado['pvp_norm'] = 50 # Se todos PVPs iguais, nota média
    else: df_filtrado['pvp_norm'] = 100 * (df_filtrado['P_VP'].max() - df_filtrado['P_VP']) / pvp_range

    df_filtrado['Score Pro'] = (df_filtrado['dy_norm'] * 0.6) + (df_filtrado['pvp_norm'] * 0.4)
    df_final = df.merge(df_filtrado[['Ticker', 'Score Pro']], on='Ticker', how='left')
    df_final['Score Pro'] = df_final['Score Pro'].fillna(0).astype(int)
    return df_final


st.title("🛰️ Radar FII Pro (Cloud V28 - Final)")
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
dy_minimo = st.sidebar.slider("Dividend Yield (12M) Mínimo (%):", 0.0, 20.0, 0.0, 0.5) # Mudança: Padrão 0.0 para incluir todos
score_minimo = st.sidebar.slider("Score Pro Mínimo (de 0 a 100):", 0, 100, 0, 5) # Mudança: Padrão 0 para incluir todos

# Filtro permite DY=0, o Score Pro cuidará da ordenação
df_filtrado = df_com_score[(df_com_score['P_VP'] <= preco_teto_pvp) & (df_com_score['DY_12M'] >= dy_minimo) & (df_com_score['Score Pro'] >= score_minimo)]

st.header(f"Resultados Encontrados: {len(df_filtrado)} (de {len(df_base)} FIIs analisados)")
colunas_para_exibir = {'Ticker': 'Ticker', 'Score Pro': 'Score Pro 🔥', 'P_VP': 'P/VP', 'DY_12M': 'DY (12M) %'}
st.dataframe(df_filtrado[colunas_para_exibir.keys()].sort_values(by='Score Pro', ascending=False).style.format({'Score Pro': '{:d} pts', 'P_VP': '{:.2f}', 'DY_12M': '{:.2f}%'}).map(lambda x: 'background-color: #3C5C3C' if x > 80 else '', subset=['Score Pro']).hide(axis="index"), use_container_width=True)
st.caption("Disclaimer: Isso não é uma recomendação de compra ou venda.")
with st.expander("Ver todos os dados brutos (antes do filtro)"): st.dataframe(df_com_score.sort_values(by='Score Pro', ascending=False), use_container_width=True)