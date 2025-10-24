# --- VERSÃO DE PRODUÇÃO (CLOUD) v31 ---
# --- AUTORADAR COM SCORE V3 (DY x Liq x DistMin x VarDia) ---

import pandas as pd
import streamlit as st
import requests # Comunicação direta com API
import json
import sqlite3
import os
import math
import time
import re
from typing import List, Dict, Tuple, Any # Para type hints

st.set_page_config(layout="wide", page_title="FII AutoRadar")

# --- PARTE 1: API DIRETA E BANCO DE DADOS ---
DB_FILE = "fiis_data.db"
TAMANHO_DO_LOTE = 10
BRAPI_BASE_URL = "https://brapi.dev/api"

# Cache para a lista de FIIs
@st.cache_data(ttl=3600 * 4) # Cache por 4 horas
def get_fii_tickers(api_key: str) -> List[Tuple[str, str]]:
    """Busca e retorna a lista limpa de tickers e setores de FIIs da API Brapi."""
    headers = {'Authorization': f'Bearer {api_key}'}
    list_url = f"{BRAPI_BASE_URL}/quote/list?type=fund&limit=1000&token={api_key}"
    try:
        response_list = requests.get(list_url, headers=headers, timeout=30)
        response_list.raise_for_status()
        fii_list_data = response_list.json()
        if 'stocks' not in fii_list_data: return []

        regex_fii_valid = re.compile(r"^[A-Z]{4}11$")
        valid_fiis = [(item.get('stock'), item.get('sector', "Desconhecido"))
                      for item in fii_list_data['stocks']
                      if isinstance(item.get('stock'), str) and regex_fii_valid.match(item.get('stock'))]
        print(f"[V31 Cache Miss] Lista de {len(valid_fiis)} FIIs obtida da API.")
        return valid_fiis
    except requests.exceptions.RequestException as req_err: st.error(f"Erro (Lista FIIs): {req_err}"); return []
    except Exception as e: st.error(f"Erro (Lista FIIs): {e}"); return []

def inicializar_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # V31: Schema com dados brutos para o Score V3 + P/VP (se disponível)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fiis (
        Ticker TEXT PRIMARY KEY,
        DY_12M REAL,              -- Dividend Yield (principal * 100)
        Liquidez_Diaria REAL,     -- regularMarketVolume
        Preco_Atual REAL,         -- regularMarketPrice
        Min_52_Semanas REAL,      -- fiftyTwoWeekLow
        Var_Dia_Percent REAL,     -- regularMarketChangePercent
        P_VP REAL,                -- priceToBook (do módulo, pode ser NULL)
        Setor TEXT,
        data_coleta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    # Adiciona colunas se não existirem (para migração de DBs antigos)
    cols = ['DY_12M', 'Liquidez_Diaria', 'Preco_Atual', 'Min_52_Semanas', 'Var_Dia_Percent', 'P_VP', 'Setor']
    existing_cols = [info[1] for info in cursor.execute(f"PRAGMA table_info(fiis)").fetchall()]
    for col in cols:
        if col not in existing_cols:
            try: cursor.execute(f"ALTER TABLE fiis ADD COLUMN {col} REAL")
            except: pass # Ignora erro se coluna já existir ou tipo diferente
    if 'Setor' not in existing_cols: # Setor é TEXT
         try: cursor.execute(f"ALTER TABLE fiis ADD COLUMN Setor TEXT")
         except: pass

    conn.commit()
    conn.close()

# --- FUNÇÃO ATUALIZAR_DADOS (V31 - DADOS PARA SCORE V3) ---
def atualizar_dados_fiis() -> bool:
    status_placeholder = st.empty()
    status_placeholder.info("Conectando diretamente à API Brapi (V31 - AutoRadar)...")
    dados_para_db: List[Tuple] = []

    try:
        api_key = st.secrets["BRAPI_API_KEY"]
        headers = {'Authorization': f'Bearer {api_key}'}

        # 1. Pega a lista de FIIs (Ticker, Setor)
        lista_fiis_com_setor = get_fii_tickers(api_key)
        if not lista_fiis_com_setor: return False

        fii_tickers = [item[0] for item in lista_fiis_com_setor]
        setor_map = {ticker: setor for ticker, setor in lista_fiis_com_setor}

        status_placeholder.info(f"Lista de {len(fii_tickers)} FIIs válidos recebida. Fatiando em lotes...")
        lotes_de_fiis = [fii_tickers[i:i + TAMANHO_DO_LOTE] for i in range(0, len(fii_tickers), TAMANHO_DO_LOTE)]

        todos_os_resultados_api: List[Dict] = []
        progress_bar = st.progress(0)
        erros_lote = 0

        # 2. Busca dados em lotes (com módulo defaultKeyStatistics)
        for i, lote in enumerate(lotes_de_fiis):
            lote_bem_sucedido = False
            try:
                lote_limpo = [str(t).strip() for t in lote if isinstance(t, str)]
                if not lote_limpo: continue

                tickers_param = ",".join(lote_limpo)
                quote_url = f"{BRAPI_BASE_URL}/quote/{tickers_param}?modules=defaultKeyStatistics&token={api_key}"

                response_quote = requests.get(quote_url, headers=headers, timeout=45)
                response_quote.raise_for_status()
                quote_data = response_quote.json()

                if 'results' in quote_data and quote_data['results']:
                    todos_os_resultados_api.extend(quote_data['results'])
                    lote_bem_sucedido = True
                else: erros_lote += 1; print(f"[ERRO V31] Lote {i+1}: 'results' vazio.")

            except requests.exceptions.HTTPError as http_err:
                erros_lote += 1; status_code = http_err.response.status_code if http_err.response else 'N/A'
                print(f"[AVISO V31] Lote {i+1} erro HTTP {status_code}: {lote_limpo}. Erro: {http_err}")
            except Exception as e_lote:
                erros_lote += 1; print(f"[ERRO V31] Falha genérica lote {i+1}: {lote_limpo}. Erro: {e_lote}")

            percentual = (i + 1) / len(lotes_de_fiis)
            status_texto = f"Buscando Lote {i+1}/{len(lotes_de_fiis)}..."
            if not lote_bem_sucedido: status_texto += " [ERRO]"
            progress_bar.progress(percentual, text=status_texto)
            time.sleep(0.1)

        progress_bar.empty()
        status_placeholder.info(f"Lotes processados ({erros_lote} falharam). Formatando {len(todos_os_resultados_api)} resultados...")

        # --- PARTE 3: PROCESSAMENTO (V31 - DADOS PARA SCORE V3) ---
        if not todos_os_resultados_api:
             st.error("Nenhum dado foi coletado com sucesso."); print("[ERRO V31] Lista 'todos_os_resultados_api' vazia."); return False

        for fii_result in todos_os_resultados_api:
            ticker = fii_result.get('symbol')
            if not ticker: continue # Pula se não tiver ticker

            # Dados Principais (Confiáveis)
            dy_decimal = fii_result.get('dividendYield')
            liquidez = fii_result.get('regularMarketVolume')
            preco = fii_result.get('regularMarketPrice')
            min_52w = fii_result.get('fiftyTwoWeekLow')
            var_dia = fii_result.get('regularMarketChangePercent')
            setor = setor_map.get(ticker, "Desconhecido")

            # Dados do Módulo (Menos Confiáveis para P/VP)
            pvp = None # Começa como None
            stats_module = fii_result.get('defaultKeyStatistics')
            if isinstance(stats_module, dict):
                pvp_raw = stats_module.get('priceToBook')
                if pvp_raw is not None and isinstance(pvp_raw, (int, float)) and pvp_raw > 0:
                    pvp = float(pvp_raw) # Só guarda se for válido

            # Validação Mínima: Precisa ter ticker, preço, liquidez e mín 52 semanas
            if ticker and preco and liquidez is not None and min_52w is not None and var_dia is not None:
                dy = (float(dy_decimal) * 100) if dy_decimal is not None and isinstance(dy_decimal, (int, float)) else 0.0
                liq_val = float(liquidez)
                preco_val = float(preco)
                min_52w_val = float(min_52w)
                var_dia_val = float(var_dia) # Já vem em percentual? A API sugere que sim.

                # Adiciona mesmo que P/VP seja None
                dados_para_db.append((ticker, dy, liq_val, preco_val, min_52w_val, var_dia_val, pvp, setor))

            else:
                 print(f"[AVISO V31] FII {ticker}: Dados essenciais (preço, liq, min52w, varDia) ausentes. Descartado.")

    except requests.exceptions.RequestException as req_err: st.error(f"Erro CRÍTICO (Conexão): {req_err}"); print(f"Erro CRÍTICO V31 (Conexão): {req_err}"); return False
    except Exception as e: st.error(f"Erro CRÍTICO (Coleta): {e}"); print(f"Erro CRÍTICO V31: {e}"); return False

    status_placeholder.empty()

    if not dados_para_db:
        st.error("Dados foram coletados, mas nenhum FII continha os dados mínimos necessários após o processamento."); print("[ERRO V31] Lista 'dados_para_db' vazia."); return False

    # 4. Salva no Banco de Dados
    conn = sqlite3.connect(DB_FILE); cursor = conn.cursor()
    cursor.executemany("""
    REPLACE INTO fiis (Ticker, DY_12M, Liquidez_Diaria, Preco_Atual, Min_52_Semanas, Var_Dia_Percent, P_VP, Setor, data_coleta)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, dados_para_db)
    conn.commit(); conn.close()

    st.success(f"Busca finalizada! {len(dados_para_db)} FIIs com dados válidos foram atualizados.")
    return True

# --- PARTE 2: APP WEB (SCORE V3 E NOVOS FILTROS) ---

@st.cache_data
def carregar_dados_do_db() -> pd.DataFrame:
    if not os.path.exists(DB_FILE): return pd.DataFrame()
    conn = sqlite3.connect(DB_FILE);
    try:
        # V31: Lemos todas as colunas novas
        df = pd.read_sql_query("SELECT * FROM fiis", conn)
        # Conversão de tipos e tratamento de nulos
        num_cols = ['DY_12M', 'Liquidez_Diaria', 'Preco_Atual', 'Min_52_Semanas', 'Var_Dia_Percent', 'P_VP']
        for col in num_cols:
             df[col] = pd.to_numeric(df[col], errors='coerce')
        # Mantemos apenas linhas com os dados essenciais para o Score V3
        essentials = ['Ticker', 'DY_12M', 'Liquidez_Diaria', 'Preco_Atual', 'Min_52_Semanas', 'Var_Dia_Percent']
        df.dropna(subset=essentials, inplace=True)
    except (pd.io.sql.DatabaseError, sqlite3.Error, Exception) as db_err: st.error(f"Erro DB: {db_err}"); df = pd.DataFrame()
    finally:
        if conn: conn.close()
    return df

# V31: Score Pro V3 (DY x Liq x DistMin x VarDia)
def calcular_score_pro(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or not all(col in df.columns for col in ['DY_12M', 'Liquidez_Diaria', 'Preco_Atual', 'Min_52_Semanas', 'Var_Dia_Percent']):
        df['Score Pro'] = pd.Series(dtype='int')
        return df

    df_calc = df.copy()
    df_calc['DY_12M'] = df_calc['DY_12M'].fillna(0)
    df_calc['Liquidez_Diaria'] = df_calc['Liquidez_Diaria'].fillna(0)
    # Garante que Preco_Atual e Min_52_Semanas são > 0 para evitar divisão por zero
    df_calc = df_calc[(df_calc['Preco_Atual'] > 0) & (df_calc['Min_52_Semanas'] > 0)]
    df_calc['Var_Dia_Percent'] = df_calc['Var_Dia_Percent'].fillna(0)

    if df_calc.empty:
        df['Score Pro'] = 0
        return df

    # 1. Métrica Distância da Mínima 52s (%) - Quanto menor, melhor
    # (Preco Atual - Mínima) / Mínima. Se Preço = Mínima, resultado é 0.
    df_calc['Dist_Min_52s'] = (df_calc['Preco_Atual'] - df_calc['Min_52_Semanas']) / df_calc['Min_52_Semanas']

    # Normalização (0-100) para cada componente
    def normalize(series, higher_is_better=True):
        min_val, max_val = series.min(), series.max()
        range_val = max_val - min_val
        if range_val == 0: return pd.Series(50, index=series.index) # Nota média se todos iguais
        if higher_is_better: return 100 * (series - min_val) / range_val
        else: return 100 * (max_val - series) / range_val # Inverte para "quanto menor, melhor"

    df_calc['dy_norm'] = normalize(df_calc['DY_12M'], higher_is_better=True)
    df_calc['liq_norm'] = normalize(df_calc['Liquidez_Diaria'], higher_is_better=True)
    df_calc['dist_min_norm'] = normalize(df_calc['Dist_Min_52s'], higher_is_better=False) # Menor distância é melhor
    df_calc['var_dia_norm'] = normalize(df_calc['Var_Dia_Percent'], higher_is_better=True) # Considerando que variação positiva recente é bom momento (pode ser ajustado)

    # Score Pro V3: 40% DY, 20% Liq, 30% DistMin, 10% VarDia
    df_calc['Score Pro'] = ( (df_calc['dy_norm'] * 0.4) +
                             (df_calc['liq_norm'] * 0.2) +
                             (df_calc['dist_min_norm'] * 0.3) +
                             (df_calc['var_dia_norm'] * 0.1) )

    df_calc['Score Pro'] = df_calc['Score Pro'].clip(0, 100) # Garante 0-100

    # Junta o Score de volta, mantendo P_VP (mesmo que não entre no score)
    df_final = df.merge(df_calc[['Ticker', 'Score Pro']], on='Ticker', how='left')
    df_final['Score Pro'] = df_final['Score Pro'].fillna(0).astype(int)
    return df_final

# --- Interface Streamlit ---
st.title("🛰️ FII AutoRadar (Cloud V31)")
st.subheader("Detectando oportunidades com base em DY, Liquidez, Preço e Variação")

inicializar_db()
df_base = carregar_dados_do_db() # Usa cache
data_atualizacao = None
dados_expirados = True

if not df_base.empty:
    try: data_atualizacao = pd.to_datetime(df_base['data_coleta'].iloc[0]); st.caption(f"Dados (cache) de: {data_atualizacao.strftime('%d/%m/%Y às %H:%M:%S')}")
    except: df_base = pd.DataFrame()
if data_atualizacao: dados_expirados = (pd.Timestamp.now() - data_atualizacao > pd.Timedelta(hours=4)) # Cache de 4h
else: dados_expirados = True

st.sidebar.header("Controles"); update_button_pressed = st.sidebar.button("Forçar Atualização Agora (API Rápida)")
atualizacao_bem_sucedida = False
if update_button_pressed:
    with st.spinner("Atualizando dados via API..."): atualizacao_bem_sucedida = atualizar_dados_fiis()
    if atualizacao_bem_sucedida: st.cache_data.clear(); df_base = carregar_dados_do_db(); st.rerun()
elif dados_expirados or df_base.empty:
    if df_base.empty: st.info("Cache local vazio. Buscando na API...")
    else: st.info("Cache expirado. Buscando na API...")
    with st.spinner("Atualizando dados via API..."): atualizacao_bem_sucedida = atualizar_dados_fiis()
    if atualizacao_bem_sucedida: st.cache_data.clear(); df_base = carregar_dados_do_db(); st.rerun()
    elif not df_base.empty: st.warning("Falha na atualização. Exibindo dados antigos.")
else: st.write("Dados carregados do cache local.")

if df_base.empty: st.error("Não há dados disponíveis."); st.stop()

df_com_score = calcular_score_pro(df_base)

# --- Filtros V31 ---
st.sidebar.header("Filtros")
dy_minimo = st.sidebar.slider("DY (12M) Mínimo (%):", 0.0, max(25.0, df_com_score['DY_12M'].max()), 0.0, 0.5)
min_liq = 0; max_liq = int(df_com_score['Liquidez_Diaria'].max()) if not df_com_score['Liquidez_Diaria'].empty else 1_000_000
default_liq = 100_000 if max_liq > 100_000 else min_liq
liquidez_minima = st.sidebar.slider("Liquidez Diária Mínima (R$):", min_liq, max_liq, default_liq, 50_000, format="R$ %d")
setores_disponiveis = sorted(df_com_score['Setor'].dropna().unique())
# Garante que "Desconhecido" não seja selecionado por padrão se houver outros
default_setores = [s for s in setores_disponiveis if s != "Desconhecido"] if len(setores_disponiveis) > 1 else setores_disponiveis
setores_selecionados = st.sidebar.multiselect("Setores:", options=setores_disponiveis, default=default_setores)
score_minimo = st.sidebar.slider("Score Pro Mínimo (0 a 100):", 0, 100, 0, 5)

# --- Lógica de Filtragem V31 ---
df_filtrado = df_com_score[
    (df_com_score['DY_12M'] >= dy_minimo) &
    (df_com_score['Liquidez_Diaria'] >= liquidez_minima) &
    (df_com_score['Setor'].isin(setores_selecionados)) &
    (df_com_score['Score Pro'] >= score_minimo)
]

# --- Tabela V31 ---
st.header(f"Resultados Encontrados: {len(df_filtrado)} (de {len(df_base)} FIIs no cache)")
# Adicionamos colunas relevantes para o Score V3
colunas_para_exibir = {
    'Ticker': 'Ticker', 'Score Pro': 'Score Pro 🚀', 'DY_12M': 'DY (12M)%',
    'Liquidez_Diaria': 'Liquidez Diária', 'Preco_Atual': 'Preço Atual',
    'Min_52_Semanas': 'Mín 52sem', 'Var_Dia_Percent': 'Var Dia %',
    'P_VP': 'P/VP*', 'Setor': 'Setor'
}
# Prepara o dataframe para exibição (seleciona e renomeia)
df_display = df_filtrado[list(colunas_para_exibir.keys())].copy()
df_display.rename(columns=colunas_para_exibir, inplace=True)

st.dataframe(
    df_display.sort_values(by='Score Pro 🚀', ascending=False)
    .style.format({
        'Score Pro 🚀': '{:d} pts',
        'DY (12M)%': '{:.2f}%',
        'Liquidez Diária': 'R$ {:,.0f}',
        'Preço Atual': 'R$ {:.2f}',
        'Mín 52sem': 'R$ {:.2f}',
        'Var Dia %': '{:.2f}%',
        'P/VP*': '{:.2f}' # Formata P/VP, mesmo que possa ser NaN
    }, na_rep='-') # Mostra '-' para valores Nulos (como P/VP*)
    .map(lambda x: 'background-color: #3C5C3C' if isinstance(x, (int, float)) and x > 80 else '', subset=['Score Pro 🚀'])
    .hide(axis="index"),
    use_container_width=True
)
st.caption("*P/VP é mostrado quando disponível na API, mas não entra no cálculo do Score Pro.")
with st.expander("Ver todos os dados brutos (antes do filtro)"): st.dataframe(df_com_score.sort_values(by='Score Pro', ascending=False), use_container_width=True)