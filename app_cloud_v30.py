# --- VERSÃO DE PRODUÇÃO (CLOUD) v30 ---
# --- REFINAMENTOS: DY CORRETO, FILTROS DE LIQUIDEZ E SETOR ---

import pandas as pd
import streamlit as st
import requests # Comunicação direta com API
import json
import sqlite3
import os
import math
import time
import re
from typing import List, Dict, Tuple, Any # Para type hints (melhora leitura)

st.set_page_config(layout="wide", page_title="Radar FII Pro")

# --- PARTE 1: API DIRETA E BANCO DE DADOS ---
DB_FILE = "fiis_data.db"
TAMANHO_DO_LOTE = 10
BRAPI_BASE_URL = "https://brapi.dev/api"

# Cache para a lista de FIIs (evita buscar toda hora)
@st.cache_data(ttl=3600 * 4) # Cache por 4 horas
def get_fii_tickers(api_key: str) -> List[str]:
    """Busca e retorna a lista limpa de tickers de FIIs da API Brapi."""
    headers = {'Authorization': f'Bearer {api_key}'}
    list_url = f"{BRAPI_BASE_URL}/quote/list?type=fund&token={api_key}"
    try:
        response_list = requests.get(list_url, headers=headers, timeout=20)
        response_list.raise_for_status()
        fii_list_data = response_list.json()

        fii_tickers_brutos = [(item.get('stock'), item.get('sector')) for item in fii_list_data.get('stocks', [])]
        regex_fii_valid = re.compile(r"^[A-Z]{4}11$")
        valid_fiis = [(ticker, sector) for ticker, sector in fii_tickers_brutos if isinstance(ticker, str) and regex_fii_valid.match(ticker)]

        print(f"[V30 Cache Miss] Lista de {len(valid_fiis)} FIIs obtida da API.")
        return valid_fiis # Retorna tuplas (Ticker, Setor)
    except requests.exceptions.RequestException as req_err:
        st.error(f"Erro de conexão ao buscar lista de FIIs: {req_err}")
        return []
    except Exception as e:
        st.error(f"Erro inesperado ao buscar lista de FIIs: {e}")
        return []

def inicializar_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # V30: Adicionamos Liquidez_Diaria e Setor
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fiis (
        Ticker TEXT PRIMARY KEY,
        P_VP REAL,
        DY_12M REAL,
        Liquidez_Diaria REAL,
        Setor TEXT,
        data_coleta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    # Opcional: Adicionar colunas se a tabela já existir de versões antigas
    try: cursor.execute("ALTER TABLE fiis ADD COLUMN Liquidez_Diaria REAL")
    except sqlite3.OperationalError: pass # Coluna já existe
    try: cursor.execute("ALTER TABLE fiis ADD COLUMN Setor TEXT")
    except sqlite3.OperationalError: pass # Coluna já existe
    conn.commit()
    conn.close()

# --- FUNÇÃO ATUALIZAR_DADOS (V30 - COM LIQUIDEZ E SETOR) ---
def atualizar_dados_fiis() -> bool:
    status_placeholder = st.empty()
    status_placeholder.info("Conectando diretamente à API Brapi (V30)...")
    dados_para_db = [] # Reinicia a lista a cada atualização

    try:
        api_key = st.secrets["BRAPI_API_KEY"]
        headers = {'Authorization': f'Bearer {api_key}'}

        # 1. Pega a lista de FIIs (Ticker, Setor) do cache ou API
        lista_fiis_com_setor = get_fii_tickers(api_key)
        if not lista_fiis_com_setor: return False # Falha se não conseguir a lista

        fii_tickers = [item[0] for item in lista_fiis_com_setor] # Apenas os tickers
        # Cria um dicionário Ticker -> Setor para consulta rápida
        setor_map = {ticker: setor for ticker, setor in lista_fiis_com_setor if setor}


        status_placeholder.info(f"Lista de {len(fii_tickers)} FIIs válidos recebida. Fatiando em lotes de {TAMANHO_DO_LOTE}...")
        lotes_de_fiis = [fii_tickers[i:i + TAMANHO_DO_LOTE] for i in range(0, len(fii_tickers), TAMANHO_DO_LOTE)]

        todos_os_dados_api = []
        progress_bar = st.progress(0)
        erros_lote = 0

        # 2. Fazemos um loop e pedimos os dados de CADA LOTE
        for i, lote in enumerate(lotes_de_fiis):
            lote_bem_sucedido = False
            try:
                lote_limpo = [str(t).strip() for t in lote if isinstance(t, str)]
                if not lote_limpo: continue

                # Pedimos os dados COM o módulo defaultKeyStatistics
                tickers_param = ",".join(lote_limpo)
                quote_url = f"{BRAPI_BASE_URL}/quote/{tickers_param}?modules=defaultKeyStatistics&token={api_key}"

                response_quote = requests.get(quote_url, headers=headers, timeout=30)
                response_quote.raise_for_status() # Levanta erro se status != 200
                quote_data = response_quote.json()

                if 'results' in quote_data:
                    todos_os_dados_api.extend(quote_data['results'])
                    lote_bem_sucedido = True
                else:
                    print(f"[ERRO V30] Lote {i+1}: Chave 'results' não encontrada na resposta JSON.")
                    erros_lote += 1

            except requests.exceptions.HTTPError as http_err:
                erros_lote += 1; status_code = http_err.response.status_code if http_err.response else 'N/A'
                print(f"[AVISO V30] Lote {i+1} erro HTTP {status_code}: {lote_limpo}. Erro: {http_err}")
            except Exception as e_lote:
                erros_lote += 1
                print(f"[ERRO V30] Falha genérica lote {i+1}: {lote_limpo}. Erro: {e_lote}")

            percentual = (i + 1) / len(lotes_de_fiis)
            status_texto = f"Buscando Lote {i+1}/{len(lotes_de_fiis)}..."
            if not lote_bem_sucedido: status_texto += " [ERRO]"
            progress_bar.progress(percentual, text=status_texto)
            time.sleep(0.1)


        progress_bar.empty()
        status_placeholder.info(f"Todos os {len(lotes_de_fiis)} lotes foram processados ({erros_lote} falharam). Formatando dados coletados...")

        # --- PARTE 3: PROCESSAMENTO (V30 - Inclui Liquidez e Setor) ---
        if not todos_os_dados_api:
             st.error("Nenhum dado foi coletado com sucesso após processar todos os lotes.")
             print("[ERRO V30] Lista 'todos_os_dados_api' está vazia.")
             return False

        total_processado = 0
        fiis_com_dados_completos = 0

        for fii_result in todos_os_dados_api:
            total_processado += 1
            ticker = fii_result.get('symbol')
            stats_module = fii_result.get('defaultKeyStatistics')
            # V30: Pegamos o volume direto da resposta principal
            liquidez = fii_result.get('regularMarketVolume')

            # Recupera o setor do mapa que criamos
            setor = setor_map.get(ticker, "Desconhecido") # Usa "Desconhecido" se não encontrar

            if ticker and isinstance(stats_module, dict) and liquidez is not None:
                pvp_direto = stats_module.get('priceToBook')
                dy_decimal = stats_module.get('dividendYield') # API retorna decimal (ex: 0.12 ou None)

                # Verifica se P/VP existe e é um número válido > 0
                if pvp_direto is not None and isinstance(pvp_direto, (int, float)) and pvp_direto > 0:
                    pvp = float(pvp_direto)
                    # Verifica se DY existe e é um número, converte para %, senão usa 0
                    dy = (float(dy_decimal) * 100) if dy_decimal is not None and isinstance(dy_decimal, (int, float)) else 0.0

                    # Adiciona ao DB mesmo se DY for 0 (P/VP e Liquidez são suficientes)
                    dados_para_db.append((ticker, pvp, dy, float(liquidez), setor))
                    fiis_com_dados_completos += 1
                else:
                    print(f"[AVISO V30] FII {ticker}: P/VP inválido ({pvp_direto}). Descartado.")
            else:
                 print(f"[AVISO V30] FII {ticker}: Dados essenciais (ticker, stats, liquidez) ausentes. Descartado. Stats: {stats_module}, Liq: {liquidez}")


    except requests.exceptions.RequestException as req_err:
        st.error(f"Erro de conexão CRÍTICO ao tentar acessar a API Brapi: {req_err}")
        print(f"Erro CRÍTICO V30 (Conexão): {req_err}")
        return False
    except Exception as e:
        st.error(f"Erro CRÍTICO inesperado durante a execução da coleta: {e}")
        print(f"Erro CRÍTICO V30: {e}")
        return False

    status_placeholder.empty()

    if not dados_para_db:
        st.error("Dados foram coletados, mas nenhum FII continha P/VP, DY e Liquidez válidos após o processamento.")
        print(f"[ERRO V30] Lista 'dados_para_db' está vazia após processar {total_processado} FIIs.")
        return False

    # 4. Salva no Banco de Dados
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # V30: Salvando Ticker, P_VP, DY_12M, Liquidez_Diaria, Setor
    cursor.executemany("""
    REPLACE INTO fiis (Ticker, P_VP, DY_12M, Liquidez_Diaria, Setor, data_coleta)
    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, dados_para_db)
    conn.commit()
    conn.close()

    st.success(f"Busca finalizada! {len(dados_para_db)} FIIs com dados válidos foram atualizados.")
    return True

# --- PARTE 2: APP WEB (COM NOVOS FILTROS E SCORE) ---

@st.cache_data # Cacheia o resultado da leitura do DB
def carregar_dados_do_db() -> pd.DataFrame:
    if not os.path.exists(DB_FILE): return pd.DataFrame()
    conn = sqlite3.connect(DB_FILE);
    try:
        # V30: Selecionamos as novas colunas
        df = pd.read_sql_query("SELECT Ticker, P_VP, DY_12M, Liquidez_Diaria, Setor, data_coleta FROM fiis", conn)
        # Convertemos tipos de dados para garantir
        df['P_VP'] = pd.to_numeric(df['P_VP'], errors='coerce')
        df['DY_12M'] = pd.to_numeric(df['DY_12M'], errors='coerce')
        df['Liquidez_Diaria'] = pd.to_numeric(df['Liquidez_Diaria'], errors='coerce')
        df.dropna(subset=['Ticker', 'P_VP', 'DY_12M', 'Liquidez_Diaria'], inplace=True) # Remove linhas com dados essenciais faltando
    except (pd.io.sql.DatabaseError, sqlite3.Error) as db_err:
        st.error(f"Erro ao ler o banco de dados: {db_err}")
        df = pd.DataFrame()
    finally:
        if conn: conn.close()
    return df

# V30: Score Pro V3 (DY x P/VP x Liquidez)
def calcular_score_pro(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or not all(col in df.columns for col in ['P_VP', 'DY_12M', 'Liquidez_Diaria']):
        df['Score Pro'] = pd.Series(dtype='int')
        return df

    # Filtro de qualidade inicial (P/VP razoável)
    df_filtrado = df[(df['P_VP'] > 0.1) & (df['P_VP'] < 1.5)].copy()
    df_filtrado['DY_12M'] = df_filtrado['DY_12M'].fillna(0) # Trata DY nulo como 0
    df_filtrado['Liquidez_Diaria'] = df_filtrado['Liquidez_Diaria'].fillna(0) # Trata Liquidez nula como 0

    if df_filtrado.empty:
        df['Score Pro'] = 0
        return df

    # Normalização (0-100)
    dy_range = df_filtrado['DY_12M'].max() - df_filtrado['DY_12M'].min()
    pvp_range = df_filtrado['P_VP'].max() - df_filtrado['P_VP'].min()
    liq_range = df_filtrado['Liquidez_Diaria'].max() - df_filtrado['Liquidez_Diaria'].min()

    # Nota DY (quanto maior, melhor) - Peso 50%
    if dy_range == 0: df_filtrado['dy_norm'] = 50
    else: df_filtrado['dy_norm'] = 100 * (df_filtrado['DY_12M'] - df_filtrado['DY_12M'].min()) / dy_range

    # Nota P/VP (quanto MENOR, melhor) - Peso 30%
    if pvp_range == 0: df_filtrado['pvp_norm'] = 50
    else: df_filtrado['pvp_norm'] = 100 * (df_filtrado['P_VP'].max() - df_filtrado['P_VP']) / pvp_range

    # Nota Liquidez (quanto maior, melhor) - Peso 20%
    if liq_range == 0: df_filtrado['liq_norm'] = 50
    else: df_filtrado['liq_norm'] = 100 * (df_filtrado['Liquidez_Diaria'] - df_filtrado['Liquidez_Diaria'].min()) / liq_range

    # Score Pro V3: 50% DY, 30% P/VP, 20% Liquidez
    df_filtrado['Score Pro'] = (df_filtrado['dy_norm'] * 0.5) + (df_filtrado['pvp_norm'] * 0.3) + (df_filtrado['liq_norm'] * 0.2)

    df_final = df.merge(df_filtrado[['Ticker', 'Score Pro']], on='Ticker', how='left')
    df_final['Score Pro'] = df_final['Score Pro'].fillna(0).astype(int)
    return df_final


st.title("🛰️ Radar FII Pro (Cloud V30 - Refinado)")
st.subheader("Encontrando as melhores oportunidades em Fundos Imobiliários")

inicializar_db()
df_base = carregar_dados_do_db() # Usa cache aqui
data_atualizacao = None
dados_expirados = True

if not df_base.empty:
    try:
        data_atualizacao = pd.to_datetime(df_base['data_coleta'].iloc[0])
        st.caption(f"Dados do cache atualizados em: {data_atualizacao.strftime('%d/%m/%Y às %H:%M:%S')}")
        dados_expirados = (pd.Timestamp.now() - data_atualizacao > pd.Timedelta(hours=1))
    except (KeyError, IndexError, TypeError):
        df_base = pd.DataFrame() # Reseta se dados corrompidos
if data_atualizacao is None: dados_expirados = True # Se não tem data, expira

st.sidebar.header("Controles")
update_button_pressed = st.sidebar.button("Forçar Atualização Agora (API Rápida)")

atualizacao_bem_sucedida = False
if update_button_pressed:
    with st.spinner("Atualizando dados via API..."):
        atualizacao_bem_sucedida = atualizar_dados_fiis()
    # Se atualização deu certo, limpa cache e recarrega
    if atualizacao_bem_sucedida:
        st.cache_data.clear() # Limpa o cache da leitura do DB
        df_base = carregar_dados_do_db() # Recarrega do DB
        st.rerun() # Reinicia para mostrar dados novos
    # Se falhou, a mensagem de erro já foi mostrada por atualizar_dados_fiis()

elif dados_expirados or df_base.empty:
    if df_base.empty: st.info("Cache local vazio. Iniciando busca na API...")
    else: st.info("Dados do cache expirados. Iniciando busca na API...")
    with st.spinner("Atualizando dados via API..."):
        atualizacao_bem_sucedida = atualizar_dados_fiis()
    if atualizacao_bem_sucedida:
        st.cache_data.clear()
        df_base = carregar_dados_do_db()
        st.rerun()
    # Se falhou e df_base já tinha algo, usa o cache antigo (não para o app)
    elif not df_base.empty:
        st.warning("Falha ao atualizar dados. Exibindo dados do último cache bem-sucedido.")
    # Se falhou E df_base estava vazio, o erro será tratado abaixo
else:
    st.write("Dados carregados do cache local.")


if df_base.empty:
    st.error("Não há dados disponíveis para exibição (nem no cache, nem da API). Tente 'Forçar Atualização' mais tarde.")
    st.stop()

# Calcula o Score V3
df_com_score = calcular_score_pro(df_base)

# --- V30: Novos Filtros na UI ---
st.sidebar.header("Filtros Avançados")
preco_teto_pvp = st.sidebar.slider("P/VP Máximo:", 0.5, 2.0, 1.2, 0.01)
dy_minimo = st.sidebar.slider("Dividend Yield (12M) Mínimo (%):", 0.0, 25.0, 0.0, 0.5)

# Filtro de Liquidez
min_liq = 0
max_liq = int(df_com_score['Liquidez_Diaria'].max()) if not df_com_score['Liquidez_Diaria'].empty else 1_000_000
default_liq = 100_000 if max_liq > 100_000 else min_liq
liquidez_minima = st.sidebar.slider(
    "Liquidez Diária Mínima (R$):",
    min_value=min_liq,
    max_value=max_liq,
    value=default_liq,
    step=50_000,
    format="R$ %d"
)

# Filtro de Setor
setores_disponiveis = sorted(df_com_score['Setor'].dropna().unique())
setores_selecionados = st.sidebar.multiselect(
    "Setores:",
    options=setores_disponiveis,
    default=setores_disponiveis # Começa com todos selecionados
)

score_minimo = st.sidebar.slider("Score Pro Mínimo (de 0 a 100):", 0, 100, 0, 5)

# --- V30: Lógica de Filtragem Atualizada ---
df_filtrado = df_com_score[
    (df_com_score['P_VP'] <= preco_teto_pvp) &
    (df_com_score['DY_12M'] >= dy_minimo) &
    (df_com_score['Liquidez_Diaria'] >= liquidez_minima) &
    (df_com_score['Setor'].isin(setores_selecionados)) &
    (df_com_score['Score Pro'] >= score_minimo)
]

# --- V30: Tabela Atualizada ---
st.header(f"Resultados Encontrados: {len(df_filtrado)} (de {len(df_base)} FIIs no cache)")
# Adicionamos Setor e Liquidez à exibição
colunas_para_exibir = {
    'Ticker': 'Ticker',
    'Score Pro': 'Score Pro 🔥',
    'P_VP': 'P/VP',
    'DY_12M': 'DY (12M) %',
    'Liquidez_Diaria': 'Liquidez Diária',
    'Setor': 'Setor'
}

st.dataframe(
    df_filtrado[colunas_para_exibir.keys()]
    .sort_values(by='Score Pro', ascending=False)
    .style.format({
        'Score Pro': '{:d} pts',
        'P_VP': '{:.2f}',
        'DY_12M': '{:.2f}%',
        'Liquidez_Diaria': 'R$ {:,.0f}' # Formato monetário
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