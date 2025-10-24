# --- VERSÃO DE DIAGNÓSTICO (CLOUD) v22 ---
# --- OBJETIVO: IMPRIMIR A ESTRUTURA REAL DA RESPOSTA DA API PARA FIIS ---

import streamlit as st
from brapi import Brapi
import json # Para imprimir a estrutura de forma legível

st.set_page_config(layout="wide", page_title="DEBUG Radar FII")

st.title("🛰️ Radar FII Pro (DEBUG V22)")
st.subheader("Verificando a estrutura da resposta da API Brapi para FIIs")

# FIIs para teste
fiis_teste = ["MXRF11", "HGLG11", "KNCR11"]

status_placeholder = st.empty()
status_placeholder.info(f"Tentando buscar dados de {', '.join(fiis_teste)} com o módulo 'defaultKeyStatistics'...")

try:
    api_key = st.secrets["BRAPI_API_KEY"]
    brapi = Brapi(api_key=api_key)

    # Pedimos os dados COM o módulo
    fiis_data_response = brapi.quote.retrieve(tickers=fiis_teste, modules="defaultKeyStatistics")

    status_placeholder.success("Dados recebidos da API! Verifique os Logs.")

    # --- IMPRESSÃO PARA DEBUG ---
    st.write("Estrutura da resposta recebida (verifique os Logs para detalhes completos):")

    # Imprime nos Logs do Streamlit Cloud
    print("\n--- INÍCIO DEBUG V22 ---")
    print(f"Número de resultados recebidos: {len(fiis_data_response.results)}")

    for i, fii_result in enumerate(fiis_data_response.results):
        print(f"\n--- FII {i+1}: {getattr(fii_result, 'stock', 'Ticker Desconhecido')} ---")
        # Tentamos imprimir a estrutura completa do objeto
        try:
            # vars() transforma o objeto em um dicionário, facilitando a visualização
            fii_dict = vars(fii_result)
            # json.dumps formata o dicionário de forma legível
            print(json.dumps(fii_dict, indent=2, default=str)) # default=str lida com tipos não serializáveis
        except Exception as print_e:
            print(f"Erro ao tentar imprimir a estrutura do FII: {print_e}")
            print(f"Objeto recebido (bruto): {fii_result}")

    print("\n--- FIM DEBUG V22 ---")

    # Mostra uma prévia na tela (limitada)
    if fiis_data_response.results:
        st.json(vars(fiis_data_response.results[0]), expanded=False) # Mostra o primeiro FII
    else:
        st.warning("A API não retornou resultados.")


except Exception as e:
    status_placeholder.error(f"Erro CRÍTICO ao conectar ou processar dados da API Brapi: {e}")
    st.exception(e) # Mostra o traceback completo do erro na tela
    print(f"Erro CRÍTICO V22: {e}") # Imprime o erro nos logs