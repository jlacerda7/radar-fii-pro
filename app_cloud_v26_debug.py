# --- VERSÃO DE DIAGNÓSTICO (CLOUD) v26 ---
# --- OBJETIVO: IMPRIMIR A ESTRUTURA REAL DA RESPOSTA DA API PARA UM FII VÁLIDO ---

import streamlit as st
from brapi import Brapi
import json # Para imprimir a estrutura de forma legível
import traceback # Para imprimir detalhes do erro, se houver

st.set_page_config(layout="wide", page_title="DEBUG Radar FII V26")

st.title("🛰️ Radar FII Pro (DEBUG V26 - Visualizador)")
st.subheader("Verificando a estrutura da resposta da API Brapi para UM FII")

# FII para teste (sabemos que tem dados)
fii_teste = "MXRF11"

status_placeholder = st.empty()
status_placeholder.info(f"Tentando buscar dados de '{fii_teste}' com o módulo 'defaultKeyStatistics'...")

try:
    api_key = st.secrets["BRAPI_API_KEY"]
    brapi = Brapi(api_key=api_key)

    # Pedimos os dados COM o módulo
    st.write(f"Enviando requisição para: {fii_teste}, modules='defaultKeyStatistics'")
    fiis_data_response = brapi.quote.retrieve(tickers=fii_teste, modules="defaultKeyStatistics")

    status_placeholder.success("Dados recebidos da API! Verifique os Logs para a estrutura completa.")

    # --- IMPRESSÃO PARA DEBUG ---
    st.write("Estrutura da resposta recebida (verifique os Logs do Streamlit Cloud):")

    # Imprime nos Logs do Streamlit Cloud
    print("\n--- INÍCIO DEBUG V26 ---")

    if fiis_data_response and hasattr(fiis_data_response, 'results') and len(fiis_data_response.results) > 0:
        fii_result = fiis_data_response.results[0] # Pega o primeiro (e único) resultado
        ticker_name = getattr(fii_result, 'stock', 'Ticker Desconhecido')
        print(f"\n--- Estrutura para FII: {ticker_name} ---")

        # Tentamos imprimir a estrutura completa do objeto
        try:
            # vars() transforma o objeto em um dicionário
            fii_dict = vars(fii_result)
            # json.dumps formata o dicionário de forma legível
            print(json.dumps(fii_dict, indent=4, default=str)) # default=str lida com tipos não serializáveis
        except Exception as print_e:
            print(f"Erro ao tentar converter/imprimir a estrutura do FII: {print_e}")
            print(f"Objeto recebido (bruto): {fii_result}") # Imprime o objeto bruto se a conversão falhar

        # Tenta acessar especificamente o módulo para ver sua estrutura interna
        stats_module = getattr(fii_result, 'defaultKeyStatistics', None)
        if stats_module:
            print(f"\n--- Detalhe do Módulo 'defaultKeyStatistics' para {ticker_name} ---")
            try:
                stats_dict = vars(stats_module)
                print(json.dumps(stats_dict, indent=4, default=str))
            except Exception as print_stats_e:
                 print(f"Erro ao tentar converter/imprimir a estrutura do módulo: {print_stats_e}")
                 print(f"Módulo recebido (bruto): {stats_module}")
        else:
            print(f"\n--- Módulo 'defaultKeyStatistics' NÃO encontrado para {ticker_name} ---")

    else:
        print("\n--- NENHUM RESULTADO RECEBIDO DA API ---")
        st.warning("A API não retornou resultados.")
        # Imprime a resposta bruta, se houver
        print(f"Resposta bruta da API: {fiis_data_response}")


    print("\n--- FIM DEBUG V26 ---")

    # Mostra uma prévia na tela (limitada)
    if fiis_data_response and hasattr(fiis_data_response, 'results') and len(fiis_data_response.results) > 0:
        st.json(vars(fiis_data_response.results[0]), expanded=True) # Mostra o primeiro FII expandido
    else:
        st.warning("A API não retornou resultados para exibição.")


except Exception as e:
    status_placeholder.error(f"Erro CRÍTICO ao conectar ou processar dados da API Brapi: {e}")
    st.exception(e) # Mostra o traceback completo do erro na tela
    print(f"Erro CRÍTICO V26: {e}") # Imprime o erro nos logs
    print(traceback.format_exc()) # Imprime o traceback completo nos logs