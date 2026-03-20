import os
from utils.utils import listar_arquivos, normaliza_cnpj, transformar_datas, mostrar_todo_dataframe
import pandas as pd
import logging


LOGGER = logging.getLogger(__name__)


def preparar_dataframe(arquivo):
    LOGGER.info(f"Lendo arquivo: {arquivo}")

    colunas = [
        "Serie/Numero CTRC",
        "Data de Emissao",
        "Cliente Remetente",
        "Cliente Destinatario",
        "Cliente Pagador",
        "Unidade Receptora",
        "Peso Calculado em Kg",
        "Numero da Nota Fiscal",
        "Valor do Frete sem ICMS",
        "Previsao de Entrega",
        "Codigo da Ultima Ocorrencia",
        "Data da Ultima Ocorrencia",
        "Valor da Mercadoria"
    ]

    df = pd.read_csv(arquivo, encoding="latin-1", sep=";", engine="python", skiprows=1, skipfooter=1, usecols=colunas)

    df.rename(columns={
        "Serie/Numero CTRC": "CTRC",
        "Data de Emissao": "EMISSAO",
        "Cliente Remetente": "REMETENTE",
        "Cliente Destinatario": "DESTINATARIO",
        "Cliente Pagador": "PAGADOR",
        "Unidade Receptora": "UNID_ENTREGA",
        "Peso Calculado em Kg": "KG_CALCULADO",
        "Numero da Nota Fiscal": "NOTA_FISCAL",
        "Valor do Frete sem ICMS": "FRETE",
        "Previsao de Entrega": "PREV_ENTREGA",
        "Codigo da Ultima Ocorrencia": "COD_OCORRENCIA",
        "Data da Ultima Ocorrencia": "DATA_OCORRENCIA",
        "Valor da Mercadoria": "VALOR_MERCADORIA"
    }, inplace=True)

    cols_data = ["EMISSAO", "DATA_OCORRENCIA", "PREV_ENTREGA"]
    transformar_datas(df, cols_data)

    return df


def gerar_455(Caminho):
    PASTA = Caminho + os.sep + "Cockpit 3.0" + os.sep + "Fontes" + os.sep + "455" + os.sep + "Subs"

    # Lista para acumular os DataFrames
    lista_df = []

    for arquivo in listar_arquivos(PASTA):
        df = preparar_dataframe(arquivo)
        lista_df.append(df)

    # Concatenar todos os DataFrames
    df = pd.concat(lista_df, ignore_index=True)


    return df
