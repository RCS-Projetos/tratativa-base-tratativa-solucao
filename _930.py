import pandas as pd
import numpy as np
import os
import datetime
import logging


LOGGER = logging.getLogger(__name__)


def listar_arquivos_930(pasta):
    lista = []
    for raiz, _, arquivos in os.walk(pasta):
        for nome in arquivos:
            caminho_completo = os.path.join(raiz, nome)
            info = os.stat(caminho_completo)
            data_modificacao = datetime.datetime.fromtimestamp(info.st_mtime).date()
            data_atual = datetime.date.today()

            lista.append(caminho_completo)

    return lista


def preparar_dataframe(arquivo):
    LOGGER.info(f"Lendo arquivo: {arquivo}")

    df = pd.read_csv(arquivo, encoding="latin-1", sep=";", engine="python", dtype={"SERIE_CTE": str})

    LOGGER.info(f"Padronizando colunas do arquivo: {arquivo}")
    # Padronizar nomes de colunas
    df.columns = (
        df.columns
        .str.strip()
        .str.upper()
        .str.replace(' ', '_')
        .str.replace(r'[^\w\s]', '', regex=True)
    )

    colunas = df.columns.tolist()
    for col in colunas:
        if df[col].dtype == "object":
            df[col] = df[col].str.strip()

    LOGGER.info(f"Convertendo colunas de datas arquivo: {arquivo}")
    # Transformando em datetime
    colunas_data = [
        "EMISSAO_CTRC",
        "DIA_INCLUSAO_OCOR",
        "DATA_OCOR",
        "DATA_ENTREGA"
    ]

    for col in colunas_data:
        df[col] = pd.to_datetime(df[col].where(df[col].notna() & (df[col] != '')), errors="coerce", dayfirst=True)

    df["DATA_ENTREGA"] = df["DATA_ENTREGA"].astype('datetime64[ns]')

    colunas_hora = [
        "HORA_INCLUSAO_OCOR",
        "HORA_OCOR",
    ]

    for col in colunas_hora:
        df[col] = pd.to_datetime(df[col], format="%H:%M", errors="coerce").dt.time

    colunas_desejadas = [
        "CTRC",
        "EMISSAO_CTRC",
        "SERIE_CTE",
        "NRO_CTE",
        "SERIE_NOTA_FISCAL",
        "NRO_NOTA_FISCAL",
        "PEDIDO",
        "EMPR_OCOR",
        "UNID_OCOR",
        "DIA_INCLUSAO_OCOR",
        "HORA_INCLUSAO_OCOR",
        "DATA_OCOR",
        "HORA_OCOR",
        "USUARIO_OCOR",
        "COD_OCOR",
        "DESCRICAO_OCOR",
        "COMPLEMENTO_OCOR",
        "DATA_ENTREGA",
        "DESCR_ITEM",
        "PRIMEIRO_MANIFESTO",
        "CANCELADO",
        "CNPJ_PAGADOR",
        "NOME_PAGADOR"
    ]

    df = df[colunas_desejadas]
    # Substituir valores nulos por None (NULL para o PostgreSQL)
    df = df.replace({pd.NaT: None, np.nan: None})

    df["INDICE_CTRC"] = df.groupby("CTRC").cumcount() + 1

    # Remover colunas duplicadas
    df = df.loc[:, ~df.columns.duplicated()]

    LOGGER.info(f"Fim da transformação de dados do arquivo: {arquivo}")

    return df


def transformar_930(Caminho):
    # PASTA_930 = r"C:\Users\DIVOP\Desktop\Automacoes_V2\ETL2\Base\930"
    # PASTA = os.getcwd() + os.sep + "Fontes" + os.sep + "930"
    PASTA = Caminho + os.sep + "Cockpit 3.0" + os.sep + "Fontes" + os.sep + "Solução" + os.sep + "930"

    # Lista para acumular os DataFrames
    lista_df = []

    for arquivo in listar_arquivos_930(PASTA):
        df = preparar_dataframe(arquivo)
        lista_df.append(df)

    # Concatenar todos os DataFrames
    df = pd.concat(lista_df, ignore_index=True)

    return df
