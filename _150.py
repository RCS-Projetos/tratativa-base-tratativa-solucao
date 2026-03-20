import pandas as pd
import os
from utils.utils import listar_arquivos, normaliza_cnpj, transformar_datas, mostrar_todo_dataframe
import logging


LOGGER = logging.getLogger(__name__)


def preparar_dataframe(arquivo):
    LOGGER.info(f"Lendo arquivo: {arquivo}")

    try:
        df = pd.read_csv(arquivo, encoding="latin-1", sep=";", engine="python", dtype={"NOTA_FISCAL": str}, skipfooter=1)
    except pd.errors.EmptyDataError:
        LOGGER.warning(f"Arquivo vazio ignorado: {arquivo}")
        return None

    # Colunas CNPJ
    cols_cnpj = ["CNPJ_REMETENTE", "CNPJ_DESTINATARIO", "CNPJ_PAGADOR"]
    normaliza_cnpj(df, cols_cnpj)

    # Colunas Datas
    cols_data = ["EMISSAO", "PREV_ENTREGA", "DATA_OCORRENCIA", "EMISSAO ULT ROMANEIO"]
    transformar_datas(df, cols_data)

    if "COD_OCORRENCIA" in df.columns:
        df["COD_OCORRENCIA"] = df["COD_OCORRENCIA"].astype("Int64")

    return df



def gerar_solucao_aberto(df_ocorrencia, BD_Ocorrencias, OUTPUT, Caminho):
    # PASTA_930 = r"C:\Users\DIVOP\Desktop\Automacoes_V2\ETL2\Base\930"
    # PASTA = os.getcwd() + os.sep + "Fontes" + os.sep + "150"
    PASTA = Caminho + os.sep + "Cockpit 3.0" + os.sep + "Fontes" + os.sep + "Solução" + os.sep + "150"

    # Lista para acumular os DataFrames
    lista_df = []

    for arquivo in listar_arquivos(PASTA):
        df = preparar_dataframe(arquivo)
        if df is not None:
            lista_df.append(df)

    if not lista_df:
        LOGGER.warning(f"Nenhum arquivo válido encontrado na pasta: {PASTA}")
        return pd.DataFrame(columns=[
            "Setor Ocorrencia", "CTRC", "EMISSAO", "REMETENTE", "DESTINATARIO", "PAGADOR",
            "UNID_ENTREGA", "KG_CALCULADO", "NOTA_FISCAL", "FRETE", "PREV_ENTREGA", 
            "COD_OCORRENCIA", "DESCR_OCORRENCIA", "DATA_OCORRENCIA", "VALOR_MERCADORIA", 
            "Data_Abertura", "COMPLEMENTO_OCORRENCIA"
        ])

    # Concatenar todos os DataFrames
    df = pd.concat(lista_df, ignore_index=True)

    # 1. INNER JOIN 150 × Ocorrencias
    df_join = df.merge(
        df_ocorrencia,
        left_on="COD_OCORRENCIA",
        right_on="Código",
        how="inner"
    )

    # 2. LEFT JOIN com BD_Ocorrencias
    df_join = df_join.merge(
        BD_Ocorrencias,
        on="CTRC",
        how="left"
    )

    # 3. WHERE
    df_join = df_join[
        (df_join["0"] == 1) &
        (df_join["Setor Ocorrencia"] == "Solução")
        ]

    # 4. GROUP BY + MIN
    df_grouped = (
        df_join.groupby([
            "Setor Ocorrencia",
            "CTRC",
            "EMISSAO",
            "REMETENTE",
            "DESTINATARIO",
            "PAGADOR",
            "UNID_ENTREGA",
            "KG_CALCULADO",
            "NOTA_FISCAL",
            "FRETE",
            "PREV_ENTREGA",
            "COD_OCORRENCIA",
            "DESCR_OCORRENCIA",
            "DATA_OCORRENCIA",
            "VALOR_MERCADORIA",
            "COMPLEMENTO_OCORRENCIA"
        ], as_index=False)["Data_Hora_Ocor"].min()
    )

    # 5. Renomeia
    df_grouped = df_grouped.rename(columns={"Data_Hora_Ocor": "Data_Abertura"})

    colunas_ordenadas = [
        "Setor Ocorrencia",
        "CTRC",
        "EMISSAO",
        "REMETENTE",
        "DESTINATARIO",
        "PAGADOR",
        "UNID_ENTREGA",
        "KG_CALCULADO",
        "NOTA_FISCAL",
        "FRETE",
        "PREV_ENTREGA",
        "COD_OCORRENCIA",
        "DESCR_OCORRENCIA",
        "DATA_OCORRENCIA",
        "VALOR_MERCADORIA",
        "Data_Abertura",  # ← agora antes
        "COMPLEMENTO_OCORRENCIA"  # ← agora depois
    ]

    df_grouped = df_grouped[colunas_ordenadas]

    arquivo = OUTPUT + os.sep + "Solucao_Aberto.parquet"
    # 6. Salvar em PARQUET
    df_grouped.to_parquet(arquivo, index=False)

    LOGGER.info(f"Salvando arquivo: {arquivo}")


    return df_grouped


def gerar_solucao_faltas(df_ocorrencia, BD_Ocorrencias, OUTPUT, Caminho):
    # PASTA = os.getcwd() + os.sep + "Fontes" + os.sep + "150"
    PASTA = Caminho + os.sep + "Cockpit 3.0" + os.sep + "Fontes" + os.sep + "Solução" + os.sep + "150"

    # Lista para acumular os DataFrames
    lista_df = []

    for arquivo in listar_arquivos(PASTA):
        df = preparar_dataframe(arquivo)
        if df is not None:
            lista_df.append(df)

    if not lista_df:
        LOGGER.warning(f"Nenhum arquivo válido encontrado na pasta: {PASTA}")
        return pd.DataFrame(columns=[
            "Setor Ocorrencia", "CTRC", "EMISSAO", "REMETENTE", "DESTINATARIO", "PAGADOR",
            "UNID_ENTREGA", "KG_CALCULADO", "NOTA_FISCAL", "FRETE", "PREV_ENTREGA", 
            "COD_OCORRENCIA", "DESCR_OCORRENCIA", "DATA_OCORRENCIA", "VALOR_MERCADORIA", 
            "Data_Abertura", "COMPLEMENTO_OCORRENCIA"
        ])

    # Concatenar todos os DataFrames
    df = pd.concat(lista_df, ignore_index=True)

    # 1. INNER JOIN 150 × Ocorrencias
    df_join = df.merge(
        df_ocorrencia,
        left_on="COD_OCORRENCIA",
        right_on="Código",
        how="inner"
    )

    # 2. LEFT JOIN com BD_Ocorrencias
    df_join = df_join.merge(
        BD_Ocorrencias,
        on="CTRC",
        how="left"
    )

    # 3. WHERE
    df_join = df_join[
        (df_join["CTRC"].notnull()) &
        (df_join["Código"].isin([25, 28, 50, 53, 49, 38]))
        ]

    # 4. GROUP BY + MIN
    df_grouped = (
        df_join.groupby([
            "Setor Ocorrencia",
            "CTRC",
            "EMISSAO",
            "REMETENTE",
            "DESTINATARIO",
            "PAGADOR",
            "UNID_ENTREGA",
            "KG_CALCULADO",
            "NOTA_FISCAL",
            "FRETE",
            "PREV_ENTREGA",
            "COD_OCORRENCIA",
            "DESCR_OCORRENCIA",
            "DATA_OCORRENCIA",
            "VALOR_MERCADORIA",
            "COMPLEMENTO_OCORRENCIA"
        ], as_index=False)["Data_Hora_Ocor"].min()
    )

    # 5. Renomeia
    df_grouped = df_grouped.rename(columns={"Data_Hora_Ocor": "Data_Abertura"})

    colunas_ordenadas = [
        "Setor Ocorrencia",
        "CTRC",
        "EMISSAO",
        "REMETENTE",
        "DESTINATARIO",
        "PAGADOR",
        "UNID_ENTREGA",
        "KG_CALCULADO",
        "NOTA_FISCAL",
        "FRETE",
        "PREV_ENTREGA",
        "COD_OCORRENCIA",
        "DESCR_OCORRENCIA",
        "DATA_OCORRENCIA",
        "VALOR_MERCADORIA",
        "Data_Abertura",  # ← agora antes
        "COMPLEMENTO_OCORRENCIA"  # ← agora depois
    ]

    df_grouped = df_grouped[colunas_ordenadas]

    arquivo = OUTPUT + os.sep + "Solucao_Faltas.parquet"

    # 6. Salvar em PARQUET
    df_grouped.to_parquet(arquivo, index=False)

    LOGGER.info(f"Salvando arquivo: {arquivo}")

    return df_grouped


def gerar_solucao_prev(df_ocorrencia, df_455, OUTPUT, Caminho):
    # PASTA = os.getcwd() + os.sep + "Fontes" + os.sep + "150"
    PASTA = Caminho + os.sep + "Cockpit 3.0" + os.sep + "Fontes" + os.sep + "Solução" + os.sep + "150"

    # Lista para acumular os DataFrames
    lista_df = []

    for arquivo in listar_arquivos(PASTA):
        df = preparar_dataframe(arquivo)
        if df is not None:
            lista_df.append(df)

    if not lista_df:
        LOGGER.warning(f"Nenhum arquivo válido encontrado na pasta: {PASTA}")
        return pd.DataFrame(columns=[
            "Setor Ocorrencia", "CTRC", "EMISSAO", "REMETENTE", "DESTINATARIO", "PAGADOR",
            "UNID_ENTREGA", "KG_CALCULADO", "NOTA_FISCAL", "FRETE", "PREV_ENTREGA", 
            "COD_OCORRENCIA", "DATA_OCORRENCIA", "VALOR_MERCADORIA", "Código", "Descrição", 
            "Tipo", "Responsabilidade", "Retorno", "Tratativa de Solução", "Tempo Tratativa (horas)", 
            "Responsabilidade2", "Ação", "Coluna1"
        ])

    # Concatenar todos os DataFrames
    df = pd.concat(lista_df, ignore_index=True)

    colunas = ["CTRC", "EMISSAO", "REMETENTE", "PAGADOR", "DESTINATARIO", "UNID_ENTREGA", "NOTA_FISCAL", "VALOR_MERCADORIA", "FRETE", "KG_CALCULADO", "COD_OCORRENCIA", "DATA_OCORRENCIA", "PREV_ENTREGA"]

    df = df[colunas]

    # União dos dois DataFrames
    df_union = pd.concat([df, df_455], ignore_index=True)

    # 1. INNER JOIN 150 × Ocorrencias
    df_join = df.merge(
        df_ocorrencia,
        left_on="COD_OCORRENCIA",
        right_on="Código",
        how="inner"
    )

    # 2. WHERE
    df_join = df_join[
        (df_join["CTRC"].notnull()) &
        (df_join["Setor Ocorrencia"] == "Solução")
        ]

    # Lista original de colunas
    colunas_originais = df_join.columns.tolist()

    # Criar nova lista com 'Setor Ocorrencia' primeiro
    colunas_novas = ['Setor Ocorrencia'] + [c for c in colunas_originais if c != 'Setor Ocorrencia']

    colunas = ["Setor Ocorrencia", "CTRC", "EMISSAO", "REMETENTE", "DESTINATARIO", "PAGADOR", "UNID_ENTREGA", "KG_CALCULADO",
               "NOTA_FISCAL", "FRETE", "PREV_ENTREGA", "COD_OCORRENCIA", "DATA_OCORRENCIA", "VALOR_MERCADORIA",
               "Código", "Descrição", "Tipo", "Responsabilidade", "Retorno", "Tratativa de Solução", "Tempo Tratativa (horas)",
               "Responsabilidade2", "Ação", "Coluna1"]

    # Reordenar o DataFrame
    df_join = df_join[colunas]

    # Ordenar pelo Código em ordem crescente
    df_join = df_join.sort_values(by="Código", ascending=True).reset_index(drop=True)

    arquivo = OUTPUT + os.sep + "Solucao_Prev.parquet"
    # 6. Salvar em PARQUET
    df_join.to_parquet(arquivo, index=False)

    LOGGER.info(f"Salvando arquivo: {arquivo}")

    return df
