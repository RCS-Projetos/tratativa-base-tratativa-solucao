import pandas as pd
import os
from utils.utils import mostrar_todo_dataframe
import logging


LOGGER = logging.getLogger(__name__)


def gerar_bd_ocorrencia(df_930, df_ocorrencia, OUTPUT):
    # Ler BD_OOCORRENCIA

    # try:
    #     BD_Ocorrencias = pd.read_csv("BD_Ocorrencias.csv")
    # except:
    #     BD_Ocorrencias = pd.DataFrame(columns=["CTRC", "COD_OCOR", "Data_Hora_Ocor"])

    arquivo = OUTPUT + os.sep + "BD_Ocorrencias.parquet"

    # Tentar ler BD_Ocorrencias (formato parquet)
    if os.path.exists(arquivo):
        LOGGER.info(f"Lendo o arquivo: {arquivo}")
        BD_Ocorrencias = pd.read_parquet(arquivo)
    else:
        LOGGER.info(f"Arquivo BD_Ocorrencia não existe. Será gerado")
        BD_Ocorrencias = pd.DataFrame(columns=["CTRC", "COD_OCOR", "Data_Hora_Ocor"])


    LOGGER.info(f"Mesclando Opção 930 com Parametro Ocorrência")
    # GERANDO BD_OCORRENCIA
    df_join = df_930.merge(
        df_ocorrencia,
        left_on="COD_OCOR",
        right_on="Código",
        how="inner"
    )

    LOGGER.info(f"Aplicando filtro 'Tratativa de Solução == Abertura'")
    # Filtra só "Abertura"
    df_join = df_join[df_join["Tratativa de Solução"] == "Abertura"]

    LOGGER.info(f"Combinando Data e Hora da Ocorrência")
    # Combina Data + Hora
    df_join["Data_Hora_Ocor"] = (
            pd.to_datetime(df_join["DATA_OCOR"]) +
            pd.to_timedelta(df_join["HORA_OCOR"].astype(str))
    )

    LOGGER.info(f"Agrupando para obter Minimo (Data + Hora)")
    # Agrupa para obter MIN(Data + Hora)
    novos_registros = (
        df_join
        .groupby(["CTRC", "COD_OCOR"], as_index=False)["Data_Hora_Ocor"]
        .min()
    )

    chaves_existentes = set(zip(BD_Ocorrencias.CTRC, BD_Ocorrencias.COD_OCOR))

    mascara_novos = ~novos_registros.apply(
        lambda row: (row.CTRC, row.COD_OCOR) in chaves_existentes,
        axis=1
    )

    novos_registros_limpos = novos_registros[mascara_novos]

    # Adicionar os novos registros na tabela BD_Ocorrencias

    LOGGER.info(f"Adicionando novos registros em BD_Ocorrencias.parquet")
    BD_Ocorrencias = pd.concat(
        [BD_Ocorrencias, novos_registros_limpos],
        ignore_index=True
    )

    LOGGER.info(f"Salvando arquivo {arquivo}")
    # Salvar em PARQUET
    BD_Ocorrencias.to_parquet(arquivo, index=False)

    return BD_Ocorrencias
