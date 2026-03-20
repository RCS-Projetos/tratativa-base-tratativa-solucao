import pandas as pd
import os
import datetime
from datetime import timezone, timedelta
import ntplib
import pytz
import shutil
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def mostrar_todo_dataframe(df):
    # Exibir todas as linhas
    pd.set_option("display.max_rows", None)

    # Exibir todas as colunas
    pd.set_option("display.max_columns", None)

    # Não cortar o conteúdo das células
    pd.set_option("display.max_colwidth", None)

    # Ajustar largura do dataframe para caber
    pd.set_option("display.expand_frame_repr", False)

    print(df)


def gerar_dataframe(arquivo, planilha, colunas_cnpj=None, dtype=None, colunas_desejadas=None, keys=None):
    df = pd.read_excel(arquivo, sheet_name=planilha, engine="openpyxl", dtype=dtype)

    df = remover_espacos_colunas(df)
    df = remover_espacos_em_branco_item(df)

    if keys:
        df = df.drop_duplicates(subset=keys)

    if colunas_cnpj:
        df = normaliza_cnpj(df, colunas_cnpj)

    if planilha == "Penaliza_Adiantado":
        df = df.rename(columns={"Cliente Remetente": "Cliente"})
    elif planilha == "Resp_Oc_Dif_Cliente":
        df = df.drop(columns=["Chave"])
        df["Chave"] = df["CNPJ Remetente"].astype(str) + df["Ocorrencia"].astype(str)
    elif planilha == "ADC_FAIXA_CEP":
        df["CEP INICIO"] = df["CEP INICIO"].astype(str).str.replace("-", "")
        df["CEP FIM"] = df["CEP FIM"].astype(str).str.replace("-", "")

    elif planilha == "Planilha1":
        try:
            df["EMBARQUE"] = pd.to_numeric(df["EMBARQUE"], errors="coerce").astype("Int64")
            df["EMBARQUE + PREVISÃO"] = pd.to_numeric(df["EMBARQUE + PREVISÃO"], errors="coerce").astype("Int64")
        except Exception as e:
            print(f"Erro ao fazer a converção na planilha '{planilha}': {e}")

    if colunas_desejadas:
        df = df[colunas_desejadas]

    return df


def remover_espacos_colunas(dataframe):
    # Remova espaços extras nas colunas
    dataframe.columns = dataframe.columns.str.strip()

    return dataframe


def remover_espacos_em_branco_item(dataframe):
    """
    Remove espaços em branco, se tiver, nos itens do dataframe se for do tipo object
    :param dataframe:
    :return:
    """

    colunas = dataframe.columns.tolist()

    for col in colunas:
        if dataframe[col].dtype == "object":
            dataframe[col] = dataframe[col].str.strip()

    return dataframe


def normaliza_cnpj(dataframe, lista_colunas):
    """
    Padroniza os CNPJs das colunas especificadas, removendo caracteres especiais e garantindo que todos fiquem com exatamente 14 dígitos, alinhados com o formato oficial de CNPJs.
    :param dataframe:
    :param lista_colunas:
    :return:
    """
    cols_cnpj = lista_colunas

    for col in cols_cnpj:
        dataframe[col] = ((dataframe[col].astype(str).str.replace(".", "", regex=False)
                    .str.replace("/", "", regex=False))
                   .str.replace("-", "", regex=False))
        dataframe[col] = dataframe[col].astype(str).str.zfill(14)

    return dataframe


def transformar_datas(dataframe, lista_colunas_data):
    colunas_data = lista_colunas_data

    for col in colunas_data:
        dataframe[col] = pd.to_datetime(dataframe[col].where(dataframe[col].notna() & (dataframe[col] != '')), errors="coerce",
                                 dayfirst=True)

    return dataframe


def transformar_horas(dataframe, lista_colunas_hora, formato):
    colunas_hora = lista_colunas_hora

    for col in colunas_hora:
        dataframe[col] = pd.to_datetime(dataframe[col], format=formato, errors="coerce").dt.time

    return dataframe


def converter_duracao(duracao):
    # Converte a duração para o formato hh:mm:ss
    hours, remainder = divmod(int(duracao), 3600)
    minutes, seconds = divmod(remainder, 60)
    duration_str = f"{hours:02}:{minutes:02}:{seconds:02}"
    return duration_str


def listar_arquivos(pasta):
    lista = []
    for raiz, _, arquivos in os.walk(pasta):
        for nome in arquivos:
            caminho_completo = os.path.join(raiz, nome)
            info = os.stat(caminho_completo)
            data_modificacao = datetime.datetime.fromtimestamp(info.st_mtime).date()
            data_atual = datetime.date.today()

            lista.append(caminho_completo)

    return lista


def obter_hora_servidor_win():
    # Criar o cliente NTP
    ntp_client = ntplib.NTPClient()

    # Consultar o servidor time.windows.com
    response = ntp_client.request('time.windows.com')

    # Converter o timestamp para datetime
    ntp_time_utc = datetime.datetime.fromtimestamp(response.tx_time, timezone.utc)

    # Definir o fuso horário de Brasília (Brasil)
    br_tz = pytz.timezone('America/Sao_Paulo')

    # Converter o horário UTC para o horário de Brasília
    ntp_time_brazil = ntp_time_utc.astimezone(br_tz)

    return ntp_time_brazil


def hora_com_tolerancia_v2(agora):
    horas_variadas = []

    # Variação de -2 até +25 segundos
    for segundos in range(-3, 13):
        hora_variada = (agora + timedelta(seconds=segundos)).strftime("%H:%M:%S")
        horas_variadas.append(hora_variada)
        # print(hora_variada)

    return horas_variadas


def get_data(dias):
    """
    Retorna a data atual menos o número especificado de dias.
    :param dias: O número de dias a subtrair da data atual.
    :return: Um objeto datetime representando a data atual menos o número de dias especificado.
    """
    # data_teste = datetime(2024, 5, 13) - timedelta(
    # days=dias)
    return datetime.datetime.today() - timedelta(days=dias)


def remover_arquivos_na_pasta(pasta):
    if os.path.isdir(pasta):
        for filename in os.listdir(pasta):
            file_path = os.path.join(pasta, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)  # Remove arquivos e links simbólicos
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # Remove subdiretórios
            except Exception as e:
                print(f'Falha ao deletar {file_path}. Razão: {e}')

def enviar_email(subject, message, from_addr, to_addr, password):
    # Cria a mensagem do e-mail
    msg = MIMEMultipart('alternative')
    # msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = to_addr

    # Adiciona a parte do texto em formato simples
    text_part = MIMEText(message, 'plain')
    msg.attach(text_part)

    # Adiciona a parte do texto em formato HTML
    html_part = MIMEText(message, 'html')
    msg.attach(html_part)

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(from_addr, password)
        server.sendmail(from_addr, to_addr, msg.as_string())
        server.quit()
    except Exception as e:
        print(f"Erro ao enviar email: {e}")

