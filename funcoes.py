from relatorios import *
from time import sleep, time
from Automacao_SSW import Automacao
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from _930 import transformar_930
from BD_Ocorrencia import gerar_bd_ocorrencia
from _150 import gerar_solucao_aberto, gerar_solucao_faltas, gerar_solucao_prev
from _455 import gerar_455
from utils.utils import mostrar_todo_dataframe, gerar_dataframe, remover_arquivos_na_pasta
import logging
import psutil  # pip install psutil


LOGGER = logging.getLogger(__name__)


def calcular_max_workers(
    mem_por_worker_gb: float = 1.5,
    limite_cpu_frac: float = 0.75,
    max_cap: int = 10,
    min_cap: int = 2,
) -> int:
    """
    Calcula um max_workers seguro considerando CPU e RAM.
    Ajuste mem_por_worker_gb conforme o consumo médio de cada automação (Selenium costuma ser 0.8~2.0 GB).
    """
    cpu_total = os.cpu_count() or 2
    limite_cpu = max(1, int(cpu_total * limite_cpu_frac))

    mem_total_gb = psutil.virtual_memory().total / (1024**3)
    limite_mem = max(1, int(mem_total_gb // mem_por_worker_gb))

    return max(min_cap, min(limite_cpu, limite_mem, max_cap))


def executar_automacao(
    codigo,
    relatorio,
    dt_inicial,
    dt_final,
    nome,
    pasta_temp,
    caminho,
    is_156,
    inicio_automacao,
    funcao,
    parametro_1,
    parametro_2,
    parametro_3,
    parametro_4,
    parametro_5,
):
    automacao = Automacao(
        codigo,
        relatorio,
        dt_inicial,
        dt_final,
        nome,
        pasta_temp,
        caminho,
        is_156,
        inicio_automacao,
        funcao,
        parametro_1,
        parametro_2,
        parametro_3,
        parametro_4,
        parametro_5,
    )
    automacao.baixar_relatorio()


def executar_processos(lista_dicionarios):
    # Ajuste estes números conforme sua máquina:
    # - mem_por_worker_gb: quanto cada automação consome
    # - max_cap: teto absoluto (evita "explodir" mesmo em máquina muito forte)
    max_processes = calcular_max_workers(mem_por_worker_gb=1.5, limite_cpu_frac=0.75, max_cap=10, min_cap=2)
    LOGGER.info(f"max_workers calculado: {max_processes}")

    # Pool único: evita overhead de criar/destroir processos a cada mini-lote
    with ProcessPoolExecutor(max_workers=max_processes) as executor:
        future_to_relatorio = {}

        for item in lista_dicionarios:
            for key, detalhe in item.items():
                LOGGER.info(f"{key}: {detalhe}")

                codigo = key[0:3]
                relatorio = key
                dt_inicial = detalhe["dt_inicial"]
                dt_final = detalhe["dt_final"]
                nome = detalhe["nome"]
                pasta_temp = detalhe["pasta_temp"]
                caminho = detalhe["caminho"]
                is_156 = detalhe["is_156"]
                inicio_automacao = datetime.today()
                funcao = detalhe["funcao"]
                parametro_1 = detalhe["parametro_1"]
                parametro_2 = detalhe["parametro_2"]
                parametro_3 = detalhe["parametro_3"]
                parametro_4 = detalhe["parametro_4"]
                parametro_5 = detalhe["parametro_5"]

                future = executor.submit(
                    executar_automacao,
                    codigo,
                    relatorio,
                    dt_inicial,
                    dt_final,
                    nome,
                    pasta_temp,
                    caminho,
                    is_156,
                    inicio_automacao,
                    funcao,
                    parametro_1,
                    parametro_2,
                    parametro_3,
                    parametro_4,
                    parametro_5,
                )
                future_to_relatorio[future] = relatorio

                # Throttle (evita "explosão" abrindo vários browsers no mesmo instante)
                # Ajuste fino:
                # - se sua automação abre Selenium pesado: aumente um pouco
                # - se for leve: diminua
                sleep(40 if is_156 else 1)

        # Coleta resultados (e loga erros)
        for future in as_completed(future_to_relatorio):
            relatorio = future_to_relatorio[future]
            try:
                future.result()
                LOGGER.info(f"Automação {relatorio} completada com sucesso.")
            except Exception as e:
                LOGGER.exception(f"Automação {relatorio} falhou com erro: {e}")


def dividir_dicionario_em_minis(dicionario, max_elementos=9):
    # Lista para armazenar os mini dicionários
    lista_minis = []

    # Variáveis para controle
    mini_dicionario = {}
    contador = 1

    # Itera sobre os itens do dicionário original
    for chave, valor in dicionario.items():
        # Adiciona o item atual ao mini dicionário
        mini_dicionario[chave] = valor

        # Verifica se o mini dicionário atingiu o número máximo de elementos
        if len(mini_dicionario) == max_elementos:
            # Adiciona o mini dicionário completo à lista e reseta para o próximo
            lista_minis.append(mini_dicionario)
            mini_dicionario = {}
            contador += 1

    # Adiciona o último mini dicionário se contiver elementos
    if mini_dicionario:
        lista_minis.append(mini_dicionario)

    return lista_minis


def run_automacao_solucao(relatorios, Caminho, PARAMETROS_FILE, PATH_OUTPUT):

    LOGGER.info("Início da extração de dados do SSW")
    relatorios_solucao(relatorios, Caminho)

    lista_mini_dicionarios = dividir_dicionario_em_minis(relatorios, max_elementos=9)
    executar_processos(lista_mini_dicionarios)
    LOGGER.info("Fim da extração de dados do SSW")

    LOGGER.info("Início do tratamento de dados no pandas")

    # Ler 930
    LOGGER.info("Lendo arquivos da opção 930")
    df_930 = transformar_930(Caminho)
    LOGGER.info("Fim da leitura dos arquivos da opção 930")

    LOGGER.info("Lendo o parâmetro de ocorrência")
    df_ocorrencia = gerar_dataframe(PARAMETROS_FILE, "Ocorrencias")
    LOGGER.info("Fim da leitura o parâmetro de ocorrência")

    LOGGER.info("Gerando BD_Ocorrência")
    BD_Ocorrencia = gerar_bd_ocorrencia(df_930, df_ocorrencia, PATH_OUTPUT)

    # Ler 455
    LOGGER.info("Lendo arquivo 455 de subcontratos")
    df_455 = gerar_455(Caminho)

    # Ler 150, Gera clt_solucao_aberto e clt_solucao_faltas
    LOGGER.info("Gerando clt_solucao_aberto, clt_solucao_faltas e clt_solucao_prev")
    solucao_aberto = gerar_solucao_aberto(df_ocorrencia, BD_Ocorrencia, PATH_OUTPUT, Caminho)
    solucao_faltas = gerar_solucao_faltas(df_ocorrencia, BD_Ocorrencia, PATH_OUTPUT, Caminho)
    solucao_prev = gerar_solucao_prev(df_ocorrencia, df_455, PATH_OUTPUT, Caminho)

    LOGGER.info(f"Excluindo arquivos da pasta 930")
    pasta_930 = Caminho + os.sep + "Cockpit 3.0" + os.sep + "Fontes" + os.sep + "Solução" + os.sep + "930"
    remover_arquivos_na_pasta(pasta_930)
