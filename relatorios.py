import os
from datetime import datetime, timedelta


def create_relatorio_dict(codigo, dt_inicial, dt_final, nome, pasta_temp, caminho, is_156, funcao, parametro_1, parametro_2, parametro_3, parametro_4, parametro_5):
    return {
        codigo: {
            "dt_inicial": dt_inicial,
            "dt_final": dt_final,
            "nome": nome,
            "pasta_temp": pasta_temp,
            "caminho": caminho,
            "is_156": is_156,
            "funcao": funcao,
            "parametro_1": parametro_1,
            "parametro_2": parametro_2,
            "parametro_3": parametro_3,
            "parametro_4": parametro_4,
            "parametro_5": parametro_5,
        }
    }


def criar_relatorio(dicionario_relatorios, codigo, dt_inicial, dt_final, nome, funcao, caminho, is_156=True, param1=None, param2=None, param3=None, param4=None, param5=None):
    pasta_temp = os.getcwd() + os.sep + "Fontes" + os.sep + codigo
    dicionario_relatorios.update(
        create_relatorio_dict(codigo, dt_inicial, dt_final, nome, pasta_temp, caminho, is_156, funcao, param1, param2, param3, param4, param5)
    )


def get_datas_930(qtd_dias):
    lista_datas = []

    for i in range(qtd_dias):
        data = datetime.today() - timedelta(days=i)
        lista_datas.append(formatar_data(data))

    return lista_datas


def formatar_data(data):
    return data.strftime("%d%m%y")


def PDV(dias):
    """
    Retorna o primeiro dia de vida do sistema SSW no formado ddmmyy
    :param dias:O número de dias para subtrair da data atual.
    :return:Str: O primeiro dia de vida do sistema SSW no formado ddmmyy.
    """
    return (datetime.today() - timedelta(days=dias)).strftime('010101')


def DtAtual():
    """
    Retorna a data atual no formato ddmmaa.
    :return: str: A data atual formatada como uma string no formato ddmmaa.
    """
    return datetime.today().strftime("%d%m%y")


def relatorios_solucao(dicionario_relatorios, Caminho):
    hoje = datetime.today()
    hora = hoje.hour

    # Relatório 455
    criar_relatorio(dicionario_relatorios, "455_subs", "011250", "311250", "Subs", "pegar_455", Caminho, True, 3, "A",
                    "B", "D")

    # 930 - Gera BD de Ocorrências
    if hora <= 11:
        datas_930 = get_datas_930(2)
    else:
        datas_930 = get_datas_930(1)

    for indice, data in enumerate(datas_930):
        codigo = f"930_{indice + 1}"
        criar_relatorio(dicionario_relatorios, codigo, data, data, data, "pegar_930", Caminho, True, "T", "C")

    # Relatório 150
    criar_relatorio(dicionario_relatorios, "150", PDV(1), DtAtual(), "150", "pegar_150", Caminho, False)
