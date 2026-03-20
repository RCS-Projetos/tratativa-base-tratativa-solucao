from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import *
from selenium.webdriver.common.by import By
from time import sleep
from datetime import datetime
import shutil
import os
from dotenv import load_dotenv
from CapSolver import capsolver
from utils.utils import obter_hora_servidor_win, hora_com_tolerancia_v2, get_data
import logging


LOGGER = logging.getLogger(__name__)


class Automacao:
    def __init__(self, codigo, relatorio, dt_inicial, dt_final, nome, pasta_temp, caminho, is_156, inicio_automacao, funcao, parametro_1, parametro_2, parametro_3, parametro_4, parametro_5):
        # Verificar se a pasta "Fontes" existe no diretório atual e cria
        self.criar_pasta("Fontes")

        self.criar_pasta(pasta_temp)

        # Set atributos
        self.codigo = codigo
        self.relatorio = relatorio
        self.dt_inicial = dt_inicial
        self.dt_final = dt_final
        self.nome = nome
        self.caminho = caminho
        self.is_156 = is_156
        self.inicio_automacao = inicio_automacao
        self.funcao = getattr(self, funcao)
        self.parametro_1 = parametro_1
        self.parametro_2 = parametro_2
        self.parametro_3 = parametro_3
        self.parametro_4 = parametro_4
        self.parametro_5 = parametro_5
        self.tempo_referencia = None
        self.tempo_referencia_mais_um_segundo = None
        self.tempo_referencia_mais_dois_segundo = None
        self.tempo_referencia_menos_um_segundo = None

        # Configuração do WebDriver e WebDriverWait...
        # options = ["--start-maximized", "--disable-extensions", "--disable-infobars"]
        # options = ["--start-maximized", "--disable-extensions", "--disable-infobars", "--headless=old"]
        options = ["--start-maximized","--disable-notifications", "--disable-extensions", "--disable-infobars", "--headless"]
        chrome_options = Options()
        for option in options:
            chrome_options.add_argument(option)

        # Configuração do diretório de download
        self.pasta_downloads = pasta_temp
        prefs = {
            "download.default_directory": self.pasta_downloads,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_setting_values.automatic_downloads": 1  # Permite múltiplos downloads automáticos
        }
        chrome_options.add_experimental_option('prefs', prefs)

        # service = Service(ChromeDriverManager(driver_version="130.0.6723.92").install())
        service = Service(ChromeDriverManager().install())
        # service = Service(r"C:\Users\DIVOP\AppData\Local\Programs\Python\Python312\chromedriver.exe")
        # service = Service(r"C:\Users\camilodossantos\Documents\Operacional\chromedriver\chromedriver.exe")
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(
            driver=self.driver,
            timeout=120,
            poll_frequency=1,
            ignored_exceptions=[NoSuchElementException, ElementNotVisibleException, ElementNotSelectableException])

    def iniciar(self):
        load_dotenv()

        lista_campos = ["f1", "f2", "f3", "f4"]
        self.driver.get("https://sistema.ssw.inf.br/")

        LOGGER.info(f"Fazendo login: Relatório: {self.relatorio}")

        self.wait.until(EC.element_to_be_clickable((By.NAME, "f1")))
        self.limpar_imputs(lista_campos)

        self.preencher_inputs("f1", os.getenv("DOMINIO"))
        self.preencher_inputs("f2", os.getenv("CPF"))
        self.preencher_inputs("f3", os.getenv("USUARIO"))
        self.preencher_inputs("f4", os.getenv("SENHA"))
        self.driver.find_element(By.ID, "5").click()
        sleep(5)

        if self.verifica_se_erro():
            LOGGER.info(f"Erro ao executar o relatório {self.codigo}. Reiniciando o sistema")
            self.reininciar_sistema()

        self.verificar_e_fechar_janelas_extras()

        # Espera o link no final da pagina aparecer. Sinal que a pagina foi carregada
        self.wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "baselnk")))
        LOGGER.info(f"Página inicial do SSW carregada. Automação {self.relatorio}")
        sleep(1)

    def reininciar_sistema(self):
        self.inicio_automacao = datetime.now()
        self.baixar_relatorio()

    def preencher_inputs(self, nome, preencher):
        nome_input = self.driver.find_element(By.NAME, nome)
        nome_input.send_keys(preencher)
        sleep(0.5)

    def limpar_imputs(self, lista_nome):
        for nome in lista_nome:
            self.driver.find_element(By.NAME, nome).clear()

    def verifica_se_erro(self):
        try:
            tela_erro = WebDriverWait(self.driver, 7).until(EC.visibility_of_element_located((By.ID, "errormsg")))
            sleep(1)

            # tela_erro = self.driver.find_element(By.ID, "errormsg")
            sleep(1)
            if len(tela_erro.text) > 0:
                print(f"{tela_erro.text.strip()}")
                # self.logger.error(f"{tela_erro.text.strip()}")
                return True
            else:
                return False

        except TimeoutException:
            LOGGER.info("Erro não ficou visível. Operação realizada com sucesso")
            return False

        except NoSuchElementException as e:
            LOGGER.error(f"Erro: {e}")
            return True

    def criar_pasta(self, pasta):
        if not os.path.exists(pasta):
            os.mkdir(pasta)

    def excluir_pasta(self, pasta):
        if os.path.exists(pasta):
            os.rmdir(pasta)

    def esperar_progresso_visivel(self):
        try:
            # Esperar até que o elemento com id "procimg" seja visível
            self.wait.until(EC.visibility_of_element_located((By.ID, "procimg")))
        except Exception as e:
            LOGGER.error(f"{self.relatorio} : Erro ao esperar progresso visível: {str(e)}")

    def esperar_progresso_invisivel(self):
        try:
            # Esperar até que o elemento com id "procimg" não seja visível
            self.wait.until(EC.invisibility_of_element_located((By.ID, "procimg")))
        except Exception as e:
            LOGGER.error(f"{self.relatorio} : Erro ao esperar progresso invisível: {str(e)}")

    def esperar_download(self, caminho):
        tempo_max_espera = 60  # Tempo máximo para esperar o download (em segundos)
        # tempo_inicio = time()

        download_em_andamento = True

        while download_em_andamento:
            arquivos = os.listdir(caminho)
            for arquivo in arquivos:
                # Verificar se o arquivo tem a extensão .crdownload
                if arquivo.endswith('.crdownload'):
                    LOGGER.info(f"{self.relatorio} : Download em andamento.")
                    sleep(1)

                else:
                    sleep(3)
                    LOGGER.info(f"{self.relatorio} : Acabou download")
                    download_em_andamento = False

        return False

    def renomear_arquivo(self, pasta_temp, nome_arquivo):
        arquivos = os.listdir(pasta_temp)
        for arquivo in arquivos:
            arquivo_antigo = os.path.join(pasta_temp, arquivo)
            arquivo_renomeado = os.path.join(pasta_temp, nome_arquivo)
            os.rename(arquivo_antigo, arquivo_renomeado)

    def remover_arquivos_na_pasta(self, pasta_temp):
        if os.path.isdir(pasta_temp):
            for filename in os.listdir(pasta_temp):
                file_path = os.path.join(pasta_temp, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)  # Remove arquivos e links simbólicos
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)  # Remove subdiretórios
                except Exception as e:
                    LOGGER.error(f'{self.relatorio} : Falha ao deletar {file_path}. Razão: {e}')

    def pesquisar(self, codigo, unidade=None):
        sleep(1)

        janela_inicial = self.driver.current_window_handle

        if unidade is not None:
            self.driver.find_element(By.NAME, "f2").clear()
            self.driver.find_element(By.NAME, "f2").send_keys(unidade)
            sleep(1)

        opcao = self.wait.until(EC.element_to_be_clickable((By.NAME, "f3")))
        opcao.send_keys(codigo)

        while self.driver.find_element(By.ID, "procimg").get_attribute("style").split(";")[5] == " visibility: visible":
            sleep(1)
        sleep(1)

        janelas = self.driver.window_handles

        for janela in janelas:
            if janela not in janela_inicial:
                self.driver.switch_to.window(janela)

    def fechar_janela(self):
        if self.driver:
            self.driver.close()

            janelas = self.driver.window_handles
            self.driver.switch_to.window(janelas[0])
            LOGGER.info(f"{self.relatorio} : Página principal")

            # Espera o link no final da pagina aparecer. Sinal que a pagina foi carregada
            self.wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "baselnk")))
            sleep(1)

    def copiar_arquivos(self, pasta_temp, pasta):
        arquivos = os.listdir(pasta_temp)
        for arquivo in arquivos:
            caminho_antigo = os.path.join(pasta_temp, arquivo)
            caminho_novo = os.path.join(pasta, arquivo)
            shutil.copy2(caminho_antigo, caminho_novo)
            LOGGER.info(f"{self.relatorio} : Arquivo copiado para {caminho_novo}")

    def mover_arquivo(self, pasta_temp, pasta):
        arquivos = os.listdir(pasta_temp)
        for arquivo in arquivos:
            caminho_antigo = os.path.join(pasta_temp, arquivo)
            caminho_novo = os.path.join(pasta, arquivo)
            shutil.move(caminho_antigo, caminho_novo)
            LOGGER.info(f"{self.relatorio} : Arquivo movido para {caminho_novo}")

    def pegar_150(self):
        LOGGER.info("150 - CTRCs Atrasados de Entrega")
        # self.logger.info("150 - CTRCs Atrasados de Entrega")
        self.remover_arquivos_na_pasta(self.pasta_downloads)
        sleep(3)

        self.wait.until(EC.element_to_be_clickable((By.NAME, "f1")))
        self.driver.find_element(By.NAME, "f1").clear()
        sleep(1)
        self.driver.find_element(By.NAME, "f2").clear()
        sleep(1)

        self.driver.find_element(By.NAME, "f1").send_keys(self.dt_inicial)
        sleep(1)
        self.driver.find_element(By.NAME, "f2").send_keys(self.dt_final)

        LOGGER.info(f"Emissão do CTRC (opc): {self.dt_inicial} a {self.dt_final}")
        # self.logger.info(f"Emissão do CTRC (opc): {dtInicial} a {dtFinal}")

        sleep(2)
        self.driver.find_element(By.NAME, "f8").clear()
        sleep(1)
        self.driver.find_element(By.NAME, "f8").send_keys("S")

        LOGGER.info("Excel: S")
        # self.logger.info("Excel: S")

        sleep(2)
        self.driver.find_element(By.ID, "10").click()

        # Esperar até que o progresso seja visível
        self.esperar_progresso_visivel()

        # Esperar até que o progresso não seja mais visível
        self.esperar_progresso_invisivel()
        sleep(1)

        self.esperar_download(self.pasta_downloads)
        sleep(1)

        # Move o arquivo baixado para a pasta Solução com o nome 150.csv
        nome_arquivo = self.nome + ".csv"
        self.renomear_arquivo(self.pasta_downloads, nome_arquivo)
        sleep(1)

        caminho_pasta = self.caminho + os.sep + "Cockpit 3.0" + os.sep + "Fontes" + os.sep + "Solução" + os.sep + "150"

        caminho_pasta_2 = self.caminho + os.sep + "Cockpit 3.0" + os.sep + "Fontes" + os.sep + "Solução"
        self.copiar_arquivos(self.pasta_downloads, caminho_pasta_2)

        self.mover_arquivo(self.pasta_downloads, caminho_pasta)
        caminho_completo = caminho_pasta + os.sep + nome_arquivo
        # self.logger.info(f"Relatório salvo no caminho: {caminho_completo}")
        sleep(1)
        LOGGER.info(f"Arquivo salvo em: {caminho_completo}")

        self.fechar_janela()

    def desmarcar_opcoes_excel(self, lista_opcoes):
        LOGGER.info("Desmarcando as opções do excel se estiver marcada")
        self.driver.find_element(By.ID, "36").click()
        sleep(3)
        janelas = self.driver.window_handles
        self.driver.switch_to.window(janelas[-1])

        tabela_completa = self.driver.find_elements(By.XPATH, "//tr[contains(@class,'srtr')]")

        # Exclui o cabeçalho e o rodapé da tabela
        linhas = tabela_completa[1:-1]

        for linha in linhas:
            # pega o texto da 3ª coluna (onde está o nome)
            nome = linha.find_element(By.XPATH, ".//td[3]//div").text.strip()

            if nome in lista_opcoes:
                checkbox = linha.find_element(By.XPATH, ".//input[@type='checkbox']")
                if checkbox.is_selected():
                    LOGGER.info(f"Desmarcando opção {nome}")
                    checkbox.click()
                    sleep(1)

        sleep(1)
        self.driver.find_element(By.ID, "127").click()
        sleep(2)
        janelas = self.driver.window_handles
        self.driver.switch_to.window(janelas[-1])

    def pegar_455(self):
        sleep(3)
        # self.logger.info(f"455 - Fretes Expedidos/Recebidos - CTRCs")
        self.wait.until(EC.element_to_be_clickable((By.NAME, "f9")))

        lista_nomes = ["f9", "f10", "f11", "f12", "f13", "f14", "f15", "f16"]

        self.limpar_imputs(lista_nomes)

        if self.parametro_1 == 1:
            LOGGER.info("455 - Período de Emissão")
            # self.logger.info("455 - Período de Emissão")
            self.driver.find_element(By.NAME, "f9").send_keys(self.dt_inicial)
            sleep(1)
            self.driver.find_element(By.NAME, "f10").send_keys(self.dt_final)
            LOGGER.info(f"Período de emissão: {self.dt_inicial} a {self.dt_final}")
            # self.logger.info(f"Período de emissão: {self.dt_inicial} a {self.dt_final}")

        elif self.parametro_1 == 2:
            LOGGER.info("455 - Período de Autorização")
            # self.logger.info("455 - Período de Autorização")
            self.driver.find_element(By.NAME, "f11").send_keys(self.dt_inicial)
            sleep(1)
            self.driver.find_element(By.NAME, "f12").send_keys(self.dt_final)
            LOGGER.info(f"Período de aurorização: {self.dt_inicial} a {self.dt_final}")
            # self.logger.info(f"Período de aurorização: {self.dt_inicial} a {self.dt_final}")

        elif self.parametro_1 == 3:
            LOGGER.info("455 - Previsão de Entrega")
            # self.logger.info("455 - Previsão de Entrega")
            self.driver.find_element(By.NAME, "f13").send_keys(self.dt_inicial)
            sleep(1)
            self.driver.find_element(By.NAME, "f14").send_keys(self.dt_final)
            LOGGER.info(f"Previsão de entrega: {self.dt_inicial} a {self.dt_final}")
            # self.logger.info(f"Previsão de entrega: {self.dt_inicial} a {self.dt_final}")

        elif self.parametro_1 == 4:
            LOGGER.info("455 - Período de Entrega")
            # self.logger.info("455 - Período de Entrega")
            self.driver.find_element(By.NAME, "f15").send_keys(self.dt_inicial)
            sleep(1)
            self.driver.find_element(By.NAME, "f16").send_keys(self.dt_final)
            LOGGER.info(f"Período de entrega: {self.dt_inicial} a {self.dt_final}")
            # self.logger.info(f"Período de entrega: {self.dt_inicial} a {self.dt_final}")

        # Arquivo sempre é Excel -> E
        sleep(2)
        self.driver.find_element(By.XPATH, "//input[@name='f35']").clear()
        sleep(1)
        self.driver.find_element(By.XPATH, "//input[@name='f35']").send_keys("E")
        LOGGER.info(f"Arquivo: E")
        # self.logger.info(f"Arquivo: {tpArquivo}")
        sleep(1)

        # Preenchimento dos dados complementares conforme a opção desejada
        self.driver.find_element(By.NAME, "f37").send_keys(self.parametro_2)
        sleep(1)
        self.driver.find_element(By.NAME, "f38").send_keys(self.parametro_3)
        sleep(1)
        self.driver.find_element(By.NAME, "f39").send_keys(self.parametro_4)
        LOGGER.info(f"Dados complementares: {self.parametro_2} | {self.parametro_3} | {self.parametro_4}\n")
        # self.logger.info(f"Dados complementares: {opCompl1} | {opCompl2} | {opCompl3}\n")
        sleep(2)

        lista_opc_config_excel = ["Base Calculo IBS/CBS", "Aliquota IBS Municipal", "Valor IBS Municipal",
                                  "Aliquota IBS Estadual", "Valor IBS Estadual", "Aliquota CBS", "Valor CBS",
                                  "CO2", "RDC", "Seguro Fluvial", "Redespacho Fluvial"]
        self.desmarcar_opcoes_excel(lista_opc_config_excel)

        # lista_opc_config_excel = ["87", "88", "89", "90"]
        # self.desmarcar_opcoes_excel(lista_opc_config_excel)

        # Botao do play id=40
        self.driver.find_element(By.ID, "40").click()
        # self.tempo_referencia = datetime.now()
        try:
            self.tempo_referencia = obter_hora_servidor_win()
        except Exception as e:
            self.tempo_referencia = datetime.now()
        LOGGER.info(f"Tempo de referencia relatorio {self.relatorio}: {self.tempo_referencia.strftime("%d/%m/%y %H:%M:%S")}")
        sleep(1)
        self.fechar_janela()

    def pegar_930(self):
        sleep(1)
        LOGGER.info("930 - Gera BD de Ocorrências")
        # self.logger.info("930 - Gera BD de Ocorrências")
        self.wait.until(EC.element_to_be_clickable((By.NAME, "f1")))

        # LOGGER.info("Resolvendo recaptcha")
        #
        # try:
        #     # Resolvendo o recaptcha
        #     api_key = os.getenv("API_KEY")
        #     site_url = self.driver.current_url
        #     site_key = self.driver.find_element(By.ID, "captcha").get_attribute("data-sitekey")
        #
        #     token = capsolver(api_key, site_key, site_url)
        #     LOGGER.info(f"Token - {self.relatorio}: {token}")
        #
        #     if token:
        #         sleep(1)
        #         self.driver.execute_script(f"document.getElementById('g-recaptcha-response').innerHTML = '{token}'")
        #         sleep(2)
        #     else:
        #         print("Falha ao pegar o token. Reiniciando...")
        #         self.fechar_janela()
        #         self.baixar_relatorio()
        # except Exception as e:
        #     LOGGER.info(f"Erro ao resolver o recaptcha: {e}.")
        #     self.fechar_janela()
        #     self.baixar_relatorio()

        elements = self.driver.find_elements(By.ID, "captcha")

        if elements:
            try:
                print("Resolvendo recaptcha")
                # Resolvendo o recaptcha
                api_key = os.getenv("API_KEY")
                site_url = self.driver.current_url
                site_key = self.driver.find_element(By.ID, "captcha").get_attribute("data-sitekey")

                token = capsolver(api_key, site_key, site_url)
                LOGGER.info(f"Token - {self.relatorio}: {token}")

                if token:
                    sleep(1)
                    self.driver.execute_script(f"document.getElementById('g-recaptcha-response').innerHTML = '{token}'")
                    sleep(2)
                else:
                    LOGGER.info("Falha ao pegar o token. Reiniciando...")
                    self.fechar_janela()
                    self.baixar_relatorio()
            except Exception as e:
                LOGGER.error(f"Erro ao resolver o recaptcha: {e}.")
                self.fechar_janela()
                self.baixar_relatorio()
        else:
            LOGGER.info("Captcha não existe")

        # Preenchendo os inputs
        lista_nome = ["f1", "f2", "f3", "f5", "f8", "f9"]
        sleep(1)
        self.limpar_imputs(lista_nome)
        sleep(1)

        self.preencher_inputs("f1", self.dt_inicial)
        self.preencher_inputs("f2", self.dt_final)
        self.preencher_inputs("f3", self.parametro_1)
        self.preencher_inputs("f5", "")
        self.preencher_inputs("f8", "")
        self.preencher_inputs("f9", self.parametro_2)

        LOGGER.info(f"Data de ocorrência: {self.dt_inicial} a {self.dt_final}")
        # self.logger.info(f"Data de ocorrência: {data_inicial} a {data_final}")

        try:
            self.tempo_referencia = obter_hora_servidor_win()
        except Exception as e:
            self.tempo_referencia = datetime.now()
        LOGGER.info(f"Tempo de referencia relatorio {self.relatorio}: {self.tempo_referencia.strftime("%d/%m/%y %H:%M:%S")}")
        sleep(1)
        self.fechar_janela()

    def fechar_ssw(self):
        self.driver.quit()
        sleep(3)

    def pegar_156(self):
        LOGGER.info(f"{self.relatorio} : 156 - Fila de processamento em lotes")
        while True:
            botao_atualizar = self.wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Atualizar")))
            botao_atualizar.click()
            LOGGER.info(f"{self.relatorio} : Esperando o carregamento dos arquivos")
            sleep(5)

            linhas = self.separar_dados_em_linhas()

            linhas_cco = self.agrupar_linhas(linhas)

            linhas_definitivas = self.agrupar_linhas_definitiva(linhas_cco)

            for linha in linhas_definitivas:
                if linha[6] != "Concluído":
                    break

            else:
                break

        return linhas_definitivas

    def separar_dados_em_linhas(self):

        tentativas = 0
        linhas = []
        max_tentativas = 5

        while tentativas < max_tentativas:
            try:
                dados = self.driver.find_elements(By.CSS_SELECTOR, "tr.srtr2 td")

                linhas = []
                sublista_auxiliar = []

                for i, item in enumerate(dados):
                    sublista_auxiliar.append(item.text)

                    if (i + 1) % 9 == 0 or i == len(dados) - 1:
                        linhas.append(sublista_auxiliar)
                        sublista_auxiliar = []

                if linhas:  # Se as linhas não estiverem vazias, retorná-las
                    return linhas

            except Exception as e:
                LOGGER.error(f"{self.relatorio} : Tentativa {tentativas + 1} falhou. Erro: {e}")

            tentativas += 1

        # Se atingir o número máximo de tentativas e ainda falhar, levanta uma exceção ou retorna uma lista vazia
        raise Exception(f"{self.relatorio} : Erro ao separar dados em linhas após várias tentativas.")

    def agrupar_linhas(self, linhas):
        linhas_rpa = []

        for linha in linhas:

            # Ignorar o cabeçalho
            if linha[0] == "Sequência":
                continue

            # Ignorar a última linha se for a de paginação
            if linha[0].strip() == "":
                continue

            if linha[3] == "rpa" and linha[6] != "Abortado":
                linhas_rpa.append(linha)

        return linhas_rpa

    def agrupar_linhas_definitiva(self, linhas):
        linhas_definitivas = []
        linhas_aux = []

        lista_hora_tolerancia = hora_com_tolerancia_v2(self.tempo_referencia)

        for linha in linhas:
            linha.append(self.relatorio)

            data_formatada = linha[2]
            formato = "%d/%m/%y %H:%M:%S"
            data_objeto = datetime.strptime(data_formatada, formato)
            data_string = data_objeto.strftime("%H:%M:%S")

            if data_string in lista_hora_tolerancia:
                condicao = True
            else:
                condicao = False
            linha.append(condicao)

            linhas_aux.append(linha)

        for linha in linhas_aux:
            if linha[10]:
                linhas_definitivas.append(linha)

        return linhas_definitivas

    def salvar_455(self, lista):
        linhas = lista
        lista_455 = []
        nome_arquivo = ""
        caminho_pasta = ""
        caminho_completo = ""
        self.remover_arquivos_na_pasta(self.pasta_downloads)

        for linha in linhas:
            if linha[1][:3] == "455":
                lista_455.append(linha)

        LOGGER.info("--------------------------------------  LISTA DE 455 ------------------------------------------")
        for linha in lista_455:
            LOGGER.info(linha)
        LOGGER.info("-----------------------------------------------------------------------------------------------")

        for indice, item in enumerate(lista_455):
            codigo = item[0]
            status = item[8]

            if status == "Baixar":

                if self.relatorio == "455_subs":
                    caminho_pasta = self.caminho + os.sep + "Cockpit 3.0" + os.sep + "Fontes" + os.sep + "455" + os.sep + "Subs"

                    if not os.path.exists(caminho_pasta):
                        os.makedirs(caminho_pasta)

                    xpath = '//a[@onclick="ajaxEnvia(' + "'" + f"DOW{codigo}" + "'" + ');return(false);"]'
                    element_to_click = self.driver.find_element(By.XPATH, xpath)
                    self.driver.execute_script("arguments[0].click();", element_to_click)

                    # Esperar até que o progresso seja visível
                    self.esperar_progresso_visivel()

                    # Esperar até que o progresso não seja mais visível
                    self.esperar_progresso_invisivel()
                    sleep(1)

                    self.esperar_download(self.pasta_downloads)
                    sleep(1)

                    nome_arquivo = self.nome + ".sswweb"
                    self.renomear_arquivo(self.pasta_downloads, nome_arquivo)
                    sleep(1)

                    self.mover_arquivo(self.pasta_downloads, caminho_pasta)
                    sleep(1)
                    caminho_completo = caminho_pasta + os.sep + nome_arquivo
                    LOGGER.info(f"Arquivo salvo em: {caminho_completo}")

                else:
                    LOGGER.info("Não foi possível setar o caminho")

    def salvar_930(self, lista):
        linhas = lista
        lista_930 = []

        hoje = get_data(0)

        # pasta_930_solucao = os.getcwd() + os.sep + "Fontes" + os.sep + "930"
        pasta_930_solucao = self.caminho + os.sep + "Cockpit 3.0" + os.sep + "Fontes" + os.sep + "Solução" + os.sep + "930"
        if not os.path.exists(pasta_930_solucao):
            os.makedirs(pasta_930_solucao)

        self.remover_arquivos_na_pasta(self.pasta_downloads)

        for linha in linhas:
            if linha[1][:3] == "930":
                lista_930.append(linha)

        LOGGER.info("--------------------------------------  LISTA DE 930 ------------------------------------------")
        for linha in lista_930:
            LOGGER.info(linha)
        LOGGER.info("-----------------------------------------------------------------------------------------------")

        for indice, item in enumerate(lista_930):
            codigo = item[0]
            status = item[8]

            if status == "Baixar":

                if "930" in self.relatorio:

                    xpath = '//a[@onclick="ajaxEnvia(' + "'" + f"DOW{codigo}" + "'" + ');return(false);"]'
                    element_to_click = self.driver.find_element(By.XPATH, xpath)
                    self.driver.execute_script("arguments[0].click();", element_to_click)

                    # Esperar até que o progresso seja visível
                    self.esperar_progresso_visivel()

                    # Esperar até que o progresso não seja mais visível
                    self.esperar_progresso_invisivel()
                    sleep(1)

                    self.esperar_download(self.pasta_downloads)
                    sleep(1)

                    nome_arquivo = self.nome + ".csv"
                    self.renomear_arquivo(self.pasta_downloads, nome_arquivo)
                    sleep(1)

                    self.mover_arquivo(self.pasta_downloads, pasta_930_solucao)
                    sleep(1)
                    caminho_completo = pasta_930_solucao + os.sep + nome_arquivo
                    LOGGER.info(f"Arquivo salvo em: {caminho_completo}")

    def baixar_relatorio(self):
        self.iniciar()

        if self.parametro_5 == "painel":
            self.pesquisar(self.codigo, self.parametro_1)
        else:
            self.pesquisar(self.codigo)
        self.funcao()

        if self.is_156:
            self.pesquisar("156")
            lista_156 = self.pegar_156()

            for lista in lista_156:
                if lista[1][:3] == "455":
                    self.salvar_455(lista_156)

                elif lista[1][:3] == "930":
                    self.salvar_930(lista_156)


        self.fechar_ssw()

    def verificar_e_fechar_janelas_extras(self):
        janelas = self.driver.window_handles
        # print(f"Numero de janelas aberta: {janelas}")
        if len(janelas) > 1:
            for janela in range(0, len(janelas) - 1):
                self.driver.switch_to.window(janelas[-1])
                self.fechar_janela()
