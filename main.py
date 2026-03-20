from utils.utils import converter_duracao, enviar_email
from multiprocessing import freeze_support
from dotenv import load_dotenv
from time import time
from datetime import datetime
from funcoes import run_automacao_solucao
from utils.log_config import setup_logging
import os
import logging


LOGGER = setup_logging(log_level=logging.INFO)


PARAMETROS_FILE = r"C:\Users\DIVOP\RODOVIARIO CAMILO DOS SANTOS FILHO LTDA\Matrix - Rodoviário Camilo dos Santos - DIVOP\Cockpit 3.0\Parametros\Parametros.xlsx"
PATH_OUTPUT = r"C:\Users\DIVOP\RODOVIARIO CAMILO DOS SANTOS FILHO LTDA\Matrix - Rodoviário Camilo dos Santos - DIVOP\Cockpit 3.0\Fontes\Solução"


if __name__ == "__main__":
    freeze_support()  # Isso é importante para o PyInstaller no Windows
    load_dotenv()

    LOGGER.info("======= INICIO DA AUTOMAÇÃO SOLUÇÃO ========")
    inicio = time()
    hoje = datetime.today()
    hora = hoje.hour

    Caminho = r"C:\Users\DIVOP\RODOVIARIO CAMILO DOS SANTOS FILHO LTDA\Matrix - Rodoviário Camilo dos Santos - DIVOP"
    relatorios = {}

    try:
        run_automacao_solucao(relatorios, Caminho, PARAMETROS_FILE, PATH_OUTPUT)
    except KeyboardInterrupt:
        LOGGER.warning("Automação interrompida manualmente pelo usuário (Ctrl+C).")
        LOGGER.info("======= FIM DA AUTOMAÇÃO SOLUÇÃO ========")
        import sys
        sys.exit(0)

    # Obtém a data e hora atuais
    current_datetime = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    fim = time()
    duracao = fim - inicio
    tempo_automacao = converter_duracao(duracao)
    LOGGER.info(f"A automação completa levou {tempo_automacao} para ser concluída.")

    assunto = "Atualização Portal Solução"

    html_message = f"""
        <html>
          <body style="font-family: Arial, sans-serif; color: #333;">
            <h2 style="color: #2c3e50;">✅ Portal Solução Concluída</h2>
            <p>Olá,</p>
            <p>Segue o resumo da última execução da automação:</p>

            <table style="border-collapse: collapse; width: 100%; margin-top: 10px;">
              <tr style="background-color: #f2f2f2;">
                <td style="padding: 8px; border: 1px solid #ddd;">📅 Data e hora</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{current_datetime}</td>
              </tr>
              <tr>
                <td style="padding: 8px; border: 1px solid #ddd;">⚙️ Tempo total da automação</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{tempo_automacao}</td>
              </tr>
            </table>

            <p style="margin-top: 20px;">Atenciosamente,<br><b>Automações Camilo dos Santos</b></p>
          </body>
        </html>
        """

    # from_addr = "automacaoticamilodossantos@gmail.com"
    from_addr = os.getenv("EMAIL_AUTOMACAO")
    # to_addr = 'pedro.goncalves@camilodossantos.com.br'
    to_addr = os.getenv("EMAIL_DEST")
    # password = "bmtj znxn cxcj zjga"
    password = os.getenv("SENHA_APP_GOOGLE")
    enviar_email(assunto, html_message, from_addr, to_addr, password)

    LOGGER.info("======= FIM DA AUTOMAÇÃO SOLUÇÃO ========")
