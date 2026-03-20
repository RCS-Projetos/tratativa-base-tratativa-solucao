import logging
from logging.handlers import TimedRotatingFileHandler
import os

LOG_FILENAME = 'file.log'
LOG_DIR = 'logs'


def setup_logging(log_level=logging.INFO):
    """Configura o sistema de logging com console e rotação de arquivo."""

    # 1. Cria o diretório de logs se não existir
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file_path = os.path.join(LOG_DIR, LOG_FILENAME)

    # 2. Define o Formatter (o que aparece no log)
    # Inclui Contexto: nome do arquivo/função, data e nível
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )

    # 3. Cria o Logger principal
    # Define o nível MÍNIMO de severidade para ser PROCESSADO
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove handlers antigos para evitar logs duplicados
    if root_logger.handlers:
        root_logger.handlers = []

    # --- HANDLER 1: Console (para feedback imediato) ---
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)  # Apenas INFO e acima no console
    root_logger.addHandler(stream_handler)

    # --- HANDLER 2: Arquivo com Rotação (para auditoria) ---
    # Rotação: Cria um novo arquivo a cada dia (midnight) e mantém os últimos 7 arquivos
    file_handler = TimedRotatingFileHandler(
        log_file_path,
        when="midnight",
        interval=1,
        backupCount=7,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)  # Tudo no arquivo
    root_logger.addHandler(file_handler)

    # Informa que o logging foi iniciado
    root_logger.info("Sistema de Logging Inicializado com sucesso.")
    return root_logger
