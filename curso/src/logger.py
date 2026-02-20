import logging
import logging.handlers
from pathlib import Path

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

def get_logger(name: str) -> logging.Logger:
    """
    Retorna um logger configurado com dois handlers:
      - Console: nível WARNING (só aparece quando algo dá errado)
      - Arquivo rotativo: nível INFO (registra sucesso e falha)
        Rotaciona ao atingir 1 MB, mantendo os últimos 5 arquivos.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # evita duplicar handlers ao importar múltiplas vezes

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # Handler 1: console — só erros e acima
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formatter)

    # Handler 2: arquivo rotativo — INFO e acima (sucesso + falha)
    file_handler = logging.handlers.RotatingFileHandler(
        filename=LOG_DIR / "pipeline.log",
        maxBytes=1 * 1024 * 1024,  # 1 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
