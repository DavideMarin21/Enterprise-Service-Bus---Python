import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

"""
Classe per gestire i logs.
Nella cartella logs crea ogni giorno un file log denominato: logs_%d%m%Y.log
"""

class LoggerHL7:
    def __init__(self, name='HL7Logger', level=logging.DEBUG):
        logs_dir = '../logs'
        os.makedirs(logs_dir, exist_ok=True)

        # Nome del file log giornaliero
        log_filename = datetime.now().strftime("logs_%d%m%Y.log")
        log_path = os.path.join(logs_dir, log_filename)

        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        if not self.logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

            # File handler con rotazione giornaliera
            file_handler = TimedRotatingFileHandler(
                filename=os.path.join(logs_dir, "logs.log"),  # file "base"
                when="midnight",
                interval=1,
                backupCount=7,
                encoding="utf-8",
                utc=False
            )

            # Sovrascrive il nome fisico dei file rotati con data
            file_handler.suffix = "%d%m%Y.log"
            file_handler.extMatch = r"^\d{8}\.log$"

            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

            # Stampa anche a console
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            self.logger.addHandler(stream_handler)

    def debug(self, message): self.logger.debug(message)
    def info(self, message): self.logger.info(message)
    def warning(self, message): self.logger.warning(message)
    def error(self, message): self.logger.error(message)
    def critical(self, message): self.logger.critical(message)