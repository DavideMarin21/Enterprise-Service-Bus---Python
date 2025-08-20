import logging
import os
from logging.handlers import TimedRotatingFileHandler

"""
Classe per gestire i logs.
Nella cartella /Users/davidemarin/VSC/Python_Server/logs
crea ogni giorno un file log denominato: logs_%d%m%Y.log
"""

class LoggerHL7:
    def __init__(self, name='HL7Logger', level=logging.DEBUG):
        # Percorso assoluto della cartella logs
        logs_dir = "/Users/davidemarin/VSC/Python_Server/logs"
        os.makedirs(logs_dir, exist_ok=True)

        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        if not self.logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

            # File handler con rotazione giornaliera
            file_handler = TimedRotatingFileHandler(
                filename=os.path.join(logs_dir, "logs"),  # file base (rinominato a mezzanotte)
                when="midnight",
                interval=1,
                backupCount=7,
                encoding="utf-8",
                utc=False
            )

            # Rinomina i file rotati con suffisso data
            file_handler.suffix = "%d%m%Y.log"

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