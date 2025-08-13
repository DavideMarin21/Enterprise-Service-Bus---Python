import logging
import os
from logging.handlers import TimedRotatingFileHandler

file_handler = TimedRotatingFileHandler(
    'hl7.log', when='midnight', interval=1, backupCount=7
)
class LoggerHL7:
    def __init__(self, name='HL7Logger', log_file='hl7.log', level=logging.DEBUG):
        logs_dir = 'logs'
        os.makedirs
        log_path = os.path.join(logs_dir, log_file)
        
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        if not self.logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] %(message)s', 
                datefmt='%Y-%m-%d %H:%M:%S'
            )

            file_handler = logging.FileHandler(log_path)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            self.logger.addHandler(stream_handler)

    def debug(self, message): self.logger.debug(message)
    def info(self, message): self.logger.info(message)
    def warning(self, message): self.logger.warning(message)
    def error(self, message): self.logger.error(message)
    def critical(self, message): self.logger.critical(message)   
    
        
        
