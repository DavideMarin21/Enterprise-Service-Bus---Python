import logging
import os
from datetime import datetime

class LoggerHL7:
    def __init__(self, log_dir='logs', log_name='hl7'):
        os.makedirs(log_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(log_dir, f'{log_name}_{timestamp}.log')
        
        self.logger = logging.getLogger('HL7Logger')
        self.logger.setLevel(logging.DEBUG)
        
        if not self.logger.handlers:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            file_handler.setFormatter(file_formatter)
            
            self.logger.addHandler(file_handler)
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(file_formatter)
            self.logger.addHandler(console_handler)
    
    def info(self, message):
        self.logger.info(self._sanitize(message))
    
    def warning(self, message):
        self.logger.warning(self._sanitize(message))
        
    def debug(self, message):
        self.logger.debug(self._sanitize(message))
    
    def error(self, message, exc=None):
        if exc:
            self.logger.error(self._sanitize(message) , exc_info=exc)
        else:
            self.logger.error(self._sanitize(message))
    
    def _sanitize(self, message):
        """
        Opzionalmente maschera dati sensibili.
        Per ora semplicemente implementato come placeholder.
        """
        # Esempio semplice: rimpiazza codici fiscali simulati (alfanumerici di 16 caratteri)
        import re
        return re.sub(r'\b[A-Z0-9]{16}\b', '[MASKED_CF]', message)
    
    
        
        
