import threading
from invio_http import invio_http
from LoggerHL7 import LoggerHL7

logger = LoggerHL7()

def dispatch_message(message, destinazioni):
    for destinazione in destinazioni:
        tipo = destinazione.get('tipo')
        thread = threading.Thread(target=smista, args=(tipo, message, destinazione))
        thread.start()
    

def smista(tipo, message, destinazione):
    if tipo == 'http':
        invio_http(message, destinazione)
        logger.info(f'Messaggio inviato via HTTP a {destinazione.get("url")}')  
    else:
        logger.error(f'Tipo di destinazione sconosciuto: {tipo}')