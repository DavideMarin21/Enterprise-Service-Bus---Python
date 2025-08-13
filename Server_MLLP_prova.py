import socketserver
from LoggerHL7 import LoggerHL7  # Logging custom
from config_loader import carica_config_db
from ACK import risposta_ack
from hl7_routing import routing_key_from_hl7
import RabbitPublisher

# Delimitatori MLLP
START_BLOCK = b'\x0b'
END_BLOCK = b'\x1c'
CARRIAGE_RETURN = b'\x0d'

# Inizializzo il logger
logger = LoggerHL7()
# Carico il file config.yaml
config_db = carica_config_db()
logger.debug(f"Configurazione YAML caricata")


class MLLPHandler(socketserver.BaseRequestHandler):
    publisher = None
    def handle(self):
        logger.info(f"Connessione aperta da {self.client_address}")
        
        data = b''
        while True:
            chunk = self.request.recv(1024)
            if not chunk:
                break
            data += chunk
            
            if END_BLOCK in data:
                # Estrai il messaggio HL7 completo
                start = data.find(START_BLOCK) + 1
                end = data.find(END_BLOCK)
                raw_hl7 = data[start:end].decode('utf-8')
                logger.info(f"Messaggio HL7 ricevuto:\n{raw_hl7}")
                try:
                    # Generazione dell'ACK
                    ACK = risposta_ack(raw_hl7, 'AA')
                    self.request.sendall(ACK.encode('utf-8'))
                    logger.info("ACK positivo inviato")
                    
                except Exception as e:
                    logger.error(f"Errore durante la gestione del messaggio HL7: {e}")
                    # Invia un ACK di errore
                    ACK = risposta_ack(raw_hl7, 'AR', tipo_errore=str(e))
                    self.request.sendall(ACK.encode('utf-8'))
                    logger.info("ACK di errore inviato")

                try:
                    # Chiamo Rabbit Publisher
                    MLLPHandler.publisher.publish(raw_hl7)
                    logger.info("Messaggio pubblicato su RabbitMQ")
                except Exception as e:
                    logger.error(f"Errore durante la pubblicazione su RabbitMQ: {e}")


            else :
                logger.info(f'Tipo messaggio sconosciuto')
                self.request.sendall('Tipo di messaggio sconosciuto'.encode('utf-8'))

        logger.info(f'Connessione chiusa da {self.client_address}')