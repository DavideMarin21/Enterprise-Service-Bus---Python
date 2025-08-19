import socketserver
from Funzioni_Database.database import close_db_connection, connect_to_db_custom_pool
from Funzioni_Gestione_HL7.ADT_A04 import ADT_A04
from Funzioni_Gestione_HL7.ADT_A23 import ADT_A23
from Funzioni_Gestione_HL7.ADT_A41 import ADT_A41
from hl7apy.parser import parse_message
from Funzioni_Gestione_HL7.funzioni_per_HL7 import mappa_campi_hl7
from Gestore_Logs.LoggerHL7 import LoggerHL7
from config_loader import carica_config_db

# MLLP delimiters
START_BLOCK = b'\x0b'
END_BLOCK = b'\x1c'
CARRIAGE_RETURN = b'\x0d'

# Inizializza logger globale
config_db = carica_config_db()
logger = LoggerHL7 ()

class MLLPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        logger.info(f"Connessione aperta da {self.client_address}")
        
        data = b''
        while True:
            chunk = self.request.recv(1024)
            if not chunk:
                break
            data += chunk

            if END_BLOCK in data:
                try:
                    # Estrai messaggio HL7 completo
                    start = data.find(START_BLOCK) + 1
                    end = data.find(END_BLOCK)
                    raw_hl7 = data[start:end].decode('utf-8')
                    logger.debug(f"Messaggio HL7 ricevuto:\n{raw_hl7}")

                    # Parsing HL7
                    try:
                        hl7_message = parse_message(raw_hl7)
                        logger.info("Parsing del messaggio HL7 riuscito")
                    except Exception as e:
                        logger.error(f"Errore nel parsing del messaggio HL7: {e}")
                        return

                    # Identifica tipo messaggio
                    msg_type = hl7_message.msh.msh_9.to_er7()
                    logger.info(f"Tipo di messaggio ricevuto: {msg_type}")

                    # Mappa i campi necessari
                    try:
                        campi = mappa_campi_hl7(hl7_message)
                        logger.debug(f"Campi mappati dal messaggio HL7: {campi}")
                    except Exception as e:
                        logger.error(f"Errore durante la mappatura dei campi HL7 {e}")
                        return
                    
                    sorgente = hl7_message.msh.msh_3.value.upper()
                    logger.info(f"Sorgente del messaggio: {sorgente}")
                    
                    fonti = config_db.get('fonti', {})
                    
                    if sorgente in config_db.get('fonti', {}):
                        logger.info(f"Configurazione DB per {sorgente}")
                        db_key = sorgente
                    else:
                        logger.warning(f"Sorgente {sorgente} non configurata nel database. Uso DEFAULT.")
                        db_key = 'DEFAULT'
                    # Connessione al DB
                    conn = connect_to_db_custom_pool(db_key, config_db)
                    if conn is None:
                        logger.error("Connessione al database fallita")
                        return

                    if msg_type == 'ADT^A04':
                        ADT_A04(hl7_message, conn)
                    elif msg_type == 'ADT^A41':
                        ADT_A41(hl7_message, conn)
                    elif msg_type == 'ADT^A23':
                        ADT_A23(hl7_message, conn)
                    else:
                        logger.warning(f"Tipo messaggio non gestito: {msg_type}")

                    # Chiudi connessione DB
                    close_db_connection(conn)
                    
                    # Invia ACK
                    ack_message = '\x0bMSH|^~\\&|SERVER|HOSPITAL|CLIENTAPP|HOSPITAL|{}||ACK^{}|ACK001|P|2.5\rMSA|AA|MSG001\x1c\r'.format(
                        hl7_message.msh.msh_7.to_er7(),
                        hl7_message.msh.msh_9.to_er7()
                    )
                    self.request.sendall(ack_message.encode('utf-8'))
                    logger.debug("ACK positivo inviato")

                except Exception as e:
                    logger.error(f"Errore nella gestione del messaggio HL7 {e}")

        logger.info(f"Connessione chiusa da {self.client_address}")
        