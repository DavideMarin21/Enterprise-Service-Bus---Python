import pika, sys, os, time
from contextlib import contextmanager
from hl7apy.parser import parse_message
from Funzioni_Database.database import connect_to_db_custom_pool
from Funzioni_Gestione_HL7.ADT_A04.ADT_A04 import ADT_A04
from Funzioni_Gestione_HL7.ADT_A23.ADT_A23 import ADT_A23
from Funzioni_Gestione_HL7.ADT_A41.ADT_A41 import ADT_A41
from Gestore_Logs.LoggerHL7 import LoggerHL7
from config_loader import carica_config_db

logger = LoggerHL7()
config_db = carica_config_db()

# --- Context manager per ottenere/rilasciare la connessione dal pool ---
@contextmanager
def db_connection_for(sorgente, config_db):
    """
    Ottiene una connessione dal pool per 'sorgente' e la rilascia sempre.
    Se la connessione non è disponibile, solleva un'eccezione.
    """
    conn = connect_to_db_custom_pool(sorgente, config_db)
    if conn is None:
        # Fallire subito: il consumer potrà fare nack + requeue
        raise RuntimeError(f"Connessione DB non disponibile per sorgente: {sorgente}")
    try:
        yield conn
    finally:
        # IMPORTANTISSIMO: rilascia la connessione al pool
        try:
            conn.close()
        except Exception as e:
            logger.warning(f"Errore nella chiusura della connessione: {e}")

def process_message(msg, conn):
    """
    Il seguente codice fa l'aggiunta e il merge di un paziente a un database.
    :param msg: oggetto HL7apy già parsato
    :param conn: connessione al database (rilasciata dal chiamante)
    """
    msg_type = msg.msh.msh_9.to_er7()
    try:
        if msg_type == 'ADT^A04':
            ADT_A04(msg, conn)
        elif msg_type == 'ADT^A41':
            ADT_A41(msg, conn)
        elif msg_type == 'ADT^A23':
            ADT_A23(msg,conn)
        else:
            logger.warning(f"Tipo messaggio non gestito: {msg_type}")
            # opzionale: solleva per far fare nack(requeue=False)
            # raise ValueError(f"Tipo messaggio non gestito: {msg_type}")
    except Exception as e:
        logger.error(f"Errore nel gestire il messaggio HL7: {e}")
        # Rilancia per gestire a livello coda
        raise

def main():
    # Nota: per produzione valuta basic_consume + callback invece di basic_get (polling)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='HL7', durable=True)
    channel.basic_qos(prefetch_count=1)

    logger.info('Inizio a gestire i messaggi HL7 in coda...')
    try:
        while True:
            method_frame, properties, body = channel.basic_get(queue='HL7', auto_ack=False)
            if not method_frame:
                # Nessun messaggio: attendo un attimo e riprovo
                time.sleep(0.5)
                continue

            text = body.decode('utf-8', errors='ignore')

            # --- Parsing sicuro del messaggio HL7 ---
            try:
                hl7_message = parse_message(text)
            except Exception as e:
                logger.error(f"Parse HL7 fallito: {e}")
                # Messaggio malformato → scarto (niente requeue)
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
                continue

            # --- Sorgente da MSH-3 con fallback ---
            sorgente = (hl7_message.msh.msh_3.value or "DEFAULT").upper()

            try:
                # Connessione per messaggio, sempre rilasciata
                with db_connection_for(sorgente, config_db) as conn:
                    process_message(hl7_message, conn)

                # Tutto ok → ACK a RabbitMQ
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                print("[ACK] Messaggio confermato\n")

            except RuntimeError as e:
                # Tipicamente problemi di risorsa (es. DB giù) → requeue per riprovare
                logger.error(f"Errore risorse (requeue): {e}")
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)

            except Exception as e:
                # Errore applicativo (dati/validazione/duplicate ecc.)
                # In molti flussi è preferibile NON requeue per non intasare la coda
                logger.error(f"Errore applicativo: {e}")
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)

    except KeyboardInterrupt:
        print("Stop richiesto.")
    finally:
        try:
            connection.close()
        except Exception:
            pass

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Programma terminato.')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)