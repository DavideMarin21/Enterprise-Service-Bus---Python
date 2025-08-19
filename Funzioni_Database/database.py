import mysql.connector
from mysql.connector import Error, pooling
from Gestore_Logs.LoggerHL7 import LoggerHL7

logger = LoggerHL7()

# Funzione per connettersi al database MySQL gia impostato
def connect_to_db():
    try:
        conn = mysql.connector.connect(
            host ='localhost',
            port = 3306,
            user = 'root',
            password = 'root',
            database = 'HL7_Db'
       )
        if conn.is_connected():
            #print("Connessione al database riuscita")
            logger.info("Connessione al database riuscita")
            return conn
    except Error as e:
        #print("Errore durante la connessione al database:", e)
        logger.error(f"Errore durante la connessione al database {e}")
        return None 

pool_cache = {}  # Cache per i pool di connessioni
# Funzione per connettersi al database MySQL con configurazione da file YAML tramite pool di connessioni
def connect_to_db_custom_pool(sorgente, config_db):
    """
    :param sorgente: Da dove arriva il messaggio HL7 e nella parte MSH 3
    :param config_db: Le configurazioni del database
    :return: Conn = la connessione al database
    """
    sorgente_normalizzata = sorgente.replace(' ', '_').upper()
    fonti = config_db.get('fonti', {})
    logger.debug(f"Chiavi presenti in fonti: {list(fonti.keys())}")
    logger.debug(f"Chiave cercata: {sorgente_normalizzata}")
    
    if sorgente_normalizzata not in fonti:
        logger.warning(f"Sorgente '{sorgente}' non trovata, uso DEFAULT.")
        sorgente_normalizzata = 'DEFAULT'
        
    info = fonti.get(sorgente_normalizzata)
    if not info:
        logger.error(f"Nessuna configurazione trovata per '{sorgente_normalizzata}'")
        return None
    
    pool_name = f"pool_{sorgente_normalizzata}"
    
    if pool_name in pool_cache:
        pool = pool_cache[pool_name]
    else: 
        try:
            logger.info(f"Creazione pool di connessioni per {sorgente_normalizzata}")
            pool = pooling.MySQLConnectionPool(
                pool_name=pool_name,
                pool_size=5,
                pool_reset_session=True,
                host=info['host'],
                port=info['porta'],
                user=info['user'],
                password=info['password'],
                database=info['database']
            )
            pool_cache[pool_name] = pool
        except Error as e:
            logger.error(f"Errore durante la creazione del pool di connessioni {e}")
            return None
    try: 
        conn = pool.get_connection()
        logger.debug(f"Connessione ottenuta dal pool per {sorgente_normalizzata}")
        return conn
    except Error as e:
        logger.error(f"Errore durante l'ottenimento della connessione dal pool {e}")
        return None
  
# Funzione per connettersi al database MySQL con configurazione da file YAML
def connect_to_db_custom(sorgente, config_db):
    sorgente_normalizzata = sorgente.replace(' ', '_').upper()
    if sorgente_normalizzata not in config_db:
        logger.warning(f"Sorgente '{sorgente}' non trovata, uso DEFAULT.")
        sorgente_normalizzata = 'DEFAULT'
        
    info = config_db[sorgente_normalizzata]
    logger.info(f"Connessione al database {sorgente} con host {info['host']} e porta {info['porta']}")
    
    try: 
        conn = mysql.connector.connect(
            host=info['host'],
            port=info['porta'],
            user=info['user'],
            password=info['password'],
            database=info['database']
        )
        if conn.is_connected():
            logger.info("Connessione al database riuscita")
            return conn
    except Error as e:
        logger.error(f"Errore durante la connessione al database {e}")
        return None

# Funzione per chiudere la connessione al database
def close_db_connection(conn):
    if conn.is_connected():
        conn.close()
        #print("Connessione al database chiusa")
        logger.info("Connessione al database chiusa")
    else:
        #print("La connessione al database non era aperta")
        logger.warning("La connessione al database non era aperta")
