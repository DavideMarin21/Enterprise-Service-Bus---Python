import mysql.connector
from mysql.connector import Error, pooling
from LoggerHL7 import LoggerHL7
import yaml

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
        logger.error("Errore durante la connessione al database", exc=e)
        return None 

# Carico la configurazione del database da un file YAML
def carica_config_db(path='db_config.yaml'):
    try:
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
            return config['fonti']
    except Exception as e:
        logger.error(f"Errore durante il caricamento del file YAML: {e}", exc=e)
        return {}

# Funzione per connettersi al database MySQL con configurazione da file YAML tramite pool di connessioni
def connect_to_db_custom_pool(sorgente, config_db):
    pool_cache = {}  
    sorgente_normalizzata = sorgente.replace(' ', '_').upper()
    fonti = config_db.get('fonti', {})
    logger.debug(f"[DEBUG] Chiavi presenti in fonti: {list(fonti.keys())}")
    logger.debug(f"[DEBUG] Chiave cercata: {sorgente_normalizzata}")
    
    if sorgente_normalizzata not in config_db:
        logger.warning(f"Sorgente '{sorgente}' non trovata, uso DEFAULT.")
        sorgente_normalizzata = 'DEFAULT'
        
    info = fonti[sorgente_normalizzata]
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
            logger.error("Errore durante la creazione del pool di connessioni", exc=e)
            return None
    try: 
        conn = pool.get_connection()
        logger.info(f"Connessione al database {sorgente_normalizzata} riuscita tramite pool")
        return conn
    except Error as e:
        logger.error("Errore durante l'ottenimento della connessione dal pool", exc=e)
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
        logger.error("Errore durante la connessione al database", exc=e)
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

# Funzione per aggiungere un paziente al database        
def aggiungi_paziente(PID_3, PID_5, PID_11, conn):
    try:
        ID_Paziente = PID_3.split('^')[0]
        Nome_Paziente = PID_5.split('^')[1]
        Cognome_Paziente = PID_5.split('^')[0]
        Via_Paziente = PID_11.split('^^')[0]
        Citta_Paziente = PID_11.split('^^')[1]
        CAP_Paziente = PID_11.split('^^')[2].split('^')[0]

        cursor = conn.cursor()

        # Controllo se il paziente è già presente
        query_check = "SELECT COUNT(*) FROM pazienti WHERE ID = %s"
        cursor.execute(query_check, (ID_Paziente,))
        (count,) = cursor.fetchone()

        if count > 0:
            logger.info(f"Paziente con ID {ID_Paziente} già presente nel database.")
            # Qui potresti attivare merge_pazienti se vuoi
            return None

        # Inserimento nuovo paziente
        sql = "INSERT INTO pazienti (ID, Nome, Cognome, Via, Citta, CAP) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (ID_Paziente, Nome_Paziente, Cognome_Paziente, Via_Paziente, Citta_Paziente, CAP_Paziente)
        cursor.execute(sql, val)
        conn.commit()

        logger.info(f"Paziente aggiunto con successo: {ID_Paziente}")
        return ID_Paziente

    except Error as e:
        logger.error("Errore durante l'inserimento del paziente", exc_info=True)
        return None

# Funzione per fare il merge di due pazienti nel database
def merge_pazienti(PID_3, MRG_1,conn):
    #conn = connect_to_db()
    try:
        PID_3 = PID_3.split('^')[0]
        MRG_1 = MRG_1.split('^')[0]
        cursor = conn.cursor()
        sql1 = 'SELECT Nome, Cognome, Via, Citta, CAP FROM PAZIENTI WHERE ID = %s'
        cursor.execute(sql1, (PID_3,))
        paziente1 = cursor.fetchone()
        sql2 = 'SELECT Nome, Cognome, Via, Citta, CAP FROM PAZIENTI WHERE ID = %s'
        cursor.execute(sql2, (MRG_1,))
        paziente2 = cursor.fetchone()
        # Se uno dei pazienti non esiste, non posso fare il merge
        if paziente1 == None:
            #print(f"Paziente con ID {PID_3} non trovato.")
            logger.error(f"Paziente con ID {PID_3} non trovato.")
            return None
        elif paziente2 == None:
            #print(f"Paziente con ID {MRG_1} non trovato.")
            logger.error(f"Paziente con ID {MRG_1} non trovato.")
            return None
        # Confronta solo Nome e Cognome
        elif paziente1[0].strip().lower() != paziente2[0].strip().lower() or \
           paziente1[1].strip().lower() != paziente2[1].strip().lower():
            #print("Nome o cognome non coincidono. Merge annullato.")
            logger.warning("Nome o cognome non coincidono. Merge annullato.")
            return None
        # Se i pazienti hanno lo stesso nome e cognome, unisco i dati
        else:
            sql = 'DELETE FROM PAZIENTI WHERE ID = %s'
            cursor.execute(sql, (MRG_1,))
            conn.commit()
            #print(f"Pazienti {PID_3} e {MRG_1} uniti con successo.")
            logger.info(f"Pazienti {PID_3} e {MRG_1} uniti con successo.")
            
            ##### IMPORTANTE #####
            # Se ho altre tabelle collegate andrebbero aggiornate con:
            # UPDATE REFERTI SET PazienteID = PID_3 WHERE PazienteID = MRG_1
            #####
    
            return True
    except Error as e:
        #print("Errore durante il merge dei pazienti:", e)
        logger.error("Errore durante il merge dei pazienti", exc=e)
    #finally:
        #close_db_connection(conn)
    
# Funzione per gestire il messaggio ADT^A04
def ADT_A04(hl7_message, conn):
    try:
        PID_3 = hl7_message.pid.pid_3.to_er7()  # ID Paziente
        PID_5 = hl7_message.pid.pid_5.to_er7()  # Nome e Cognome
        PID_11 = hl7_message.pid.pid_11.to_er7()  # Indirizzo
        result = aggiungi_paziente(PID_3, PID_5, PID_11,conn)
        if result:
            logger.info(f"Paziente aggiunto con ID {result}")

        else:
            logger.info(f"Paziente già presente: {PID_3.split('^')[0]}")
    except Error as e:
        logger.error("Errore durante l'inserimento del paziente", exc=e) 
        
# Funzione per gestire il messaggio ADT^A41    
def ADT_A41(hl7_message, conn):
    try:
        PID_3 = hl7_message.pid.pid_3.to_er7()  # ID Paziente
        MRG_1 = hl7_message.mrg.mrg_1.to_er7()  # ID Paziente da unire
        result = merge_pazienti(PID_3, MRG_1, conn)
        if result:
            logger.info(f"Merge completato tra {MRG_1.split('^')[0]} → {PID_3.split('^')[0]}")
        else:
            logger.warning(f"Merge fallito tra {MRG_1.split('^')[0]} → {PID_3.split('^')[0]}")
    except Error as e:
        logger.error("Errore durante il merge dei pazienti", exc=e)