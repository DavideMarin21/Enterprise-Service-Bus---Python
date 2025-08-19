from mysql.connector import Error
from Gestore_Logs.LoggerHL7 import LoggerHL7

logger = LoggerHL7()


# Funzione per fare il merge di due pazienti nel database
def merge_pazienti(PID_3, MRG_1, conn):
    # conn = connect_to_db()
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
            # print(f"Paziente con ID {PID_3} non trovato.")
            logger.error(f"Paziente con ID {PID_3} non trovato.")
            return None
        elif paziente2 == None:
            # print(f"Paziente con ID {MRG_1} non trovato.")
            logger.error(f"Paziente con ID {MRG_1} non trovato.")
            return None
        # Confronta solo Nome e Cognome
        elif paziente1[0].strip().lower() != paziente2[0].strip().lower() or \
                paziente1[1].strip().lower() != paziente2[1].strip().lower():
            # print("Nome o cognome non coincidono. Merge annullato.")
            logger.warning("Nome o cognome non coincidono. Merge annullato.")
            return None
        # Se i pazienti hanno lo stesso nome e cognome, unisco i dati
        else:
            sql = 'DELETE FROM PAZIENTI WHERE ID = %s'
            cursor.execute(sql, (MRG_1,))
            conn.commit()
            # print(f"Pazienti {PID_3} e {MRG_1} uniti con successo.")
            logger.info(f"Pazienti {PID_3} e {MRG_1} uniti con successo.")

            ##### IMPORTANTE #####
            # Se ho altre tabelle collegate andrebbero aggiornate con:
            # UPDATE REFERTI SET PazienteID = PID_3 WHERE PazienteID = MRG_1
            #####

            return True
    except Error as e:
        # print("Errore durante il merge dei pazienti:", e)
        logger.error(f"Errore durante il merge dei pazienti {e}")
    # finally:
    # close_db_connection(conn)

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
        logger.error(f"Errore durante il merge dei pazienti {e}")
