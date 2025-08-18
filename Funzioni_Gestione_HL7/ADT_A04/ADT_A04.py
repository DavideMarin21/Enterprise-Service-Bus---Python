from mysql.connector import Error
from LoggerHL7 import LoggerHL7

logger = LoggerHL7()


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
        logger.error(f"Errore durante l'inserimento del paziente")
        return None

# Funzione per gestire il messaggio ADT^A04
def ADT_A04(hl7_message, conn):
    try:
        PID_3 = hl7_message.pid.pid_3.to_er7()  # ID Paziente
        PID_5 = hl7_message.pid.pid_5.to_er7()  # Nome e Cognome
        PID_11 = hl7_message.pid.pid_11.to_er7()  # Indirizzo
        result = aggiungi_paziente(PID_3, PID_5, PID_11, conn)
        if result:
            logger.info(f"Paziente aggiunto con ID {result}")
        else:
            logger.info(f"Paziente già presente: {PID_3.split('^')[0]}")
    except Error as e:
        logger.error(f"Errore durante l'inserimento del paziente {e}")
