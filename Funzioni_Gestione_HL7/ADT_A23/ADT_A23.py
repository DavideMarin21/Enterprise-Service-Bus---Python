from mysql.connector import Error
from LoggerHL7 import LoggerHL7

logger = LoggerHL7()

# Funzione per fare il delete di un paziente nel database
def delete_paziente(PID_3, conn):
    try:
        ID_Paziente = PID_3.split('^')[0]
        cursor = conn.cursor()

        # Controllo se il paziente è  presente
        query_check = "SELECT COUNT(*) FROM pazienti WHERE ID = %s"
        cursor.execute(query_check, (ID_Paziente,))
        (count,) = cursor.fetchone()

        if count > 0:
            sql = "DELETE FROM pazienti WHERE ID = %s"
            cursor.execute(sql, (ID_Paziente,))
            conn.commit()
            logger.info(f"Paziente tolto con successo: {ID_Paziente}")
            return ID_Paziente

    except Error as e:
        logger.error(f"Errore durante il delete del paziente")
        return None

# Funzione per gestire il messaggio ADT^A23
def ADT_A23(hl7_message, conn):
    try:
        PID_3 = hl7_message.pid.pid_3.to_er7()  # ID Paziente
        result = delete_paziente(PID_3, conn)
        if result:
           logger.info(f"Paziente con ID {result} tolto")
        else:
           logger.info(f"Paziente non presente: {PID_3.split('^')[0]}")

    except Error as e:
        logger.error(f"Errore durante il delete del paziente {e}")