# Queste funzioni sono utili per gestire i dati HL7 in un server HTTP.
from hl7apy.parser import parse_message
from hl7apy.core import Message
from datetime import datetime
import mysql.connector
import json


# Questa funzione prende un messaggio HL7 come stringa e lo mappa in un dizionario
def mappa_campi_hl7(hl7_message):
    msg = hl7_message
    #msg = parse_message(hl7_message) per TCP_server.py
    dz = {} 
    for segment in msg.children: 
        for field in segment.children:
            dz[field.name] = field.value
            print(field.name,':' ,field.value)
    return dz

# Questa funzione converte un messaggio HL7 in un formato JSON
def hl7_to_JSON(hl7_message):
    msg = hl7_message
    json_data = {}
    for segment in msg.children:
        segment_name = segment.name
        json_data[segment_name] = {}
        for field in segment.children:
            field_name = field.name
            field_value = field.value
            json_data[segment_name][field_name] = field_value
            json_data = json.dumps(json_data, indent=4)
    return json_data

# Questa funzione converte un messaggio JSON in un messaggio HL7
def JSON_to_hl7(json_data):
    msg = Message("ADT_A01", version='2.5')
    for segment_name, fields in json_data.items():
        segment = msg.add_segment(segment_name)
        for field_name, field_value in fields.items():
            segment.add_field(field_name, field_value)
    return msg



# Questa funzione restituisce un messaggio di errore in formato HL7
# Prende in ingresso l'errore e un tipo di errore opzionale
# Se il messaggio HL7 è giusto errore sara False, altrimenti True
# Se errore è False, tipo_errore rimane None
# Se errore è True, tipo_errore sarà l'errore specifico da restituire
def risposta_ack(msg, errore, tipo_errore=None):
    
    try:
    # Estrazione dei campi principali da MSH
        sending_app = msg.MSH.sending_application.value
        sending_fac = msg.MSH.sending_facility.value
        receiving_app = msg.MSH.receiving_application.value
        receiving_fac = msg.MSH.receiving_facility.value
        original_msg_control_id = msg.MSH.message_control_id.value
        message_type = msg.MSH.message_type.message_code.value
     
        # Parte MSH
        ack = Message("ACK", version='2.5')
        ack.msh.msh_1 = '|'    # Obbligatorio, separatore di campo
        ack.msh.msh_2 = '^~\\&' # Obbligatorio, separatore di componente
        ack.msh.msh_3 = receiving_app # Opzionale, applicazione ricevente
        ack.msh.msh_4 = receiving_fac # Opzionale, struttura ricevente
        ack.msh.msh_5 = sending_app # Opzionale, applicazione mittente
        ack.msh.msh_6 = sending_fac # Opzionale, struttura mittente
        ack.msh.msh_7 = datetime.now().strftime('%Y%m%d%H%M%S') # Obbligatorio, data e ora di invio
        ack.msh.msh_9 = f'ACK^{message_type}' # Obbligatorio, tipo di messaggio
        ack.msh.msh_10 = 'ACK_' + original_msg_control_id # Obbligatorio, ID di controllo del messaggio originale
        ack.msh.msh_11 = 'P' # Obbligatorio, priorità del messaggio
        ack.msh.msh_12 = '2.5' # Obbligatorio, versione del protocollo HL7
        
        # Parte MSA
        
        # Se non ho un errore
        if errore == False:
            ack.msa.msa_1 = 'AA' # Obbligatorio, codice di stato
            ack.msa.msa_2 = original_msg_control_id # Obbligatorio, ID di controllo del messaggio originale
            
        # Se ho un errore    
        else: 
            ack.msa.msa_1 = 'AE' # Obbligatorio, codice di stato (AE per errore)
            ack.msa.msa_2 = original_msg_control_id # Obbligatorio, ID di controllo del messaggio originale
            ack.msa.msa_3 = tipo_errore if tipo_errore else 'Errore sconosciuto' # Opzionale, descrizione dell'errore
            
        return ack
    
    
    except Exception as e:
        print(f"Errore durante la creazione della risposta ACK: {e}")
        return None
    
    
    
    
    
    
    
  
        
        
        
        
        
        