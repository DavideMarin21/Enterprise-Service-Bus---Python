# Queste funzioni sono utili per gestire i dati HL7.

from hl7apy.core import Message

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
