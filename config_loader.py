import yaml
from Gestore_Logs.LoggerHL7 import LoggerHL7

logger = LoggerHL7()

"""
La funzione permette di caricare le configuarazioni nel file db_config.yaml in modo da poter configurare i parametri dei vari
database a cui devo accedere.
"""
# Carico la configurazione da un file YAML
def carica_config_db(path='db_config.yaml'):
    try:
        with open(path, 'r') as f:
            return  yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Errore durante il caricamento del file YAML: {e}")
        return {}
    
   