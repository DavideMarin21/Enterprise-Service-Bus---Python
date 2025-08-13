from LoggerHL7 import LoggerHL7
from hl7apy.parser import parse_message
from hl7apy.core import Message
from datetime import datetime

logger = LoggerHL7()  # Inizializza logger globale

# Delimitatori MLLP (come caratteri di controllo nella stringa)
START_BLOCK = '\x0b'   # <VT>
END_BLOCK = '\x1c'     # <FS>
CARRIAGE_RETURN = '\r' # <CR>

def risposta_ack(messaggio_hl7, esito: str, tipo_errore: str | None = None,
                 server_app: str = "SERVER", server_facility: str = "HOSPITAL") -> str:
    """
    Crea un ACK HL7 già incapsulato MLLP (stringa con \x0b ... \x1c\r),
    seguendo il formato richiesto:
      \x0bMSH|^~\&|SERVER|HOSPITAL|CLIENTAPP|HOSPITAL|{TS}||ACK^{MSH-9}|{ACK_CTRL}|P|{VER}\r
      MSA|{AA/AR}|{ORIG_CTRL}[|{ERRORE}]\x1c\r

    Parametri:
      - messaggio_hl7: stringa ER7 o oggetto hl7apy.Message del messaggio ricevuto
      - esito: 'AA' (accettato) oppure 'AR' (reject). Supporta anche 'AE' se vuoi.
      - tipo_errore: testo opzionale per MSA-3 quando esito non è 'AA'
      - server_app/server_facility: identificativi del tuo server in MSH-3/4

    Ritorna:
      - stringa pronta da .encode('utf-8') e inviare via socket
    """
    # Validazione esito
    if esito not in ('AA', 'AR', 'AE'):
        logger.error(f"Esito non valido: {esito}. Usa 'AA', 'AE' o 'AR'.")
        raise ValueError("esito deve essere 'AA', 'AE' o 'AR'")

    # Parsing: accetta sia stringa che oggetto Message
    msg = messaggio_hl7
    if not isinstance(msg, Message):
        try:
            msg = parse_message(messaggio_hl7)
        except Exception as e:
            logger.exception("Impossibile fare il parse del messaggio HL7")
            # In fallback costruiamo comunque un ACK minimale
            msg = None

    # Estrazione sicura dei campi dal MSH originale
    def get(field_path: str, default: str = "") -> str:
        try:
            node = eval(f"msg.{field_path}")  # es. "msh.msh_3"
            return node.to_er7() if hasattr(node, "to_er7") else str(node)
        except Exception:
            return default

    recv_app      = get("msh.msh_3", "CLIENTAPP")
    recv_facility = get("msh.msh_4", "HOSPITAL")
    orig_msg_type = get("msh.msh_9", "")         # es. "ADT^A04"
    orig_ctrl_id  = get("msh.msh_10", "MSG001")  # es. "MSG001"
    version       = get("msh.msh_12", "2.5")

    # Timestamp corrente formattato HL7
    ts = datetime.now().strftime("%Y%m%d%H%M%S")

    # MSH-9 dell'ACK secondo il formato richiesto: "ACK^{MSH-9 originale}"
    ack_msg_type = f"ACK^{orig_msg_type}" if orig_msg_type else "ACK"

    # MSH-10: controllo dell'ACK
    ack_control_id = f"ACK_{orig_ctrl_id}" if orig_ctrl_id else "ACK001"

    # Costruzione ER7 dell'ACK (solo MSH+MSA)
    msh = f"MSH|^~\\&|{server_app}|{server_facility}|{recv_app}|{recv_facility}|{ts}||{ack_msg_type}|{ack_control_id}|P|{version}\r"

    # MSA: MSA-1 esito, MSA-2 control id originale, MSA-3 opzionale in errore
    if esito == 'AA':
        msa = f"MSA|AA|{orig_ctrl_id}\r"
    else:
        # Per 'AE'/'AR' includiamo anche il testo dell'errore se fornito
        descr = tipo_errore if tipo_errore else "Errore applicativo"
        msa = f"MSA|{esito}|{orig_ctrl_id}|{descr}\r"

    # Incapsulamento MLLP
    ack_message = f"{START_BLOCK}{msh}{msa}{END_BLOCK}{CARRIAGE_RETURN}"

    logger.debug(f"ACK MLLP generato:\n{ack_message.encode('utf-8')!r}")
    return ack_message