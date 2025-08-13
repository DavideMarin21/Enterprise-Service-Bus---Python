from hl7apy.parser import parse_message

def routing_key_from_hl7(raw_hl7 : str, fallback : str = 'hl7.unknown') -> str:
    try :
        msg = parse_message(raw_hl7, find_groups= False, validation_level= 2)
        msh9 = msg.msh.msh_9
        code = (msh9.msh_9_1.to_er7() if hasattr(msh9, "msh_9_1") else "").strip() or "UNK"
        trig = (msh9.msh_9_2.to_er7() if hasattr(msh9, "msh_9_2") else "").strip() or "UNK"
        return f'hl7.{code}.{trig}'
    except Exception:
        return fallback