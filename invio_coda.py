import pika
from LoggerHL7 import LoggerHL7

logger = LoggerHL7()

def invio_coda(messaggio, config):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=config['host']))
        channel = connection.channel()
        channel.queue_declare(queue=config['queue'], durable=True)
        channel.basic_publish(exchange='',
                              routing_key=config['queue'],
                              body=messaggio.encode('utf-8'))
        logger.log(f"Messaggio inviato alla coda {config['queue']}: {messaggio}")
        connection.close()
    except Exception as e:
        logger.log(f"Errore durante l'invio alla coda: {e}")
        
