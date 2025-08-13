import pika, sys, os
from database import *

# Si occupa di fare cose nella coda dei messaggi di RabbitMQ
def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='HL7', durable=True)
    def callback(ch, method, properties, body):
        text = body.decode('utf-8', errors='ignore')  # decode per leggibilità
        print(f"Messaggio ricevuto:\n{text}\n")

    channel.basic_consume(queue='HL7', on_message_callback=callback, auto_ack=True)

    print(f'Aspettando il messaggio...')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Programa terminato.')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

