import pika

# Classe per poter pubblicare i messaggi in ingresso nella coda di RabbitMQ
class RabbitPublisher:

    def __init__(self, host, queue_name, logger):
        self.queue_name = queue_name
        self.logger = logger
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)

    def publish(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body = message,
                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
            )
            self.logger.info(f'Messaggio inviato a coda {self.queue_name}')
        except Exception as e:
            self.logger.error(f'Errore nel mandare il messaggio: {e}')
            raise

    def close(self):
        try:
            self.connection.close()
            self.logger.info(f'Connessione chiusa')
        except Exception as e:
            self.logger.error(f'Problema con la chiusura: {e}')


