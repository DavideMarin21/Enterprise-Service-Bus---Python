import socketserver
from Server_MLLP_prova import MLLPHandler
from LoggerHL7 import LoggerHL7
from RabbitPublisher import RabbitPublisher
logger = LoggerHL7()

if __name__ == "__main__":
    HOST, PORT = "localhost", 2575

    # Creo una sola connessione RabbitMQ per tutti gli handler
    publisher = RabbitPublisher(host="localhost", queue_name="HL7",logger = logger)
    MLLPHandler.publisher = publisher

    with socketserver.TCPServer((HOST, PORT), MLLPHandler) as server:
        logger.info(f'Server MLLP in ascolto su {HOST}:{PORT}')
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            logger.info(f'Server MLLP chiuso.')
            publisher.close()
        finally:
            try:
                publisher.close()
                logger.info(f'Chiusura connessione publisher.')
            except Exception as e:
                logger.error(f'Chiusura connessione publisher non riuscita: {e}')
        
        


# Per il multithreading:
# import socketserver
# from Server_mllp import MLLPHandler

# if __name__ == "__main__":
#     HOST, PORT = "localhost", 2575
#     with socketserver.ThreadingTCPServer((HOST, PORT), MLLPHandler) as server:
#         print(f"Server MLLP in ascolto su {HOST}:{PORT}")
#         server.serve_forever()