import socketserver
from Server_mllp import MLLPHandler
from LoggerHL7 import LoggerHL7

logger = LoggerHL7()

if __name__ == "__main__":
    HOST, PORT = "localhost", 2575
    with socketserver.TCPServer((HOST, PORT), MLLPHandler) as server:
        logger.info(f'Server MLLP in ascolto su {HOST}:{PORT}')
        server.serve_forever()
        
        
        


# Per il multithreading:
# import socketserver
# from Server_mllp import MLLPHandler

# if __name__ == "__main__":
#     HOST, PORT = "localhost", 2575
#     with socketserver.ThreadingTCPServer((HOST, PORT), MLLPHandler) as server:
#         print(f"Server MLLP in ascolto su {HOST}:{PORT}")
#         server.serve_forever()