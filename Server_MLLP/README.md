Classe MLLP per creare un server MLLP. Quando ricevono in ingresso un messaggio HL7 
ritornano indietro un ACK in caso riesca a parsare correttamente il messaggio, un NACK in 
caso contrario

Server_mllp.py crea un semplice server senza coda di RabbitMQ.

Server_MLLP_prova crea un server che spara ad una coda di RabbitMQ.