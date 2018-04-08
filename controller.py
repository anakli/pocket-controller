##############  Pocket controller  ####################

from jsonsocket import Server

HOSTNAME = 'localhost'
PORT = 12345

def rx_util_info(server):
    data = server.recv()
    server.send({'data': data})
    return

if __name__ == "__main__":
    server = Server(HOSTNAME, PORT)
    server.accept()
    while True:
        rx_util_info(server)
    
