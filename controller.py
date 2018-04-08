##############  Pocket controller  ####################

from jsonsocket import Server
import sys

HOSTNAME = sys.argv[1]
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
    
