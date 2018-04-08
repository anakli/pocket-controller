##############  Pocket controller  ####################

import SocketServer
import sys
import json

HOSTNAME = sys.argv[1]
PORT = 12345

class ReqHandler(SocketServer.StreamRequestHandler):
    """
    The request handler class for controller server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def handle(self):
        # self.request is the TCP socket connected to the client
        numbytes = self.rfile.readline().strip()
        self.data = self.request.recv(int(numbytes)).strip()
        try:
            deserialized = json.loads(self.data)
        except (TypeError, ValueError), e:
            raise Exception('Data received was not in JSON format')
        print("{} wrote:".format(self.client_address[0]))
        print(deserialized)
        self.wfile.write(str(numbytes) + "\n")
        self.wfile.write(json.dumps(deserialized))

# TODO: compute total cluster utilization
# TODO: implement heuristics for deciding when to add/remove datanode
if __name__ == "__main__":
    server = SocketServer.TCPServer((HOSTNAME, PORT), ReqHandler)
    server.allow_reuse_address = True
    server.serve_forever()
    
