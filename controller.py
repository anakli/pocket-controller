##############  Pocket controller  ####################

import socket
import asyncore
import sys
import json

HOSTNAME = sys.argv[1]
PORT = 12345

global_cpu = {}
global_rxGbs = {}
global_rxGbs = {}
global_lastUpdate = {}

BYTES_PER_SEC_LIMIT_10Gbs = 1.25e9
TIMEOUT_SEC = 5

class ReqHandler(asyncore.dispatcher):

    def __init__(self, conn_sock, client_address, server):
        self.server             = server
        self.client_address     = client_address
        self.buffer             = ""

        # We dont have anything to write, to start with
        self.is_writable        = False

        # Create ourselves, but with an already provided socket
        asyncore.dispatcher.__init__(self, conn_sock)

    def readable(self):
        return True     # We are always happy to read

    def writable(self):
        return self.is_writable # But we might not have
                                # anything to send all the time

    def handle_read(self):
        length_str = ''
        char = self.recv(1)
        while char != '\n':
            length_str += char
            char = self.recv(1)
        total = int(length_str)
        # use a memoryview to receive the data chunk by chunk efficiently
        view = memoryview(bytearray(total))
        next_offset = 0
        while total - next_offset > 0:
            recv_size = self.recv_into(view[next_offset:], total - next_offset)
            next_offset += recv_size
        try:
            util = json.loads(view.tobytes())
            print util
            dnId = util['datanodeid']
            global_cpu[dnId] = util['cpu']
            print "util rx:" , util['rx']
            global_rxGbs[dnId] = util['rx'] / BYTES_PER_SEC_LIMIT_10Gbs
            global_rxGbs[dnId] = util['tx'] / BYTES_PER_SEC_LIMIT_10Gbs
            global_lastUpdate[dnId] = util['timestamp']
            #TODO: global_DRAM_GB, global_Flash_GB
        except (TypeError, ValueError), e:
            raise Exception('Data received was not in JSON format')

    def handle_write(self):
        if self.buffer:
            sent = self.send(self.buffer)
            self.buffer = self.buffer[sent:]
        if len(self.buffer) == 0:
            self.is_writable = False

    def handle_error(self):
        pass 

    def handle_close(self):
        print "close conn" 
        self.close()
        self.server.handle_accept()
        return False
        

class ReqServer(asyncore.dispatcher):

    allow_reuse_address         = True
    request_queue_size          = 10    #FIXME: how should set this?
    address_family              = socket.AF_INET
    socket_type                 = socket.SOCK_STREAM

    def __init__(self, address, handlerClass=ReqHandler):
        self.address            = address
        self.handlerClass       = handlerClass

        asyncore.dispatcher.__init__(self)
        self.create_socket(self.address_family,
                               self.socket_type)

        if self.allow_reuse_address:
            self.set_reuse_addr()

        self.server_bind()
        self.server_activate()

    def server_bind(self):
        self.bind(self.address)
        print("bind: address=%s:%s" % (self.address[0], self.address[1]))

    def server_activate(self):
        self.listen(self.request_queue_size)
        print("listen: backlog=%d" % self.request_queue_size)

    def fileno(self):
        return self.socket.fileno()

    def serve_forever(self):
        asyncore.loop(timeout=1, count=TIMEOUT_SEC)

    # Internal use
    def handle_accept(self):
        (conn_sock, client_address) = self.accept()
        if self.verify_request(conn_sock, client_address):
            self.process_request(conn_sock, client_address)

    def verify_request(self, conn_sock, client_address):
        return True

    def process_request(self, conn_sock, client_address):
        print("conn_made: client_address=%s:%s" % \
                     (client_address[0],
                      client_address[1]))
        self.handlerClass(conn_sock, client_address, self)

    def handle_close(self):
        self.close()


if __name__ == "__main__":
    server = ReqServer((HOSTNAME, PORT))
    while True:
        server.serve_forever()
        # TODO: compute average cluster util every TIMEOUT_SEC
        # TODO: implement heuristics for deciding when to add/remove datanode
       
        # example: calculating avg rx net bw utilization (assuming 10 Gb/s max per node)
        # do we care more about avg across nodes or peak among nodes?
        if len(global_rxGbs) > 0:
            #print global_rxGbs
            rx_util = float(sum(global_rxGbs.values())) / len(global_rxGbs)
        else: 
            rx_util = 0
        print "avg cluster rx net BW:" , rx_util
