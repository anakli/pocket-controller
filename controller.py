##############  Pocket controller  ####################

import socket
import asyncore
import sys
import json

HOSTNAME = sys.argv[1]
PORT = 12345


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
            deserialized = json.loads(view.tobytes())
            print deserialized
        except (TypeError, ValueError), e:
            raise Exception('Data received was not in JSON format')
        ## echo server code
        #data = self.recv(1024)
        #print("after recv")
        #if data:
        #    print("got data")
        #    self.buffer += data
        #    self.is_writable = True  # sth to send back now
        #else:
        #    print("got null data")

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
    request_queue_size          = 10
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
        asyncore.loop()

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


# TODO: compute total cluster utilization
# TODO: implement heuristics for deciding when to add/remove datanode
if __name__ == "__main__":
    server = ReqServer((HOSTNAME, PORT))
    server.serve_forever()
    
