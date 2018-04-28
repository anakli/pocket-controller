##############  Pocket resource utilization daemon  ####################

#from jsonsocket import Client
import time
import psutil
import ifcfg
import threading
import pickle
import sys
import socket
import struct

CONTROLLER_IP = "10.1.47.178"
CONTROLLER_PORT = 2345

SEND_STATS_INTERVAL = 1 # in seconds
UTIL_STAT_CMD = 15
INT = 4
LONG = 8
FLOAT = 4
SHORT = 2
BYTE = 1

def get_net_bytes(rxbytes, txbytes) :
    SAMPLE_INTERVAL = 1.0
    cpu_util = psutil.cpu_percent(interval=SAMPLE_INTERVAL, percpu=True)
    rxbytes.append(int(ifcfg.default_interface()['rxbytes']))
    txbytes.append(int(ifcfg.default_interface()['txbytes']))
    rxbytes_per_s = (rxbytes[-1] - rxbytes[-2])/SAMPLE_INTERVAL
    txbytes_per_s = (txbytes[-1] - txbytes[-2])/SAMPLE_INTERVAL
    return cpu_util, rxbytes_per_s, txbytes_per_s

def ip2long(ip):
    """
    Convert an IP string to long
    """
    packedIP = socket.inet_aton(ip)
    return struct.unpack("!L", packedIP)[0]


def send_util_info(sock, cpu_util, rxbytes_per_s, txbytes_per_s):
    rxMbps = rxbytes_per_s * 8 / 1e6
    txMbps = txbytes_per_s * 8 / 1e6
    log = {'timestamp': time.time(),
            'datanodeid': socket.gethostbyname(socket.gethostname()),
            'rx': rxMbps,
            'tx': txMbps,
            'cpu': cpu_util}
    # BINARY FORMAT: msgLen, ticket=timestamp, cmd, rxMbps, txMbs, numcores, cpuUtilCore0, cpuUtilCore1, etc..
    msg_packer = struct.Struct("!iqhiiii" + "i"*len(cpu_util)) 
    msgLen = INT + LONG + SHORT + 4*(INT) + len(cpu_util) * INT
    TICKET = int(time.time())

    ipaddr = ip2long(socket.gethostbyname(socket.gethostname())) # FIXME: check if this will work for the ReFlex datanodes!!!
    cpu_tuple = tuple([int(i) for i in cpu_util])
    sampleMsg = (msgLen, TICKET, UTIL_STAT_CMD, ipaddr, int(rxMbps), int(txMbps), len(cpu_util)) + cpu_tuple #  int(sum(cpu_util)/len(cpu_util)))
    flatten = lambda lst: reduce(lambda l, i: l + flatten(i) if isinstance(i, (list, tuple)) else l + [i], lst, [])   
    sampleMsg = tuple(flatten(sampleMsg))
    pkt = msg_packer.pack(*sampleMsg)
    sock.sendall(pkt)
    print sampleMsg
    return

if __name__ == "__main__":
    iface = ifcfg.default_interface()
    rxbytes = [int(iface['rxbytes'])]
    txbytes = [int(iface['txbytes'])]
    
    # connect to controller
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((CONTROLLER_IP, CONTROLLER_PORT))

    while(True):
        c, r, t = get_net_bytes(rxbytes, txbytes)
        time.sleep(SEND_STATS_INTERVAL)
        send_util_info(sock, c, r, t)

