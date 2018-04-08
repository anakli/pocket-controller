##############  Pocket resource utilization daemon  ####################

from jsonsocket import Client
import time
import psutil
import ifcfg
import threading
import pickle
import sys
import socket

HOSTNAME = sys.argv[1]
PORT = 12345

def get_net_bytes(rxbytes, txbytes) :
    SAMPLE_INTERVAL = 1.0
    cpu_util = psutil.cpu_percent(interval=SAMPLE_INTERVAL, percpu=True)
    rxbytes.append(int(ifcfg.default_interface()['rxbytes']))
    txbytes.append(int(ifcfg.default_interface()['txbytes']))
    rxbytes_per_s = (rxbytes[-1] - rxbytes[-2])/SAMPLE_INTERVAL
    txbytes_per_s = (txbytes[-1] - txbytes[-2])/SAMPLE_INTERVAL
    return cpu_util, rxbytes_per_s, txbytes_per_s

def tx_util_info(client, conn, cpu_util, rxbytes_per_s, txbytes_per_s):
    log = {'timestamp': time.time(),
            'datanodeid': conn.socket.getsockname()[0],
            'rx': rxbytes_per_s,
            'tx': txbytes_per_s,
            'cpu': cpu_util}
    conn.send(log)
    response = client.recv()
    print response
    return

if __name__ == "__main__":
    iface = ifcfg.default_interface()
    rxbytes = [int(iface['rxbytes'])]
    txbytes = [int(iface['txbytes'])]
    
    client = Client()
    conn = client.connect(HOSTNAME, PORT)
    print "Connected to ", HOSTNAME

    while(True):
        c, r, t = get_net_bytes(rxbytes, txbytes)
        time.sleep(1)
        tx_util_info(client,conn, c, r, t)

