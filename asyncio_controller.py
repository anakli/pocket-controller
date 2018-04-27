#!/usr/bin/env python3
import asyncio
import struct
import pandas as pd
from random import randint
import socket
#import pocket

# define RPC_CMDs and CMD_TYPEs
RPC_IOCTL_CMD = 13
NN_IOCTL_CMD = 13
RPC_JOB_CMD = 14
JOB_CMD = 14

CMD_DEL = 2
CMD_CREATE_DIR = 3
CMD_CLOSE = 4

NN_RESPONSE_BYTES = 4 + 8 + 2 # INT (msg_len) + LONG (ticket) + SHORT (OK or ERROR)

# define IOCTL OPCODES
NOP = 1
DN_REMOVE_OPCODE = 2
GET_CLASS_STATS_OPCODE = 3
NN_SET_WMASK_OPCODE = 4
REGISTER_OPCODE = 0
DEREGISTER_OPCODE = 1

MAX_DIR_DEPTH = 16
TICKET = 1000

INT = 4
LONG = 8
FLOAT = 4
SHORT = 2
BYTE = 1

REQ_STRUCT_FORMAT = "!iqhhi" # msg_len (INT), ticket (LONG LONG), cmd (SHORT), cmd_type (SHORT), register_type (INT)
REQ_LEN_HDR = SHORT + SHORT + BYTE # CMD, CMD_TYPE, IOCTL_OPCODE (note: doesn't include msg_len or ticket from NaRPC hdr)
MSG_LEN_HDR = INT + LONG + SHORT + SHORT + INT # MSG_LEN + TICKET + CMD, CMD_TYPE, OPCODE

RESP_STRUCT_FORMAT = "!iqhhi" # msg_len (INT), ticket (LONG LONG), cmd (SHORT), error (SHORT), register_opcode (INT)
RESP_LEN_BYTES = INT + LONG + SHORT + SHORT + INT # MSG_LEN, TICKET, CMD, ERROR, REGISTER_OPCODE 


hdr_req_packer = struct.Struct(REQ_STRUCT_FORMAT)
hdr_resp_packer = struct.Struct(RESP_STRUCT_FORMAT)

job_table = pd.DataFrame(columns=['jobid', 'GB', 'Mbps', 'wmask']).set_index('jobid')

def add_job(jobid, GB, Mbps, wmask):
  if jobid in job_table.index.values:
    print("ERROR: jobid {} already exists!".format(jobid))
    return -1
  job_table.loc[jobid,:] = dict(GB=GB, Mbps=Mbps, wmask=wmask) 
  print("Adding job " + jobid)
  print(job_table)
  return 0


@asyncio.coroutine
def handle_register_job(reader, writer):
  jobname_len = yield from reader.read(INT)
  jobname_len, = struct.Struct("!i").unpack(jobname_len)
  jobname = yield from reader.read(jobname_len)
  jobname, = struct.Struct("!" + str(jobname_len) + "s").unpack(jobname)
  jobname = jobname.decode('utf-8')
  # TODO: receive hints
  
  # generate jobid
  jobid_int = randint(0,1000000)
  jobid = jobname + "-" + str(jobid_int)

  # compute GB and GB/s based on heuristics and hints
  # FIXME: use hints
  jobGB = 50*2060      # default policty is 50 i3 nodes with DRAM and Flash tiers
  jobMbps = 50*8000   
 
  # register job in table
  add_job(jobid, jobGB, jobMbps, [(1234,0.1), (12345, 0.4)])   

  # create dir named jobid
  print("connect to namenode...")
#  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  NAMENODE_IP = "10.1.88.82"
  NAMENODE_PORT = 9070
#  sock.connect((NAMENODE_IP, NAMENODE_PORT))
#  jobid_len = len(jobid)
#  msg_packer = struct.Struct("!iqhi" + str(jobid_len) + "si")
#  msg_len = 2 + 4 + jobid_len + 4 

#  msg = (msg_len, TICKET, CMD_CREATE_DIR, jobid_len, jobid.encode('utf-8'), 0)
#  pkt = msg_packer.pack(*msg)

#  print("send to namenode...")
#  sock.sendall(pkt) 
#  data = sock.recv(NN_RESPONSE_BYTES)
#  print(data)


  # FIXME: figure out how to import pocket with python3
#  pocket = libpocket.PocketDispatcher()
#  res = pocket.Initialize(NAMENODE_IP, NAMENODE_PORT)
#  if res != 0:
#    print("Connecting to metadata server failed!")  
#  pocket.MakeDir(jobid)

  # generate wmask and send to metadata master

  
  # reply to client with jobid int
  #writer.write()

  return
  

@asyncio.coroutine
def handle_connection(reader, writer):
  address = writer.get_extra_info('peername')
  print('Accepted connection from {}'.format(address))
  while True:
    hdr = yield from reader.read(MSG_LEN_HDR) 
    [msg_len, ticket, cmd, cmd_type, opcode] = hdr_req_packer.unpack(hdr)
    print(msg_len, ticket, cmd, cmd_type, opcode)
    if cmd != cmd_type:
      print("ERROR: expected CMD_TYPE == CMD")

    if cmd == RPC_JOB_CMD:
      if opcode == REGISTER_OPCODE:
        yield from handle_register_job(reader, writer)
      elif opcode == DEREGISTER_OPCODE:
        print("Deregister job...");
      else:
        print("ERROR: unknown JOB_CMD opcode ", opcode);

    if cmd == RPC_IOCTL_CMD:
      if opcode == DN_REMOVE_OPCODE:
        print("Remove datanode...")
      elif opcode == NN_SET_WMASK_OPCODE:
        print("Set wmask...")
      elif opcode == GET_CLASS_STATS_OPCODE:
        print("Get capacity stats...")
      else:
        print("ERROR: unknown IOCTL_CMD opcode ", opcode);
    
    return      

#    data = b''
#    while not data.endswith(b'?'):
#      more_data = yield from reader.read(4096)
#      if not more_data:
#        if data:
#          print('Client {} sent {!r} but then closed'
#              .format(address, data))
#        else:
#          print('Client {} closed socket normally'.format(address))
#        return
#      data += more_data
#    answer =  get_answer(data)
#    writer.write(answer)

if __name__ == '__main__':
  #address = ("localhost", 4321) 
  address = ("10.1.45.35", 4321) 
  loop = asyncio.get_event_loop()
  coro = asyncio.start_server(handle_connection, *address)
  server = loop.run_until_complete(coro)
  print('Listening at {}'.format(address))
  try:
    loop.run_forever()
  finally:
    server.close()
    loop.close()
