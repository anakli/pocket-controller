#!/usr/bin/env python3
import asyncio
import struct
import pandas as pd
from random import randint
import socket
import pocket
import pocket_metadata_cmds as ioctlcmd

NAMENODE_IP = "10.1.88.82"
NAMENODE_PORT = 9070

STORAGE_TIERS = [0, 1]           # 0 is DRAM, 1 is Flash
GET_CAPACITY_STATS_INTERVAL = 5  # in seconds

# define RPC_CMDs and CMD_TYPEs
RPC_IOCTL_CMD = 13
NN_IOCTL_CMD = 13
RPC_JOB_CMD = 14
JOB_CMD = 14
UTIL_STAT_CMD = 15

CMD_DEL = 2
CMD_CREATE_DIR = 3
CMD_CLOSE = 4


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
DN_LEN_HDR = INT + LONG + SHORT + 4*(INT)

RESP_STRUCT_FORMAT = "!iqhhi" # msg_len (INT), ticket (LONG LONG), cmd (SHORT), error (SHORT), register_opcode (INT)
RESP_LEN_BYTES = INT + LONG + SHORT + SHORT + INT # MSG_LEN, TICKET, CMD, ERROR, REGISTER_OPCODE 
NN_RESPONSE_BYTES = 4 + 8 + 2 # INT (msg_len) + LONG (ticket) + SHORT (OK or ERROR)

RESP_OK = 0
RESP_ERR = 1

hdr_req_packer = struct.Struct(REQ_STRUCT_FORMAT)
hdr_resp_packer = struct.Struct(RESP_STRUCT_FORMAT)
dn_req_packer = struct.Struct("!iqhiiii")

job_table = pd.DataFrame(columns=['jobid', 'GB', 'Mbps', 'wmask']).set_index('jobid')
avg_util = {'cpu': 0, 'net': 0, 'DRAM': 0, 'Flash': 0}


def add_job(jobid, GB, Mbps, wmask):
  if jobid in job_table.index.values:
    print("ERROR: jobid {} already exists!".format(jobid))
    return 1
  job_table.loc[jobid,:] = dict(GB=GB, Mbps=Mbps, wmask=wmask) 
  print("Adding job " + jobid)
  print(job_table)
  return 0

def remove_job(jobid):
  if jobid not in job_table.index.values:
    print("ERROR: jobid {} not found!".format(jobid))
    return 1
  job_table.drop(jobid, inplace=True)
  print("Removed job " + jobid)
  print(job_table)
  return 0

@asyncio.coroutine
def handle_register_job(reader, writer):
  jobname_len = yield from reader.read(INT)
  jobname_len, = struct.Struct("!i").unpack(jobname_len)
  jobname = yield from reader.read(jobname_len)
  jobname, = struct.Struct("!" + str(jobname_len) + "s").unpack(jobname)
  jobname = jobname.decode('utf-8')
  # FIXME: receive hints
  print("TODO: receive and use hints...")
  
  # generate jobid
  jobid_int = randint(0,1000000)
  jobid = jobname + "-" + str(jobid_int)

  # compute GB and GB/s based on heuristics and hints
  # FIXME: use hints
  jobGB = 50*2060      # default policty is 50 i3 nodes with DRAM and Flash tiers
  jobMbps = 50*8000   
 
  # create dir named jobid
  # NOTE: this is blocking but we are not yielding
  createdirsock = pocket.connect(NAMENODE_IP, NAMENODE_PORT)
  if createdirsock is None:
    return
  pocket.create_dir(createdirsock, None, jobid)
  #pocket.close(createdirsock)

  print("TODO: generate wmask...")
  # FIXME: generate wmask for job
  #wmask =  [(1234, 0.1), (12345, 0.4)]
  wmask = [(ioctlcmd.calculate_datanode_hash("10.1.88.82", 50030), 1)]

  # register job in table
  err = add_job(jobid, jobGB, jobMbps, wmask)

  # send wmask to metadata server   
  ioctlsock = yield from ioctlcmd.connect(NAMENODE_IP, NAMENODE_PORT)
  if ioctlsock is None:
    return
  yield from ioctlcmd.send_weightmask(ioctlsock, jobid, wmask) 

  # reply to client with jobid int
  resp_packer = struct.Struct(RESP_STRUCT_FORMAT + "i")
  resp = (RESP_LEN_BYTES + INT, TICKET, JOB_CMD, err, REGISTER_OPCODE, jobid_int)
  pkt = resp_packer.pack(*resp)
  writer.write(pkt)

  return
  

@asyncio.coroutine
def handle_deregister_job(reader, writer):
  jobid_len = yield from reader.read(INT)
  jobid_len, = struct.Struct("!i").unpack(jobid_len)
  jobid = yield from reader.read(jobid_len)
  jobid, = struct.Struct("!" + str(jobid_len) + "s").unpack(jobid)
  jobid = jobid.decode('utf-8')
  
  # delete job from table
  err = remove_job(jobid)
  if err == 0:
    # delete dir named jobid
    # NOTE: this is blocking but we are not yielding
    # FIXME: fix bug in c++ client for deleting files and dirs!
    createdirsock = pocket.connect(NAMENODE_IP, NAMENODE_PORT)
    if createdirsock is None:
      return
    pocket.delete(createdirsock, None, "/" + jobid)
    #pocket.close(createdirsock)
  
  # reply to client with jobid int
  resp_packer = struct.Struct(RESP_STRUCT_FORMAT)
  resp = (RESP_LEN_BYTES + INT, TICKET, JOB_CMD, err, DEREGISTER_OPCODE)
  pkt = resp_packer.pack(*resp)
  writer.write(pkt)
  return


@asyncio.coroutine
def handle_jobs(reader, writer):
  address = writer.get_extra_info('peername')
  print('Accepted job connection from {}'.format(address))
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
        yield from handle_deregister_job(reader, writer)
      else:
        print("ERROR: unknown JOB_CMD opcode ", opcode);

    if cmd == RPC_IOCTL_CMD:
      if opcode == DN_REMOVE_OPCODE:
        print("Remove datanode...")
      elif opcode == GET_CLASS_STATS_OPCODE:
        print("Get capacity stats...")
      else:
        print("ERROR: unknown IOCTL_CMD opcode ", opcode);
    
    return      

@asyncio.coroutine
def handle_datanodes(reader, writer):
  address = writer.get_extra_info('peername')
  print('Accepted datanode connection from {}'.format(address))
  while True:
    hdr = yield from reader.read(DN_LEN_HDR) 
    [msg_len, ticket, cmd, datanode_addr, rx_util, tx_util, num_cores] = dn_req_packer.unpack(hdr)
    if cmd != UTIL_STAT_CMD:
      print("ERROR: unknown IOCTL_CMD opcode ", opcode);
    cpu_util = yield from reader.read(num_cores * INT)
    cpu_util = struct.Struct("!" + "i"*num_cores).unpack(cpu_util)
    print(datanode_addr, ticket, rx_util, tx_util, cpu_util)

@asyncio.coroutine
def send_periodically(sock):
  while True:
    yield from asyncio.sleep(GET_CAPACITY_STATS_INTERVAL)
    for tier in STORAGE_TIERS: 
      all_blocks, free_blocks = yield from ioctlcmd.get_class_stats(sock, tier)
      if all_blocks:
        avg_usage = (all_blocks-free_blocks)*1.0/all_blocks
        print("Capacity usage for Tier", tier, ":", free_blocks, "free blocks out of", \
               all_blocks, "(", avg_usage, "% )")
      else:
        avg_usage = -1
      # update global avg_util dictionary
      if tier == 0:
        avg_util['DRAM'] = avg_usage 
      elif tier == 1:
        avg_util['Flash'] = avg_usage 


if __name__ == '__main__':
  
  loop = asyncio.get_event_loop()
  # Start server listening for register/deregister job connections
  #address = ("localhost", 4321) 
  address = ("10.1.47.178", 4321) 
  coro = asyncio.start_server(handle_jobs, *address)
  server = loop.run_until_complete(coro)
  print('Listening at {}'.format(address))
 
  # Initialize routine to periodically send
  metadata_socket = ioctlcmd.connect_until_succeed(NAMENODE_IP, NAMENODE_PORT)
  asyncio.async(send_periodically(metadata_socket))

  # Start server listening for datanode util info
  address = ("10.1.47.178", 2345) 
  coro = asyncio.start_server(handle_datanodes, *address)
  server = loop.run_until_complete(coro)
  print('Listening at {}'.format(address))
  print("Start loop...") 
  try:
    loop.run_forever()
  finally:
    server.close()
    loop.close()
