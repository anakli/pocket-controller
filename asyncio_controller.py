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
GET_CAPACITY_STATS_INTERVAL = 1  # in seconds
AUTOSCALE_INTERVAL = 1           # in seconds

WAIT_FOR_DRAM_STARTUP = 15       # in seconds
WAIT_FOR_FLASH_STARTUP = 45      # in seconds

DRAM_NODE_GB = 60
FLASH_NODE_GB = 2000
NODE_Mbps = 8000
DEFAULT_NUM_NODES = 50
PER_LAMBDA_MAX_Mbps = 600  

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
DN_LEN_HDR = INT + LONG + SHORT + 5*(INT)

RESP_STRUCT_FORMAT = "!iqhhi" # msg_len (INT), ticket (LONG LONG), cmd (SHORT), error (SHORT), register_opcode (INT)
RESP_LEN_BYTES = INT + LONG + SHORT + SHORT + INT # MSG_LEN, TICKET, CMD, ERROR, REGISTER_OPCODE 
NN_RESPONSE_BYTES = 4 + 8 + 2 # INT (msg_len) + LONG (ticket) + SHORT (OK or ERROR)

RESP_OK = 0
RESP_ERR = 1

hdr_req_packer = struct.Struct(REQ_STRUCT_FORMAT)
hdr_resp_packer = struct.Struct(RESP_STRUCT_FORMAT)
dn_req_packer = struct.Struct("!iqhiiiii")

job_table = pd.DataFrame(columns=['jobid', 'GB', 'Mbps', 'wmask']).set_index('jobid')
datanode_usage = pd.DataFrame(columns=['datanodeip_port', 'cpu', 'net_Mbps','blacklisted']).set_index(['datanodeip_port'])
datanode_alloc = pd.DataFrame(columns=['datanodeip_port', 'cpu', 'net_Mbps', 'DRAM_GB', 'Flash_GB', 'blacklisted']).set_index(['datanodeip_port'])
datanode_provisioned = pd.DataFrame(columns=['datanodeip_port', 'cpu_num', 'net_Mbps', 'DRAM_GB', 'Flash_GB', 'blacklisted']).set_index(['datanodeip_port'])
avg_util = {'cpu': 0, 'net': 0, 'dram': 0, 'flash': 0}


# NOTE: assuming i3 and r4 2xlarge instances
def add_datanode_provisioned(datanodeip, port, num_cpu):
  datanode = datanodeip + ":" + str(port)
  if datanode in datanode_provisioned.index.values:
    #print("Datanode {}:{} is already in table".format(datanodeip, port))
    return 1
  if port == 50030:
    datanode_provisioned.loc[datanode,:] = dict(cpu_num=num_cpu, net_Mbps=8000, DRAM_GB=60, Flash_GB=0, blacklisted=0)
  elif port == 1234:
    datanode_provisioned.loc[datanode,:] = dict(cpu_num=num_cpu, net_Mbps=8000, DRAM_GB=0, Flash_GB=2000, blacklisted=0)
  else:
    print("ERROR: unrecognized port! assuming 50030 for dram, 1234 for Flash/ReFlex")

def add_datanode_alloc(datanodeip, port):
  datanode = datanodeip + ":" + str(port)
  if datanode in datanode_alloc.index.values:
    #print("Datanode {}:{} is already in table".format(datanodeip, port))
    return 1
  #datanode_alloc.loc[datanode,:] = dict(cpu=0.0, net_Mbps=0.0, DRAM_GB=0.0, Flash_GB=0.0, blacklisted=0)
  datanode_alloc.loc[datanode,:] = dict(cpu=0.0, net_Mbps=randint(0,10)*1.0/10, DRAM_GB=0.0, Flash_GB=0.0, blacklisted=0)

def add_datanode_usage(datanodeip, port, cpu, net):
  datanode = datanodeip + ":" + str(port)
  if datanode not in datanode_usage.index.values:
    datanode_usage.loc[datanode,:] = dict(cpu=cpu, net_Mbps=net, blacklisted=0)
  else:
    datanode_usage.at[datanode ,'cpu'] = cpu 
    datanode_usage.at[datanode, 'net'] = net


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

# Algorithm to generate weightmask
# The weightmask defines the list of datanodes (including tier type) 
# to spread this job's data across, along with an associated weight for each node
# the weight is used for weighted random datanode block selection at the metdata server
def generate_weightmask(jobid, jobGB, jobMbps, latency_sensitive):
  print("generate weightmask for ", jobid, jobGB, jobMbps, latency_sensitive)
  wmask = []
  # Step 1: determine if capacity or throughput bound
  if latency_sensitive:  
    num_nodes_for_capacity = jobGB / DRAM_NODE_GB
  else:
    num_nodes_for_capacity = jobGB / FLASH_NODE_GB
   
  num_nodes_for_throughput = jobMbps / NODE_Mbps

  if num_nodes_for_throughput > num_nodes_for_capacity:
    print("jobid {} is throughput-bound".format(jobid))
    throughput_bound = 1
  else:
    print("jobid {} is capacity-bound".format(jobid))
    throughput_bound = 0
  
  # Step 2: check available resources in cluster
  # If throughput bound, will allocate nodes based on CPU and network demand 
  if throughput_bound:
    # find all nodes that have spare Mbps 
    print(datanode_alloc)
    spare_throughput = datanode_alloc['net_Mbps'] < 1
    candidate_nodes_net = datanode_alloc[spare_throughput].sort_values(by='net_Mbps', ascending=False).loc[:, 'net_Mbps']
    spare_net_weight_alloc = 0
    job_net_weight_req = jobMbps * 1.0 / NODE_Mbps
    print(candidate_nodes_net)

    print("job net weight req {}, this is {} Mbps".format(job_net_weight_req, jobMbps))
    # smallest fit first algorithm: fill in smallest gap first
    for node in candidate_nodes_net.index: 
      net = candidate_nodes_net[node]
      print("net for node {} is {}".format(node, net))
      if net == 1.0:
        continue
      #TODO: before decide to use a node, also much check weight*capacity satisfies capacity constraint!
      if job_net_weight_req - spare_net_weight_alloc >= 1 - net: 
        spare_net_weight_alloc += 1 - net
        print("setting datanode_alloc to 1 for datanode {}".format(node))
        datanode_alloc.at[node,'net_Mbps'] = 1
        wmask.append((node, 1- net))
      elif job_net_weight_req - spare_net_weight_alloc < 1 - net:
        print("setting datanode_alloc to {} for datanode {}".format(net + (job_net_weight_req - spare_net_weight_alloc),node))
        datanode_alloc.at[node,'net_Mbps'] = net + (job_net_weight_req - spare_net_weight_alloc)
        wmask.append((node, job_net_weight_req - spare_net_weight_alloc))
        spare_net_weight_alloc += job_net_weight_req - spare_net_weight_alloc

      if spare_net_weight_alloc == job_net_weight_req:
        break
    
      if spare_net_weight_alloc > job_net_weight_req:
        print("ERROR: shouldn't be allocating more than job needs! something went wrong...")
        break
    if spare_net_weight_alloc == job_net_weight_req:
      print("Satisified job without needing to launch new nodes :)")
      print(datanode_alloc)
    else:
      extra_nodes_needed = (job_net_weight_req - spare_net_weight_alloc)
      last_weight = extra_nodes_needed - int(extra_nodes_needed)
      if last_weight == 0:
        new_node_weights = [1 for i in range(0, extra_nodes_needed)]
      else:
        new_node_weights = [1 for i in range(0, int(extra_nodes_needed))]
        new_node_weights.append(last_weight)
      datanode_alloc_prelaunch = datanode_alloc
      print("TODO: LAUNCH {} extra nodes, wait for them to come up and assing proper weights {}"\
              .format(extra_nodes_needed, new_node_weights))
      #FIXME: KUBERNETES LAUNCH JOBS with parallelism=extra_nodes_needed
      #FIXME: need to wait for new nodes to start sending stats and add themselves to the datanode_alloc table,
      #       then assign them the proper weights
      #TODO: have a function that monitors status of datanode_alloc and compares to old version datanode_alloc_prelaunch
   
  
  # TODO: If capacity bound, will allocate nodes based on DRAM or Flash capacity (depending on latency sensitivity)
  else:
    # find all nodes that have spare capacity
    print("NOTICE: the app is capacity-bound. \
           TODO: need to implement weightmask generation for this case. Not yet supported.\n")
    # should be similar to sizing based on capacity
    # but need to decide whether to use DRAM or Flash capacity based on latency sensitivity
    # skipping this for now since all our apps are throughput-bound

  # convert weightmask to proper format
  job_wmask = []
  for (datanodeip_port, weight) in wmask:
    datanode_ip = datanodeip_port.split(":")[0]
    datanode_port = int(datanodeip_port.split(":")[1])
    datanode_hash = ioctlcmd.calculate_datanode_hash(datanode_ip, datanode_port) 
    job_wmask.append((datanode_hash, float("{0:.2f}".format(weight))))

  print(job_wmask) 
  return job_wmask

def compute_GB_Mbps_with_hints(num_lambdas, jobGB, peakMbps, latency_sensitive):

  # determine jobGB and peakMbps based on provided hints (0 means not provided)
  if num_lambdas == 0 and jobGB == 0 and peakMbps == 0: 
    if latency_sensitive: 
      jobGB = DEFAULT_NUM_NODES * (DRAM_NODE_GB + FLASH_NODE_GB)   
    else:
      jobGB = DEFAULT_NUM_NODES * (FLASH_NODE_GB)   
    peakMbps = DEFAULT_NUM_NODES * NODE_Mbps  
  elif num_lambdas != 0 and jobGB == 0 and peakMbps == 0:
    num_nodes = num_lambdas * PER_LAMBDA_MAX_Mbps / NODE_Mbps
    if latency_sensitive: 
      jobGB = num_nodes * (DRAM_NODE_GB + FLASH_NODE_GB)   
    else:
      jobGB = num_nodes * (FLASH_NODE_GB)   
    peakMbps = num_nodes * NODE_Mbps
  elif num_lambdas != 0 and jobGB == 0: # only capacity unknown
    num_nodes = num_lambdas * PER_LAMBDA_MAX_Mbps / NODE_Mbps
    if latency_sensitive: 
      jobGB = num_nodes * (DRAM_NODE_GB + FLASH_NODE_GB)   
    else:
      jobGB = num_nodes * (FLASH_NODE_GB)   
  elif num_lambdas != 0 and peakMbps == 0: # only Mbps unknown
    num_nodes = num_lambdas * PER_LAMBDA_MAX_Mbps / NODE_Mbps
    peakMbps = num_nodes * NODE_Mbps
  elif num_lambdas == 0 and jobGB == 0: # capacity and lambdas unknown
    # use peakMbps hint to estimate number of nodes needed
    num_nodes = peakMbps / NODE_Mbps
    if latency_sensitive: 
      jobGB = num_nodes * (DRAM_NODE_GB + FLASH_NODE_GB)   
    else:
      jobGB = num_nodes * (FLASH_NODE_GB)   
  elif num_lambdas == 0 and peakMbps == 0: # Mbps and lambdas unknown
    # use capacity hint to estimate number of nodes needed
    if latency_sensitive:
      num_nodes = jobGB / DRAM_NODE_GB
    else:
      num_nodes = jobGB / FLASH_NODE_GB
    peakMbps = num_nodes * NODE_Mbps
  if jobGB == 0:
    jobGB = 1

  return jobGB, peakMbps


@asyncio.coroutine
def handle_register_job(reader, writer):
  jobname_len = yield from reader.read(INT)
  jobname_len, = struct.Struct("!i").unpack(jobname_len)
  jobname = yield from reader.read(jobname_len + 3*INT + SHORT)
  jobname, num_lambdas, jobGB, peakMbps, latency_sensitive = struct.Struct("!" + str(jobname_len) + "siiih").unpack(jobname)
  jobname = jobname.decode('utf-8')
  
  # generate jobid
  jobid_int = randint(0,1000000)
  jobid = jobname + "-" + str(jobid_int)

  print("received hints ", jobid, num_lambdas, jobGB, peakMbps, latency_sensitive) 
  # create dir named jobid
  # NOTE: this is blocking but we are not yielding
  createdirsock = pocket.connect(NAMENODE_IP, NAMENODE_PORT)
  if createdirsock is None:
    return
  pocket.create_dir(createdirsock, None, jobid)
  #pocket.close(createdirsock)

  if jobGB == 0 or peakMbps == 0: 
    jobGB, peakMbps = compute_GB_Mbps_with_hints(num_lambdas, jobGB, peakMbps, latency_sensitive)

  # generate weightmask 
  wmask = generate_weightmask(jobid, jobGB, peakMbps, latency_sensitive)
 # wmask = [(ioctlcmd.calculate_datanode_hash("10.1.88.82", 50030), 1)]

  # register job in table
  err = add_job(jobid, jobGB, peakMbps, wmask)

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
    [msg_len, ticket, cmd, datanode_int, port, rx_util, tx_util, num_cores] = dn_req_packer.unpack(hdr)
    if cmd != UTIL_STAT_CMD:
      print("ERROR: unknown datanode opcode ", opcode);
    cpu_util = yield from reader.read(num_cores * INT)
    cpu_util = struct.Struct("!" + "i"*num_cores).unpack(cpu_util)
    if len(cpu_util) == 0:
      avg_cpu = 0
    else:
      avg_cpu = sum(cpu_util)/len(cpu_util)
    peak_net = int(max(rx_util, tx_util)*1.0/NODE_Mbps * 100)
    # add datanode to tables
    datanode_ip = socket.inet_ntoa(struct.pack('!L', datanode_int))
    add_datanode_provisioned(datanode_ip, port, len(cpu_util))
    add_datanode_alloc(datanode_ip, port) 
    add_datanode_usage(datanode_ip, port, avg_cpu, peak_net) 
    print(datanode_ip, port, ticket, rx_util, tx_util, cpu_util)

@asyncio.coroutine
def get_capacity_stats_periodically(sock):
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
        avg_util['dram'] = avg_usage 
      elif tier == 1:
        avg_util['flash'] = avg_usage 



@asyncio.coroutine
def launch_dram_datanode():
  print("TODO: launch dram datanode........")
  yield from asyncio.sleep(WAIT_FOR_DRAM_STARTUP)
  return

@asyncio.coroutine
def launch_flash_datanode():
  print("TODO: launch flash datanode........")
  yield from asyncio.sleep(WAIT_FOR_FLASH_STARTUP)
  return


UTIL_DRAM_LOWER_LIMIT = 70
UTIL_DRAM_UPPER_LIMIT = 80
UTIL_FLASH_LOWER_LIMIT = 70
UTIL_FLASH_UPPER_LIMIT = 80
UTIL_CPU_LOWER_LIMIT = 70
UTIL_CPU_UPPER_LIMIT = 80
UTIL_NET_LOWER_LIMIT = 70
UTIL_NET_UPPER_LIMIT = 80
@asyncio.coroutine
def autoscale_cluster():
  while True:
    yield from asyncio.sleep(AUTOSCALE_INTERVAL)
    # compute average
    print(datanode_usage)
    avg_util['cpu'] = datanode_usage.loc[datanode_usage['blacklisted'] == 0].loc[:,'cpu'].mean()  
    avg_util['net'] = datanode_usage.loc[datanode_usage['blacklisted'] == 0].loc[:,'net_Mbps'].mean()  
    print(avg_util)
    # check datanode resource utilization and add/remove nodes as necessary
    # TODO: do the same for metadata server utilization and metadata node scaling
    
    # Logic to add node... (if any resource util is above upper limit)
    if avg_util['dram'] > UTIL_DRAM_UPPER_LIMIT:
      # add a DRAM datanode
      print("add a dram datanode. dram util is {}".format(avg_util['dram']))
      yield from launch_dram_datanode()
    elif avg_util['flash'] > UTIL_FLASH_UPPER_LIMIT:
      # add a Flash datanode
      print("add a reflex datanode. flash util is {}".format(avg_util['flash']))
      yield from launch_flash_datanode()
    elif avg_util['cpu'] > UTIL_CPU_UPPER_LIMIT:
      # add a node to cluster
      # need to decide between DRAM or Flash node
      # check if CPU is higher on DRAM or Flash nodes 
      cpu_util_dram = datanode_usage.filter(like='50030', axis=0).loc[:, 'cpu'].mean()
      cpu_util_flash = datanode_usage.filter(like='1234', axis=0).loc[:, 'cpu'].mean()
      if cpu_util_dram > cpu_util_flash:
        print("add a dram datanode, cpu util is high")
        yield from launch_dram_datanode()
      else:
        print("add a dram datanode, cpu util is high")
        yield from launch_flash_datanode()
    elif avg_util['net'] > UTIL_NET_UPPER_LIMIT:
      # add a node to cluster
      # need to decide between DRAM or Flash node
      # check if network is higher on DRAM or Flash nodes
      net_util_dram = datanode_usage.filter(like='50030', axis=0).loc[:, 'net_Mbps'].mean()
      net_util_flash = datanode_usage.filter(like='1234', axis=0).loc[:, 'net_Mbps'].mean()
      if net_util_dram > net_util_flash:
        print("add a dram datanode, net util is high")
        yield from launch_dram_datanode()
      else:
        print("add a dram datanode, net util is high")
        yield from launch_flash_datanode()
    
    # Logic to remove node... (if all resources are below lower limit)
    elif avg_util['cpu'] < UTIL_CPU_LOWER_LIMIT and \
       avg_util['net'] < UTIL_NET_LOWER_LIMIT and \
       avg_util['dram'] < UTIL_DRAM_LOWER_LIMIT and \
       avg_util['flash'] < UTIL_FLASH_LOWER_LIMIT: 
      # remove a node with the lowest network utilization 
      datanodeip_port = datanode_usage['net_Mbps'].idxmin()
      print("Datanode with lowest net or cpu usage is: ", datanodeip_port)
      datanode_alloc.at[datanodeip_port, 'blacklisted'] = 1
      datanode_usage.at[datanodeip_port, 'blacklisted'] = 1
      datanode_provisioned.at[datanodeip_port, 'blacklisted'] = 1

      
    


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
  asyncio.async(get_capacity_stats_periodically(metadata_socket))

  # Periodically check avg utilization and run autoscale algorithm
  asyncio.async(autoscale_cluster())
 
  # Start server listening for datanode util info
  address = ("10.1.47.178", 2345) 
  coro = asyncio.start_server(handle_datanodes, *address)
  server = loop.run_until_complete(coro)
  print('Listening at {}'.format(address))
  print("Start loop...") 

  # test with dummy datanodes
  add_datanode_alloc("10.1.88.82", 50030)
  add_datanode_alloc("10.1.88.83", 1234)
  add_datanode_alloc("10.1.88.84", 50030)
  add_datanode_alloc("10.1.88.85", 1234)
  add_datanode_usage("10.1.88.82", 50030, cpu=50, net=70)
  add_datanode_usage("10.1.88.83", 1234, cpu=80, net=92)
  add_datanode_usage("10.1.88.84", 50030, cpu=90, net=95)
  add_datanode_usage("10.1.88.85", 1234, cpu=20, net=90)

  try:
    loop.run_forever()
  finally:
    server.close()
    loop.close()
