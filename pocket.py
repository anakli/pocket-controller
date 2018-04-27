#######################################################
##  Python library/API to communicate with Pocket    ##
#######################################################

import time
import sys
import os
import socket
import struct
import errno
import libpocket
from subprocess import call, Popen

PORT = 2345
HOSTNAME = "localhost"

CONTROLLER_IP = "10.1.45.35"
CONTROLLER_PORT = 4321

MAX_DIR_DEPTH = 16

INT = 4
LONG = 8
FLOAT = 4
SHORT = 2
BYTE = 1

REQ_STRUCT_FORMAT = "!iqhhi" # msg_len (INT), ticket (LONG LONG), cmd (SHORT), cmd_type (SHORT), register_type (BYTE)
REQ_LEN_HDR = SHORT + SHORT + INT # CMD, CMD_TYPE, IOCTL_OPCODE (note: doesn't include msg_len or ticket from NaRPC hdr)

RESP_STRUCT_FORMAT = "!iqhhi" # msg_len (INT), ticket (LONG LONG), cmd (SHORT), error (SHORT), register_opcode (BYTE)
RESP_LEN_BYTES = INT + LONG + SHORT + SHORT + INT # MSG_LEN, TICKET, CMD, ERROR, REGISTER_OPCODE 

TICKET = 1000
RPC_JOB_CMD = 14
JOB_CMD = 14
REGISTER_OPCODE = 0
DEREGISTER_OPCODE = 1


def launch_dispatcher_from_lambda():
  return 

def launch_dispatcher(crail_home_path):
  return 

# TODO: add heuristics
def register_job(jobname):
  # connect to controller
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.connect((CONTROLLER_IP, CONTROLLER_PORT))
  #TODO: add hints

  # send register request to controller
  msg_packer = struct.Struct(REQ_STRUCT_FORMAT + "i" + str(len(jobname)) + "s") # len(jobname) (INT) + jobname (STRING)
  msgLen = REQ_LEN_HDR + INT + len(jobname)
  sampleMsg = (msgLen, TICKET, RPC_JOB_CMD, JOB_CMD, REGISTER_OPCODE, len(jobname), jobname)
  pkt = msg_packer.pack(*sampleMsg)
  sock.sendall(pkt)

  # get jobid response
  data = sock.recv(RESP_LEN_BYTES + INT)
  resp_packer = struct.Struct(RESP_STRUCT_FORMAT + "i")
  [length, ticket, type_, err, opcode, jobIdNum] = resp_packer.unpack(data)
  if err != 0:
    print("Error removing datanode: ", err)
  else:
    jobid = jobname + "-" + str(jobIdNum)
    print("Registered jobid ", jobid)
  return jobid

 
  #res = pocket.MakeDir(jobid)
  #if res != 0:
  #  print("Error registering job!")
  #return res

def connect(hostname, port):
  pocketHandle = libpocket.PocketDispatcher()
  res = pocketHandle.Initialize(hostname, port)
  if res != 0:
    print("Connecting to metadata server failed!")

  return pocketHandle

def put(pocket, src_filename, dst_filename, jobid, PERSIST_AFTER_JOB=False):  
  '''
  Send a PUT request to Pocket to write key

  :param pocket:           pocketHandle returned from connect()
  :param str src_filename: name of local file containing data to PUT
  :param str dst_filename: name of file/key in Pocket which writing to
  :param str jobid:        id unique to this job, used to separate keyspace for job
  :param PERSIST_AFTER_JOB:optional hint, if True, data written to table persisted after job done
  :return: the Pocket dispatcher response 
  '''

  if PERSIST_AFTER_JOB:
    set_filename = jobid + "-persist/" + dst_filename
  else:
    set_filename = jobid + "/" + dst_filename

  res = pocket.PutFile(src_filename, set_filename)

  return res

 
def get(pocket, src_filename, dst_filename, jobid, DELETE_AFTER_READ=False):  
  '''
  Send a GET request to Pocket to read key

  :param pocket:           pocketHandle returned from connect()
  :param str src_filename: name of file/key in Pocket from which reading
  :param str dst_filename: name of local file where want to store data from GET
  :param str jobid:        id unique to this job, used to separate keyspace for job
  :param DELETE_AFTER_READ:optional hint, if True, data deleted after job done
  :return: the Pocket dispatcher response 
  '''

  get_filename = jobid + "/" + src_filename

  res = pocket.GetFile(get_filename, dst_filename)
  if res != 0:
    print("GET failed!")
    return res

  if DELETE_AFTER_READ:
    res = delete(pocket, src_filename, jobid);

  return res


def delete(pocket, src_filename, jobid):  
  '''
  Send a DEL request to Pocket to delete key

  :param pocket:           pocketHandle returned from connect()
  :param str src_filename: name of file/key in Pocket which deleting
  :param str jobid:        id unique to this job, used to separate keyspace for job
  :return: the Pocket dispatcher response 
  '''
  
  if src_filename:
    src_filename = jobid + "/" + src_filename
  else:
    src_filename = jobid

  res = pocket.DeleteFile(src_filename) #FIXME: or DeleteDir if want recursive delete always?? 
  
  return res


def create_dir(pocket, src_filename, jobid):  
  '''
  Send a CREATE DIRECTORY request to Pocket

  :param pocket:           pocketHandle returned from connect()
  :param str src_filename: name of directory to create in Pocket 
  :param str jobid:        id unique to this job, used to separate keyspace for job
  :return: the Pocket dispatcher response 
  '''
  
  if src_filename:
    src_filename = jobid + "/" + src_filename
  else:
    src_filename = jobid

  res = pocket.MakeDir(src_filename)

  return res


def close(pocket):  
  '''
  Send a CLOSE request to PocketFS

  :param pocket:           pocketHandle returned from connect()
  :return: the Pocket dispatcher response 
  '''
  return pocket.Close() #TODO
