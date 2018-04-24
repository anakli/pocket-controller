import subprocess as sp
import shlex
from subprocess import Popen, PIPE
import re


NAMENODE_IP = "10.1.191.110"

def get_exitcode_stdout_stderr(cmd):
    """
    Execute the external command and get its exitcode, stdout and stderr.
    """
    args = shlex.split(cmd)

    proc = Popen(args, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    exitcode = proc.returncode
    #
    return exitcode, out, err


def remove_namenode_eni():
    # get attachment id
    cmd = "aws ec2 describe-network-interfaces --filter Name=description,Values='*eni for namenode*' --query \"NetworkInterfaces[*].Attachment.AttachmentId\""
    exitcode, out, err = get_exitcode_stdout_stderr(cmd)
    pattern = r'"([A-Za-z0-9_\./\\-]*)"'
    attachment_id = re.search(pattern, out).group().strip('\"')

    # detach eni
    cmd = "aws ec2 detach-network-interface --attachment-id " + attachment_id
    sp.call(cmd, shell=True)

    # get namenode eni id
    cmd = "aws ec2 describe-network-interfaces --filter Name=description,Values='*eni for namenode*' --query \"NetworkInterfaces[*].NetworkInterfaceId\""
    exitcode, out, err = get_exitcode_stdout_stderr(cmd)
    pattern = r'"([A-Za-z0-9_\./\\-]*)"'
    namenode_eni = re.search(pattern, out).group().strip('\"')
    print "Namenode ENI id is: " + namenode_eni

    # delete eni to free up security group for kops to delete
    cmd = "aws ec2 delete-network-interface --network-interface-id " + namenode_eni
    sp.call(cmd, shell=True)
    
ddef main():
    add_lambda_security_group_ingress_rule()
    remove_namenode_eni()


if __name__ == '__main__':
    main()
