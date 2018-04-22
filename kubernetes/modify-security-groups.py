import subprocess as sp
import shlex
from subprocess import Popen, PIPE
import re

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


def main():
    # get group id for pocket-kubernetes-lax security group 
    cmd = "aws ec2 describe-security-groups --filters Name=group-name,Values='*pocket-kubernetes-lax*' --query \"SecurityGroups[*].GroupId\""  
    exitcode, out, err = get_exitcode_stdout_stderr(cmd)
    pattern = r'"([A-Za-z0-9_\./\\-]*)"'
    pocket_lax_groupid = re.search(pattern, out).group().strip('\"')

    # get group id for node security group
    cmd = "aws ec2 describe-security-groups --filters Name=group-name,Values='*node*' --query \"SecurityGroups[*].GroupId\""  
    exitcode, out, err = get_exitcode_stdout_stderr(cmd)
    pattern = r'"([A-Za-z0-9_\./\\-]*)"'
    node_groupid = re.search(pattern, out).group().strip('\"')
    #print pocket_lax_groupid, node_groupid

    # add ingress rule for node group to accept traffic from pocket-kubernetes-lax group (this is group lamdbas will be in)
    modify_security_group_command = 'aws ec2 authorize-security-group-ingress --group-id ' + node_groupid + ' --protocol all --source-group ' + pocket_lax_groupid
    print modify_security_group_command
    sp.call(modify_security_group_command, shell=True)


if __name__ == '__main__':
    main()
