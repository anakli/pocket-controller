import subprocess as sp
import shlex
from subprocess import Popen, PIPE
import re


NAMENODE_IP = "10.1.0.10"

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


def prep_i3instances(private_subnetid, node_groupid):
    # get instance ids for i3 instances
    cmd = "aws ec2 describe-instances --filters Name=tag:Name,Values='nvme-nodes.pocketcluster*' " \
           + "Name=instance-state-name,Values='running'  --query \"Reservations[*].Instances[*].InstanceId\""
    exitcode, out, err = get_exitcode_stdout_stderr(cmd)
    pattern = r'"([A-Za-z0-9_\./\\-]*)"'
    p = re.compile(pattern)
    i3_instance_ids = p.findall(out) 
    print "i3 instance ids findall: ", i3_instance_ids 
    
    i = 0
    for i3_instance_id in i3_instance_ids:
        # create elastic network interface 
        create_eni_command = "aws ec2 create-network-interface --subnet-id " + private_subnetid + " --description \"eni for i3\" --groups " \
                               + node_groupid  
        print create_eni_command
        sp.call(create_eni_command, shell=True)
        
        # get i3 eni id
        cmd = "aws ec2 describe-network-interfaces --filter Name=description,Values='*eni for i3*' --query \"NetworkInterfaces[*].NetworkInterfaceId\""
        exitcode, out, err = get_exitcode_stdout_stderr(cmd)
        pattern = r'"([A-Za-z0-9_\./\\-]*)"'
        #i3_eni = re.search(pattern, out).group().strip('\"')
        i3_enis = p.findall(out)
        i3_eni = i3_enis[i]
        i = i + 1 #FIXME: are they always listed in order created?
        print "i3 ENI id is: " + i3_eni
    
        # attach eni to i3 instance
        attach_eni_command = "aws ec2 attach-network-interface --network-interface-id " + i3_eni + " --instance-id " + i3_instance_id + " --device-index 1"
        print attach_eni_command
        sp.call(attach_eni_command, shell=True)
   


def add_namenode_eni():
    # get subnet id for private subnet
    cmd = "aws ec2 describe-subnets --filters Name=tag:Name,Values='pocket-kube-private' --query \"Subnets[*].SubnetId\""
    exitcode, out, err = get_exitcode_stdout_stderr(cmd)
    pattern = r'"([A-Za-z0-9_\./\\-]*)"'
    private_subnetid = re.search(pattern, out).group().strip('\"')

    # get security group for nodes
    cmd = "aws ec2 describe-security-groups --filters Name=group-name,Values='*node*' --query \"SecurityGroups[*].GroupId\""  
    exitcode, out, err = get_exitcode_stdout_stderr(cmd)
    pattern = r'"([A-Za-z0-9_\./\\-]*)"'
    node_groupid = re.search(pattern, out).group().strip('\"')

    # create elastic network interface with predefined IP
    create_eni_command = "aws ec2 create-network-interface --subnet-id " + private_subnetid + " --description \"eni for namenode\" --groups " \
                           + node_groupid + " --private-ip-address " + NAMENODE_IP 
    print create_eni_command
    sp.call(create_eni_command, shell=True)
    
    # get namenode eni id
    cmd = "aws ec2 describe-network-interfaces --filter Name=description,Values='*eni for namenode*' --query \"NetworkInterfaces[*].NetworkInterfaceId\""
    exitcode, out, err = get_exitcode_stdout_stderr(cmd)
    pattern = r'"([A-Za-z0-9_\./\\-]*)"'
    namenode_eni = re.search(pattern, out).group().strip('\"')
    print "Namenode ENI id is: " + namenode_eni


    # get instance id for namendoe
    cmd = "aws ec2 describe-instances --filters Name=tag:Name,Values='metadata-nodes.pocketcluster*' " \
           + "Name=instance-state-name,Values='running'  --query \"Reservations[*].Instances[*].InstanceId\""
    exitcode, out, err = get_exitcode_stdout_stderr(cmd)
    pattern = r'"([A-Za-z0-9_\./\\-]*)"'
    namenode_instance_id = re.search(pattern, out).group().strip('\"')
 
    # attach eni to namenode
    attach_eni_command = "aws ec2 attach-network-interface --network-interface-id " + namenode_eni + " --instance-id " + namenode_instance_id + " --device-index 1"
    print attach_eni_command
    sp.call(attach_eni_command, shell=True)

    return private_subnetid, node_groupid


def add_lambda_security_group_ingress_rule():
    # get group id for pocket-kubernetes-lax security group 
    cmd = "aws ec2 describe-security-groups --filters Name=group-name,Values='*pocket-kube-relax*' --query \"SecurityGroups[*].GroupId\""  
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


def main():
    add_lambda_security_group_ingress_rule()
    private_subnet_id, node_groupid = add_namenode_eni()
    prep_i3instances(private_subnet_id, node_groupid)


if __name__ == '__main__':
    main()
