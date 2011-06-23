import os
import boto.ec2.connection
import boto.ec2.elb
import boto.ec2

### EC2 HOSTLIST QUERIES

def hosts_from_security_group(
    group_name,
    display=False,
    region=None,
    aws_access_key=None,
    aws_secret_key=None,
    return_private_dns_names=False,
):
    if aws_access_key is None:
        aws_access_key = os.environ['AWS_ACCESS_KEY']
    if aws_secret_key is None:
        aws_secret_key = os.environ['AWS_SECRET_KEY']
    sg_hosts = []
    
    if region:
        ec2c = boto.ec2.connect_to_region(region, aws_access_key_id=env.access_key_id, aws_secret_access_key=env.secret_access_key)
    else:
        ec2c = boto.ec2.connection.EC2Connection(aws_access_key, aws_secret_key)
    sg   = ec2c.get_all_security_groups( groupnames=[group_name] )[0]
    for i in sg.instances():
        if i.state!='running':
            if display:
                print "Skipping", i.private_dns_name, "(not running)"
            continue
        if return_private_dns_names:
            sg_hosts.append(i.private_dns_name)
        else:
            sg_hosts.append(i.public_dns_name)
        if display:
            print i.private_dns_name
    return sg_hosts


def hosts_from_elb(elb_name):
    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_secret_key = os.environ['AWS_SECRET_KEY']

    ec2c = boto.ec2.connection.EC2Connection(aws_access_key, aws_secret_key)
    elbc = boto.ec2.elb.ELBConnection(aws_access_key, aws_secret_key)
    bh = elbc.get_all_load_balancers(elb_name)[0]
    rs = ec2c.get_all_instances(instance_ids=map(lambda x:x.id, bh.instances))
    elb_hosts = []
    for r in rs:
        for i in r.instances:
            if i.state!='running':
                continue
            elb_hosts.append(i.public_dns_name)
    return elb_hosts


def hosts_by_instance_id(ids):
    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_secret_key = os.environ['AWS_SECRET_KEY']

    ec2c = boto.ec2.connection.EC2Connection(aws_access_key, aws_secret_key)
    reservations = ec2c.get_all_instances(ids.split('|'))
    hosts = []
    for r in reservations:
        for i in r.instances:
            if i.state!='running':
                continue
            hosts.append(i.public_dns_name)
    return hosts