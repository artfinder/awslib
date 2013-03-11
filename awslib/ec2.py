import os
import re
import time
import boto.ec2.connection
import boto.ec2.elb
import boto.ec2
import boto.utils

### EC2 HOSTLIST QUERIES


def current_region(aws_access_key=None, aws_secret_key=None):
    if aws_access_key is None:
        aws_access_key = os.environ['AWS_ACCESS_KEY']
    if aws_secret_key is None:
        aws_secret_key = os.environ['AWS_SECRET_KEY']

    instance_metadata = boto.utils.get_instance_metadata()
    return re.match(
        "([a-z]*-[a-z]*-\d*)",
        instance_metadata["placement"]["availability-zone"]).groups()[0]


def connection(aws_access_key_id, aws_secret_access_key, region=None):
    if region:
        ec2c = boto.ec2.connect_to_region(
            region, aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key)
    else:
        ec2c = boto.ec2.connection.EC2Connection(
            aws_access_key_id, aws_secret_access_key)
    return ec2c


def current_instance(aws_access_key=None, aws_secret_key=None, region=None):
    if aws_access_key is None:
        aws_access_key = os.environ['AWS_ACCESS_KEY']
    if aws_secret_key is None:
        aws_secret_key = os.environ['AWS_SECRET_KEY']

    current_instance = boto.utils.get_instance_metadata()
    if region is None:
        region = current_region(
            aws_access_key=aws_access_key, aws_secret_key=aws_secret_key)
    ec2c = connection(aws_access_key, aws_secret_key, region)
    reservations = ec2c.get_all_instances([current_instance["instance-id"]])
    try:
        return reservations[0].instances[0]
    except IndexError:
        return None


def instances_from_security_group(
    group_name,
    region=None,
    aws_access_key=None,
    aws_secret_key=None
):
    if aws_access_key is None:
        aws_access_key = os.environ['AWS_ACCESS_KEY']
    if aws_secret_key is None:
        aws_secret_key = os.environ['AWS_SECRET_KEY']

    ec2c = connection(aws_access_key, aws_secret_key, region)
    sg = ec2c.get_all_security_groups(groupnames=[group_name])[0]
    return sg.instances()


def hosts_from_security_group(
    group_name,
    display=False,
    region=None,
    aws_access_key=None,
    aws_secret_key=None,
    return_private_dns_names=False,
):
    sg_hosts = []
    for i in instances_from_security_group(
        group_name,
        region,
        aws_access_key,
        aws_secret_key
    ):
        if i.state != 'running':
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


def flip_elb_to(elb_name, instance_ids, instance_check=None, region=None):
    """
    Flip the ELB to point to the new instances, returning the list of
    its previous instance ids.
    """
    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_secret_key = os.environ['AWS_SECRET_KEY']
    ec2c = connection(aws_access_key, aws_secret_key, region)
    elbc = boto.ec2.elb.ELBConnection(
        aws_access_key, aws_secret_key, region=ec2c.region)
    bh = elbc.get_all_load_balancers(elb_name)[0]
    old_instance_ids = map(lambda x: x.id, bh.instances)
    bh.register_instances(instance_ids)
    while True:
        ih = elbc.describe_instance_health(
            elb_name,
            instances=instance_ids
            )
        if len(filter(lambda x: x.state != u'InService', ih)) == 0:
            if instance_check is None or \
                    len(filter(instance_check, instance_ids)) == 0:
                break
        # this usually takes 10-15 seconds minimum
        time.sleep(5)
    bh.deregister_instances(old_instance_ids)
    return old_instance_ids


def flip_elb_to_security_group(
    elb_name,
    security_group,
    instance_check=None,
    region=None):
    """
    Flip the ELB to all instances in a given security group.
    Return the instance ids previously registered with the ELB.
    """
    instances = hosts_from_security_group(security_group, region=region)
    return flip_elb_to(
        elb_name,
        [i.id for i in instances if i.state == 'running'],
        instance_check,
        region=None,
    )


def hosts_from_elb(elb_name, region=None):
    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_secret_key = os.environ['AWS_SECRET_KEY']

    ec2c = connection(aws_access_key, aws_secret_key, region)
    elbc = boto.ec2.elb.ELBConnection(
        aws_access_key, aws_secret_key, region=ec2c.region)
    bh = elbc.get_all_load_balancers(elb_name)[0]
    rs = ec2c.get_all_instances(instance_ids=map(lambda x: x.id, bh.instances))
    elb_hosts = []
    for r in rs:
        for i in r.instances:
            if i.state != 'running':
                continue
            elb_hosts.append(i.public_dns_name)
    return elb_hosts


def hosts_by_instance_id(ids, region=None):
    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_secret_key = os.environ['AWS_SECRET_KEY']

    ec2c = connection(aws_access_key, aws_secret_key, region)
    reservations = ec2c.get_all_instances(ids.split('|'))
    hosts = []
    for r in reservations:
        for i in r.instances:
            if i.state != 'running':
                continue
            hosts.append(i.public_dns_name)
    return hosts
