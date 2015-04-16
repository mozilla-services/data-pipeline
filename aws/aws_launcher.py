#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# ideas from
# https://github.com/mozilla/telemetry-server/tree/master/provisioning/aws

import argparse
import json
import sys
import traceback
import time

try:
    import boto.ec2
    from boto.ec2.blockdevicemapping import BlockDeviceType
    from boto.ec2.blockdevicemapping import BlockDeviceMapping
except:
    sys.stderr.write("Requires boto; try 'pip install boto'\n")
    exit(1)

default_config = {
    "image": "ami-ace67f9c",
    "region": "us-west-2",
    "key_name": "kparlante-pipeline-dev",
    "instance_type": "c3.2xlarge",
    "security_groups": ["mreid-heka-build-and-test"],
    "iam_role": "pipeline-dev-iam-access-IamInstanceProfile-YVZ950U23IFP",
    "shutdown": "stop",
    "ephemeral_map": {
        "/dev/xvdb": "ephemeral0",
        "/dev/xvdc": "ephemeral1"
    },
    "owner": "datapipeline",
    "tags": {
        "Name": "pipeline-analysis",
        "App": "pipeline",
        "Type": "analysis",
        "Env": "dev",
    }
}


class Launcher(object):
    def __init__(self):
        parser = self.get_arg_parser()
        args = parser.parse_args()
        self.read_user_data()
        self.setup_config(args)

    def get_arg_parser(self):
        parser = argparse.ArgumentParser(description='Launch EC2 instances')
        parser.add_argument(
            "-c", "--config-file",
            help="JSON config file",
            type=file,
            default=None
        )
        parser.add_argument(
            "-k", "--aws-key",
            help="AWS Key",
            default=None
        )
        parser.add_argument(
            "-s", "--aws-secret-key",
            help="AWS Secret Key",
            default=None
        )
        return parser

    def read_user_data(self):
        with open("userdata.sh", "r") as fh:
            self.user_data = fh.read()

    def setup_config(self, args):
        self.config = default_config.copy()
        if args.config_file:
            user_config = json.load(args.config_file)
            self.config.update(user_config)
        if args.aws_key:
            self.config["aws_key"] = args.aws_key
        if args.aws_secret_key:
            self.config["aws_secret_key"] = args.aws_secret_key

    def fire_up_instance(self):
        self.conn = boto.ec2.connect_to_region(
            self.config["region"],
            aws_access_key_id=self.config.get("aws_key", None),
            aws_secret_access_key=self.config.get("aws_secret_key", None)
        )

        mapping = BlockDeviceMapping()
        for device, eph_name in self.config["ephemeral_map"].iteritems():
            mapping[device] = BlockDeviceType(ephemeral_name=eph_name)

        reservation = self.conn.run_instances(
            self.config["image"],
            key_name=self.config["key_name"],
            instance_type=self.config["instance_type"],
            security_groups=self.config["security_groups"],
            block_device_map=mapping,
            user_data=self.user_data,
            instance_profile_name=self.config["iam_role"],
            instance_initiated_shutdown_behavior=self.config["shutdown"]
        )

        instance = reservation.instances[0]

        owner_tag = {"Owner": self.config["owner"]}
        self.conn.create_tags([instance.id], owner_tag)
        self.conn.create_tags([instance.id], self.config["tags"])

        while instance.state == 'pending':
            print "Instance is pending -- Waiting 10s for instance", \
                instance.id, "to start up..."
            time.sleep(10)
            instance.update()

        print ("Instance {0} is {1}".format(instance.id, instance.state))
        print ("ubuntu@{0}".format(instance.public_dns_name))


def main():
    try:
        launcher = Launcher()
        launcher.fire_up_instance()
        return 0
    except Exception, e:
        print "Error:", e
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
