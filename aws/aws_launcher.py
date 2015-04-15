#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# ideas from
# https://github.com/mozilla/telemetry-server/tree/master/provisioning/aws

import argparse
import simplejson as json
import sys
import traceback
import time
import boto.ec2
from boto.ec2.blockdevicemapping import BlockDeviceType
from boto.ec2.blockdevicemapping import BlockDeviceMapping

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
    "tags": {
        "Name": "data-pipeline-analysis"
    }
}


class Launcher(object):
    def __init__(self):
        parser = self.get_arg_parser()
        args = parser.parse_args()
        self.aws_key = args.aws_key
        self.aws_secret_key = args.aws_secret_key
        self.read_user_data()
        self.setup_config(args.config_file)

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
            required=True,
            default=None
        )
        parser.add_argument(
            "-s", "--aws-secret-key",
            help="AWS Secret Key",
            required=False,
            default=None
        )
        return parser

    def read_user_data(self):
        fh = open("userdata.sh", "r")
        self.user_data = fh.read()
        fh.close()

    def setup_config(self, config_file):
        self.config = default_config
        if config_file:
            user_config = json.load(config_file)
            for key in user_config:
                self.config[key] = user_config[key]

    def fire_up_instance(self):
        self.conn = boto.ec2.connect_to_region(
            self.config["region"],
            aws_access_key_id=self.aws_key,
            aws_secret_access_key=self.aws_secret_key
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

        self.conn.create_tags([instance.id], self.config["tags"])
        while instance.state == 'pending':
            print "Instance is pending -- Waiting 10s for instance", \
                instance.id, "to start up..."
            time.sleep(10)
            instance.update()

        print "Instance", instance.id, "is", instance.state
        print "ubuntu@%s" % instance.public_dns_name


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
