#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# Check specified submission day, alert if the data volume exceeds target.
# If we exceed any targets for the day, send an alert email.
# Targets may be found at
# s3://net-mozaws-prod-us-west-2-pipeline-metadata/telemetry-2/budget_targets.json

import sys
import json
import argparse
from boto.ses import connect_to_region as ses_connect

def pct(actual, expected):
    return "{:.1%}".format(float(actual) / expected)

def gb(num_bytes):
    return "{:.2f}GB".format(float(num_bytes) / 1024.0 / 1024.0 / 1024.0)

def fmt_err(channel, docType, actual, expected):
    sign = ">"
    if actual < expected:
        sign = "<"
    return "Channel {}, Type {}: Actual {} {} Expected {} ({})".format(
        channel, docType, gb(actual), sign, gb(expected), pct(actual, expected))

def main():
    parser = argparse.ArgumentParser(description="Check Budget Targets")
    parser.add_argument("--day", help="Day to check (YYYYMMDD)", required=True)
    parser.add_argument("--targets-file", help="JSON file containing budget targets", type=file, required=True)
    parser.add_argument("--data-file", help="JSON file containing observed data", type=file, required=True)
    parser.add_argument("--from-email", help="Email 'from:' address", required=True)
    parser.add_argument("--to-email", help="Email 'to:' address (multiple allowed)", action="append", required=True)
    parser.add_argument("--dry-run", help="Print out what would happen instead of sending alert email", action="store_true")
    parser.add_argument("--verbose", help="Print all the messages", action="store_true")
    args = parser.parse_args()

    target_day = args.day
    try:
        targets = json.load(args.targets_file)
    except Exception as e:
        print "Error parsing JSON from {}: {}".format(args.targets_file.name, e)
        return 2

    try:
        data = json.load(args.data_file)
    except Exception as e:
        print "Error parsing JSON from {}: {}".format(args.data_file.name, e)
        return 2

    errors = []
    exit_code = 0
    try:
        s = data["submission"]
        for c in targets.keys():
            if c not in s:
                if args.verbose:
                    print "warning: {} not found in data.".format(c)
                continue
            if target_day not in s[c]:
                if args.verbose:
                    print "warning: {}/{} not found in data.".format(c, target_day)
                continue

            scd = s[c][target_day]
            clients = targets[c]["clients"]
            for docType in targets[c].keys():
                if docType == "clients":
                    continue
                else:
                    if docType not in scd:
                        if args.verbose:
                            print "warning: {}/{}/{} not found in data.".format(c, target_day, docType)
                        continue
                    scdt = scd[docType]
                    expected_size = targets[c][docType]["size"] * targets[c][docType]["count"] * clients
                    actual_size = scdt["size"]
                    if actual_size > expected_size:
                        errors.append(fmt_err(c, docType, actual_size, expected_size))
                    else:
                        if args.verbose:
                            print "ok: {}".format(fmt_err(c, docType, actual_size, expected_size))
    except Exception as e:
        print "Data error: {}".format(e)
        exit_code = 3

    if len(errors) > 0:
        message = "Incoming data for {} exceeded budget targets:\n".format(args.day) + "\n".join(sorted(errors))
        subject = "TEST MESSAGE 2: Incoming Telemetry data exceeded budget targets for {}".format(args.day)
        if args.dry_run:
            print "Dry-run mode. Would have sent:"
            print "=============================="
            print "   From:", args.from_email
            print "     To:", args.to_email
            print "Subject:", subject
            print "   Body:", message
        else:
            # ses = ses_connect('us-east-1')
            ses = ses_connect('us-west-2')
            ses.send_email(
                source       = args.from_email,
                subject      = subject,
                format       = "text",
                body         = message,
                to_addresses = args.to_email
            )
    elif args.dry_run:
        print "Dry-run mode, but would not have sent any alerts."

    return exit_code

if __name__ == "__main__":
    sys.exit(main())
