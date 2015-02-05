/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Mark Reid (mreid@mozilla.com)
# ***** END LICENSE BLOCK *****/

/*

A command-line utility for counting, viewing, filtering, and extracting Heka
protobuf logs from files on Amazon S3.

*/
package main

import (
	"bufio"
	"code.google.com/p/gogoprotobuf/proto"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/mozilla-services/heka/message"
	"io"
	"os"
	"time"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/mozilla-services/data-pipeline/heka/plugins/s3splitfile"
)

func main() {
	flagMatch := flag.String("match", "TRUE", "message_matcher filter expression")
	flagFormat := flag.String("format", "txt", "output format [txt|json|heka|count]")
	flagOutput := flag.String("output", "", "output filename, defaults to stdout")
	flagStdin := flag.Bool("stdin", false, "read list of s3 key names from stdin")
    flagBucket := flag.String("bucket", "default-bucket", "S3 Bucket name")
    flagAWSKey := flag.String("aws-key", "DUMMY", "AWS Key")
    flagAWSSecretKey := flag.String("aws-secret-key", "DUMMY", "AWS Secret Key")
    flagAWSRegion := flag.String("aws-region", "us-west-2", "AWS Region")
	flag.Parse()

	if !*flagStdin && flag.NArg() < 1 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	var err error
	var match *message.MatcherSpecification
	if match, err = message.CreateMatcherSpecification(*flagMatch); err != nil {
		fmt.Printf("Match specification - %s\n", err)
		os.Exit(2)
	}

	var out *os.File
	if "" == *flagOutput {
		out = os.Stdout
	} else {
		if out, err = os.OpenFile(*flagOutput, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
			fmt.Printf("%s\n", err)
			os.Exit(3)
		}
		defer out.Close()
	}

	auth := aws.Auth{AccessKey: *flagAWSKey, SecretKey: *flagAWSSecretKey}
	region, ok := aws.Regions[*flagAWSRegion]
	if !ok {
		fmt.Printf("Parameter 'aws-region' must be a valid AWS Region\n")
		os.Exit(4)
	}
	s := s3.New(auth, region)
	bucket := s.Bucket(*flagBucket)

	if *flagStdin {
		scanner := bufio.NewScanner(os.Stdin)
	    for scanner.Scan() {
	        filename := scanner.Text()
			cat(bucket, filename, match, *flagFormat, out)
	    }
	} else {
		for _, filename := range flag.Args() {
			cat(bucket, filename, match, *flagFormat, out)
		}
	}
}

func cat(bucket *s3.Bucket, s3Key string, match *message.MatcherSpecification, format string, out *os.File) {
	var offset, processed, matched int64
	msg := new(message.Message)

	for r := range s3splitfile.S3FileIterator(bucket, s3Key) {
		n := r.BytesRead
		record := r.Record
		err := r.Err

		if err != nil && err != io.EOF {
			fmt.Printf("Error reading %s: %s\n", s3Key, err)
		} else {
			if len(record) > 0 {
				processed += 1
				headerLen := int(record[1]) + message.HEADER_FRAMING_SIZE
				if err = proto.Unmarshal(record[headerLen:], msg); err != nil {
					fmt.Printf("Error unmarshalling message %d at offset: %d error: %s\n", processed, offset, err)
					continue
				}

				if !match.Match(msg) {
					continue
				}
				matched += 1

				switch format {
				case "count":
					// no op
				case "json":
					contents, _ := json.Marshal(msg)
					fmt.Fprintf(out, "%s\n", contents)
				case "heka":
					fmt.Fprintf(out, "%s", record)
				default:
					fmt.Fprintf(out, "Timestamp: %s\n"+
						"Type: %s\n"+
						"Hostname: %s\n"+
						"Pid: %d\n"+
						"UUID: %s\n"+
						"Logger: %s\n"+
						"Payload: %s\n"+
						"EnvVersion: %s\n"+
						"Severity: %d\n"+
						"Fields: %+v\n\n",
						time.Unix(0, msg.GetTimestamp()), msg.GetType(),
						msg.GetHostname(), msg.GetPid(), msg.GetUuidString(),
						msg.GetLogger(), msg.GetPayload(), msg.GetEnvVersion(),
						msg.GetSeverity(), msg.Fields)
				}
			}
		}

		offset += int64(n)
	}

	fmt.Printf("%s: Processed: %d, matched: %d messages\n", s3Key, processed, matched)
}
