/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
# ***** END LICENSE BLOCK *****/

/*

A command-line utility for listing files on Amazon S3, filtered by dimension.

*/
package main

import (
	"flag"
	"fmt"
	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/mozilla-services/data-pipeline/s3splitfile"
	"os"
	"time"
)

func main() {
	flagSchema := flag.String("schema", "", "Filename of the schema to use as a filter")
	flagBucket := flag.String("bucket", "default-bucket", "S3 Bucket name")
	flagBucketPrefix := flag.String("bucket-prefix", "", "S3 Bucket path prefix")
	flagAWSKey := flag.String("aws-key", "", "AWS Key")
	flagAWSSecretKey := flag.String("aws-secret-key", "", "AWS Secret Key")
	flagAWSRegion := flag.String("aws-region", "us-west-2", "AWS Region")
	flagDryRun := flag.Bool("dry-run", false, "Don't actually do anything, just output what would be done")
	flagVerbose := flag.Bool("verbose", false, "Print detailed info")
	flag.Parse()

	if flag.NArg() != 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	var err error
	var schema s3splitfile.Schema
	schema, err = s3splitfile.LoadSchema(*flagSchema)
	if err != nil {
		fmt.Printf("schema: %s\n", err)
		os.Exit(2)
	}

	if *flagDryRun {
		fmt.Printf("Dry Run: Would have listed files in s3://%s/%s according to filter schema %s\n",
			*flagBucket, *flagBucketPrefix, *flagSchema)
		os.Exit(0)
	}

	var b *s3.Bucket

	prefix := s3splitfile.CleanBucketPrefix(*flagBucketPrefix)

	// Initialize the S3 bucket
	auth, err := aws.GetAuth(*flagAWSKey, *flagAWSSecretKey, "", time.Now())
	if err != nil {
		fmt.Printf("Authentication error: %s\n", err)
		os.Exit(4)
	}
	region, ok := aws.Regions[*flagAWSRegion]
	if !ok {
		fmt.Printf("Parameter 'aws-region' must be a valid AWS Region\n")
		os.Exit(5)
	}
	s := s3.New(auth, region)
	b = s.Bucket(*flagBucket)

	var errCount int
	var totalCount int
	var totalSize int64

	startTime := time.Now().UTC()

	// List the keys as we see them
	for k := range s3splitfile.S3Iterator(b, prefix, schema) {
		if k.Err != nil {
			fmt.Printf("ERROR fetching key: %s\n", k.Err)
			errCount++
		} else {
			totalCount++
			totalSize += k.Key.Size
			fmt.Printf("%s\n", k.Key.Key)
		}
	}

	duration := time.Now().UTC().Sub(startTime).Seconds()

	if *flagVerbose {
		fmt.Printf("Filter matched %d files totaling %s in %.02fs (%d errors)\n",
			totalCount, s3splitfile.PrettySize(totalSize), duration, errCount)
	}
}
