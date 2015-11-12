/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
# ***** END LICENSE BLOCK *****/

/*

A command-line utility for fetching a set of files on Amazon S3 as a single data
stream.

*/
package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"io"
	"math"
	"os"
	"time"
)

var bytesRead uint64

func main() {
	flagStdin := flag.Bool("stdin", false, "read list of s3 key names from stdin")
	flagBucket := flag.String("bucket", "default-bucket", "S3 Bucket name")
	flagAWSKey := flag.String("aws-key", "", "AWS Key")
	flagAWSSecretKey := flag.String("aws-secret-key", "", "AWS Secret Key")
	flagAWSRegion := flag.String("aws-region", "us-west-2", "AWS Region")
	flagConnectTimeout := flag.Uint64("connect_timeout", 60, "Max seconds to wait for an S3 connection")
	flagReadTimeout := flag.Uint64("read_timeout", 300, "Max seconds to wait for an S3 file read to complete")
	flag.Parse()

	if !*flagStdin && flag.NArg() < 1 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	var connectTimeout uint32
	if *flagConnectTimeout < math.MaxUint32 {
		connectTimeout = uint32(*flagConnectTimeout)
	} else {
		fmt.Fprintf(os.Stderr, "Connection Timeout is too large:%d.\n", flagConnectTimeout)
		os.Exit(8)
	}

	var readTimeout uint32
	if *flagReadTimeout < math.MaxUint32 {
		readTimeout = uint32(*flagReadTimeout)
	} else {
		fmt.Fprintf(os.Stderr, "Read Timeout is too large:%d.\n", flagReadTimeout)
		os.Exit(8)
	}

	auth, err := aws.GetAuth(*flagAWSKey, *flagAWSSecretKey, "", time.Now())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Authentication error: %s\n", err)
		os.Exit(4)
	}
	region, ok := aws.Regions[*flagAWSRegion]
	if !ok {
		fmt.Fprintf(os.Stderr, "Parameter 'aws-region' must be a valid AWS Region\n")
		os.Exit(5)
	}
	s := s3.New(auth, region)
	if connectTimeout > 0 {
		s.ConnectTimeout = time.Duration(connectTimeout) * time.Second
	}
	if readTimeout > 0 {
		s.ReadTimeout = time.Duration(readTimeout) * time.Second
	}
	bucket := s.Bucket(*flagBucket)

	startTime := time.Now().UTC()
	totalFiles := 0
	if *flagStdin {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			filename := scanner.Text()
			totalFiles++
			cat(bucket, filename)
		}
	} else {
		for _, filename := range flag.Args() {
			totalFiles++
			cat(bucket, filename)
		}
	}

	duration := time.Now().UTC().Sub(startTime).Seconds()
	mb := float64(bytesRead) / 1024.0 / 1024.0
	if duration == 0.0 {
		duration = 1.0
	}
	fmt.Fprintf(os.Stderr, "All done processing %d files, %.2fMB in %.2f seconds (%.2fMB/s)\n", totalFiles, mb, duration, (mb / duration))
}

// Cat the data from a single S3 key
func cat(bucket *s3.Bucket, s3Key string) {
	var lastGoodOffset uint64

RetryS3:
	for attempt := 1; attempt <= 5; attempt++ {
		rc, err := getS3Reader(bucket, s3Key, lastGoodOffset)
		if err != nil && err != io.EOF {
			fmt.Fprintf(os.Stderr, "Error in attempt %d reading %s at offset %d: %s\n", attempt, s3Key, lastGoodOffset, err)
			continue RetryS3
		} else {
			nr := bufio.NewReader(rc)
			n, err := nr.WriteTo(os.Stdout)
			if err != nil && err != io.EOF {
				fmt.Fprintf(os.Stderr, "Error in attempt %d writing %s at offset %d: %s\n", attempt, s3Key, lastGoodOffset, err)
				rc.Close()
				continue RetryS3
			}
			lastGoodOffset += uint64(n)
			bytesRead += uint64(n)
		}
		rc.Close()
		break
	}
}

// Callers must call Close() on rc.
func getS3Reader(bucket *s3.Bucket, s3Key string, offset uint64) (rc io.ReadCloser, err error) {
	if offset == 0 {
		rc, err = bucket.GetReader(s3Key)
		return
	}

	headers := map[string][]string{
		"Range": []string{fmt.Sprintf("bytes=%d-", offset)},
	}

	resp, err := bucket.GetResponseWithHeaders(s3Key, headers)

	if resp != nil {
		rc = resp.Body
	}
	return
}
