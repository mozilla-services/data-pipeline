/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
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
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/mozilla-services/data-pipeline/heka/plugins/s3splitfile"
	"github.com/mozilla-services/heka/message"
	"io"
	"math"
	"os"
	"time"
)

func main() {
	flagMatch := flag.String("match", "TRUE", "message_matcher filter expression")
	flagFormat := flag.String("format", "txt", "output format [txt|json|heka|count]")
	flagOutput := flag.String("output", "", "output filename, defaults to stdout")
	flagStdin := flag.Bool("stdin", false, "read list of s3 key names from stdin")
	flagBucket := flag.String("bucket", "default-bucket", "S3 Bucket name")
	flagAWSKey := flag.String("aws-key", "", "AWS Key")
	flagAWSSecretKey := flag.String("aws-secret-key", "", "AWS Secret Key")
	flagAWSRegion := flag.String("aws-region", "us-west-2", "AWS Region")
	flagMaxMessageSize := flag.Uint64("max-message-size", 4*1024*1024, "maximum message size in bytes")
	flagWorkers := flag.Uint64("workers", 16, "number of parallel workers")
	flag.Parse()

	if !*flagStdin && flag.NArg() < 1 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *flagMaxMessageSize < math.MaxUint32 {
		maxSize := uint32(*flagMaxMessageSize)
		message.SetMaxMessageSize(maxSize)
	} else {
		fmt.Printf("Message size is too large: %d\n", flagMaxMessageSize)
		os.Exit(8)
	}

	workers := 1
	if *flagWorkers < math.MaxUint32 {
		workers = int(*flagWorkers)
	} else {
		fmt.Printf("Too many workers: %d\n", flagWorkers)
		os.Exit(8)
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
	bucket := s.Bucket(*flagBucket)

	filenameChannel := make(chan string, 1000)
	recordChannel := make(chan s3splitfile.S3Record, 1000)
	doneChannel := make(chan string, 1000)
	allDone := make(chan int)

	for i := 1; i <= workers; i++ {
		go cat(bucket, filenameChannel, recordChannel, doneChannel)
	}
	go save(recordChannel, match, *flagFormat, out, allDone)

	startTime := time.Now().UTC()
	totalFiles := 0
	pendingFiles := 0
	if *flagStdin {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			filename := scanner.Text()
			totalFiles++
			pendingFiles++
			filenameChannel <- filename
			if pendingFiles >= 1000 {
				waitFor(doneChannel, 1)
				pendingFiles--
			}
		}
		close(filenameChannel)
	} else {
		for _, filename := range flag.Args() {
			totalFiles++
			pendingFiles++
			filenameChannel <- filename
			if pendingFiles >= 1000 {
				waitFor(doneChannel, 1)
				pendingFiles--
			}
		}
		close(filenameChannel)
	}

	fmt.Printf("Waiting for last %d files\n", pendingFiles)
	waitFor(doneChannel, pendingFiles)
	close(recordChannel)
	bytesRead := <-allDone
	// All done! Win!
	duration := time.Now().UTC().Sub(startTime).Seconds()
	mb := float64(bytesRead) / 1024.0 / 1024.0
	if duration == 0.0 {
		duration = 1.0
	}
	fmt.Printf("All done processing %d files, %.2fMB in %.2f seconds (%.2fMB/s)\n", totalFiles, mb, duration, (mb / duration))
}

func cat(bucket *s3.Bucket, filenameChannel <-chan string, recordChannel chan<- s3splitfile.S3Record, doneChannel chan<- string) {
	ok := true
	for ok {
		filename, ok := <-filenameChannel
		if !ok {
			// Channel is closed
			break
		}

		catOne(bucket, filename, recordChannel)
		doneChannel <- filename
	}
}

func catOne(bucket *s3.Bucket, s3Key string, recordChannel chan<- s3splitfile.S3Record) {
	var processed int64

	for r := range s3splitfile.S3FileIterator(bucket, s3Key) {
		err := r.Err

		if err != nil && err != io.EOF {
			fmt.Printf("Error reading %s: %s\n", s3Key, err)
		} else {
			if len(r.Record) > 0 {
				processed += 1
				recordChannel <- r
			}
		}
	}

	fmt.Printf("%s: Processed: %d messages\n", s3Key, processed)
}

func save(recordChannel <-chan s3splitfile.S3Record, match *message.MatcherSpecification, format string, out *os.File, done chan<- int) {
	processed := 0
	matched := 0
	bytes := 0
	msg := new(message.Message)
	ok := true
	for ok {
		r, ok := <-recordChannel
		if !ok {
			// Channel is closed
			done <- bytes
			break
		}

		bytes += len(r.Record)

		processed += 1
		headerLen := int(r.Record[1]) + message.HEADER_FRAMING_SIZE
		if err := proto.Unmarshal(r.Record[headerLen:], msg); err != nil {
			fmt.Printf("Error unmarshalling message %d, error: %s\n", processed, err)
			continue
		}

		if !match.Match(msg) {
			continue
		}

		matched += 1

		// fmt.Printf("Saving data for %s\n", msg.GetPayload())
		switch format {
		case "count":
			// no op
		case "json":
			contents, _ := json.Marshal(msg)
			fmt.Fprintf(out, "%s\n", contents)
		case "heka":
			fmt.Fprintf(out, "%s", r.Record)
		case "offsets":
			// Use offsets mode for indexing the S3 files by clientId
			clientId, ok := msg.GetFieldValue("clientId")
			recordLength := len(r.Record) - headerLen)
			if ok {
				fmt.Fprintf(out, "%s\t%s\t%d\t%d\n", r.Key, clientId, (r.Offset + uint64(headerLen)), recordLength)
			} else {
				fmt.Printf("Missing client id in %s @ %d+%d\n", r.Key, r.Offset, recordLength)
			}
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
	fmt.Printf("Processed: %d, matched: %d messages (%.2f MB)\n", processed, matched, (float64(bytes) / 1024.0 / 1024.0))
}

func waitFor(completedChannel <-chan string, count int) {
	var completed string
	// Now wait for all the clients to complete:
	for i := 1; i <= count; i++ {
		// fmt.Printf("Waiting for client %d of %d...\n", i, count)
		completed = <-completedChannel
		fmt.Printf("Completed: %s\n", completed)
		// fmt.Printf("Finished reading %s, %d of %d completed.\n", completed, i, count)
	}
}
