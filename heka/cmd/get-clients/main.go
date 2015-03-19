/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
# ***** END LICENSE BLOCK *****/

/*

A command-line utility for getting data by clientid.

*/
package main

import (
	"bufio"
	// "code.google.com/p/gogoprotobuf/proto"
	// "encoding/json"
	"flag"
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	// "github.com/mozilla-services/data-pipeline/heka/plugins/s3splitfile"
	"github.com/mozilla-services/heka/message"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

type MessageLocation struct {
	Key string
	// ClientId string
	Offset uint32
	Length uint32
}

type OffsetCache struct {
	messages map[string][]MessageLocation
	count    int
}

func NewOffsetCache() *OffsetCache {
	return &OffsetCache{map[string][]MessageLocation{}, 0}
}

func (o *OffsetCache) Add(clientId string, offset MessageLocation) {
	entries, ok := o.messages[clientId]
	if !ok {
		entries = []MessageLocation{}
	}

	o.messages[clientId] = append(entries, offset)
	// fmt.Printf("%s now has %d items\n", clientId, len(o.messages[clientId]))
	o.count++
}

func (o *OffsetCache) Get(clientId string) []MessageLocation {
	entries, ok := o.messages[clientId]
	if !ok {
		return []MessageLocation{}
	}
	return entries
}

func (o *OffsetCache) Size() int {
	return o.count
}

//var offsets = nil

func main() {
	// flagMatch := flag.String("match", "TRUE", "message_matcher filter expression")
	// flagFormat := flag.String("format", "txt", "output format [txt|json|heka|count]")
	flagOutput := flag.String("output", "", "output filename, defaults to stdout")
	flagOffsets := flag.String("offsets", "", "file containing offset info")
	flagBucket := flag.String("bucket", "", "S3 Bucket name")
	flagAWSKey := flag.String("aws-key", "", "AWS Key")
	flagAWSSecretKey := flag.String("aws-secret-key", "", "AWS Secret Key")
	flagAWSRegion := flag.String("aws-region", "us-west-2", "AWS Region")
	flagMaxMessageSize := flag.Uint64("max-message-size", 4*1024*1024, "maximum message size in bytes")
	flagWorkers := flag.Uint64("workers", 16, "number of parallel workers")
	flag.Parse()

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
		fmt.Printf("Too many workers: %d. Are you crazy?\n", flagWorkers)
		os.Exit(8)
	}

	var err error
	// var match *message.MatcherSpecification
	// if match, err = message.CreateMatcherSpecification(*flagMatch); err != nil {
	// 	fmt.Printf("Match specification - %s\n", err)
	// 	os.Exit(2)
	// }

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

	// TODO: read offsets file globally
	offsets, err := readOffsets(*flagOffsets)
	if err != nil {
		fmt.Printf("Error reading offsets file: %s\n", err)
		os.Exit(9)
	}

	fmt.Printf("Loaded %d offsets\n", offsets.Size())

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

	clientIdChannel := make(chan string, 1000)
	recordChannel := make(chan []byte, 1000)
	done := make(chan bool)
	doneSaving := make(chan bool)

	for i := 1; i <= workers; i++ {
		fmt.Printf("Starting worker %d\n", i)
		go getClientRecords(bucket, offsets, clientIdChannel, recordChannel, done)
	}
	go saveRecords(recordChannel, doneSaving)

	startTime := time.Now().UTC()
	totalClientIds := 0
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		clientId := scanner.Text()

		fmt.Printf("TODO: get %s\n", clientId)
		totalClientIds++
		clientIdChannel <- clientId
	}
	close(clientIdChannel)

	<-done
	close(recordChannel)
	<-doneSaving
	duration := time.Now().UTC().Sub(startTime).Seconds()
	fmt.Printf("All done processing %d clientIds in %.2f seconds\n", totalClientIds, duration)
}

func makeInt(numstr string) (uint32, error) {
	i, err := strconv.ParseInt(string(numstr), 10, 64)
	if err != nil {
		return 0, err
	}
	if i < 0 || i > math.MaxUint32 {
		return 0, fmt.Errorf("Error parsing %d as uint32")
	}
	return uint32(i), nil
}

func getClientRecords(bucket *s3.Bucket, offsets *OffsetCache, todoChannel <-chan string, recordChannel chan<- []byte, done chan<- bool) {
	fmt.Printf("One client starting up\n")
	ok := true
	for ok {
		clientId, ok := <-todoChannel
		if !ok {
			// Channel is closed
			done <- true
			break
		}

		var headers = map[string][]string{
			"Range": []string{""},
		}

		fmt.Printf("Fetching data for %s\n", clientId)
		for _, o := range offsets.Get(clientId) {

			headers["Range"][0] = fmt.Sprintf("bytes=%d-%d", o.Offset, o.Offset+o.Length)
			// rangeHeader := fmt.Sprintf("bytes=%d-%d", o.Offset, o.Offset+o.Length)
			fmt.Printf("Getting %s: %s @ %d+%d // %v\n", clientId, o.Key, o.Offset, o.Length, headers)
			// record, err := getClientRecord(bucket, o)
			// if err != nil {
			// 	fmt.Printf("Error fetching %s @ %d+%d: %s\n", o.Key, o.Offset, o.Length, err)
			// 	continue
			// }
			// fmt.Printf("Successfully fetched %s @ %d+%d: %s\n", o.Key, o.Offset, o.Length, err)
			// recordChannel <- record
		}
		// recordChannel <- []byte(clientId[0:10])
		// recordChannel <- []byte(clientId[10:20])
		// // time.Sleep(time.Second * 1)
		// recordChannel <- []byte(clientId[20:30])
	}
}

var headers = map[string][]string{
	"Range": []string{""},
}

// headers["Range"] =

func getClientRecord(bucket *s3.Bucket, o MessageLocation) ([]byte, error) {
	headers["Range"][0] = fmt.Sprintf("bytes=%d-%d", o.Offset, o.Offset+o.Length)
	resp, err := bucket.GetResponseWithHeaders(o.Key, headers)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if len(body) != int(o.Length) {
		fmt.Printf("Unexpected body length: %d != %d\n", len(body), o.Length)
	} else {
		fmt.Printf("Fetched record of %d.\n", o.Length)
	}
	return body, err
}

func saveRecords(recordChannel <-chan []byte, done chan<- bool) {
	ok := true
	for ok {
		record, ok := <-recordChannel
		if !ok {
			// Channel is closed
			done <- true
			break
		}

		fmt.Printf("Saving data for %d\n", len(record))
	}
}

func readOffsets(filename string) (*OffsetCache, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// clientId -> locations
	var offsets *OffsetCache
	offsets = NewOffsetCache()

	scanner := bufio.NewScanner(file)
	lineNum := 0
	for scanner.Scan() {
		pieces := strings.Split(scanner.Text(), "\t")
		lineNum++
		if len(pieces) != 4 {
			return nil, fmt.Errorf("Error on line %d: invalid line. Expected 4 values, found %d.", lineNum, len(pieces))
		}
		o, err := makeInt(pieces[2])
		if err != nil {
			return nil, err
		}
		l, err := makeInt(pieces[3])
		if err != nil {
			return nil, err
		}
		offsets.Add(pieces[1], MessageLocation{pieces[0], o, l})
		// offsets = append(offsets, MessageLocation{pieces[0], pieces[1], o, l})
	}
	fmt.Printf("Successfully processed %d offset lines\n", lineNum)
	return offsets, nil
}
