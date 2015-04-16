/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
# ***** END LICENSE BLOCK *****/

package s3splitfile

import (
	"bufio"
	"fmt"
	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type MessageLocation struct {
	Key    string
	Offset uint32
	Length uint32
}

type S3OffsetInput struct {
	processMessageCount    int64
	processMessageFailures int64
	processMessageBytes    int64

	*S3OffsetInputConfig
	clientids  map[string]struct{}
	bucket     *s3.Bucket
	metaBucket *s3.Bucket
	stop       chan bool
	offsetChan chan MessageLocation
}

type S3OffsetInputConfig struct {
	// So we can default to using ProtobufDecoder.
	Decoder            string
	Splitter           string
	ClientIdListFile   string `toml:"client_id_list"`
	StartDate          string `toml:"start_date"`
	EndDate            string `toml:"end_date"`
	AWSKey             string `toml:"aws_key"`
	AWSSecretKey       string `toml:"aws_secret_key"`
	AWSRegion          string `toml:"aws_region"`
	S3MetaBucket       string `toml:"s3_meta_bucket"`
	S3MetaBucketPrefix string `toml:"s3_meta_bucket_prefix"`
	S3Bucket           string `toml:"s3_bucket"`
	S3BucketPrefix     string `toml:"s3_bucket_prefix"`
	S3Retries          uint32 `toml:"s3_retries"`
	S3WorkerCount      uint32 `toml:"s3_worker_count"`
}

func (input *S3OffsetInput) ConfigStruct() interface{} {
	return &S3OffsetInputConfig{
		Decoder:            "ProtobufDecoder",
		Splitter:           "NullSplitter",
		StartDate:          "20150101",
		EndDate:            time.Now().UTC().Format("20060102"),
		AWSKey:             "",
		AWSSecretKey:       "",
		AWSRegion:          "us-west-2",
		S3MetaBucket:       "",
		S3MetaBucketPrefix: "",
		S3Bucket:           "",
		S3BucketPrefix:     "",
		S3Retries:          5,
		S3WorkerCount:      16,
	}
}

func (input *S3OffsetInput) Init(config interface{}) (err error) {
	conf := config.(*S3OffsetInputConfig)
	input.S3OffsetInputConfig = conf

	// Load clientids from file.
	input.clientids, err = readLines(conf.ClientIdListFile)
	if err != nil {
		return fmt.Errorf("Error reading file %s for 'client_id_list': %s", conf.ClientIdListFile, err)
	}

	auth, err := aws.GetAuth(conf.AWSKey, conf.AWSSecretKey, "", time.Now())
	if err != nil {
		return fmt.Errorf("Authentication error: %s\n", err)
	}
	region, ok := aws.Regions[conf.AWSRegion]
	if !ok {
		return fmt.Errorf("Parameter 'aws_region' must be a valid AWS Region")
	}
	s := s3.New(auth, region)
	// TODO: ensure we can read from (and list, for meta) the buckets.
	input.bucket = s.Bucket(conf.S3Bucket)
	input.metaBucket = s.Bucket(conf.S3MetaBucket)

	// Remove any excess path separators from the bucket prefix.
	conf.S3BucketPrefix = CleanBucketPrefix(conf.S3BucketPrefix)
	conf.S3MetaBucketPrefix = CleanBucketPrefix(conf.S3MetaBucketPrefix)

	input.stop = make(chan bool)
	input.offsetChan = make(chan MessageLocation, 1000)

	return nil
}

func (input *S3OffsetInput) Stop() {
	close(input.stop)
}

func (input *S3OffsetInput) Run(runner pipeline.InputRunner, helper pipeline.PluginHelper) error {
	// List offset metadata index files
	// For each index D >= start and <= end
	//   Read index D
	//   Write offsets for any desired clients to offsetChan
	// Meanwhile, for each item in offsetChan
	//   Go fetch that record, inject resulting message into pipeline.

	var (
		wg          sync.WaitGroup
		i           uint32
		emptySchema Schema
	)

	wg.Add(1)
	go func() {
		runner.LogMessage("Starting S3 list")
		for r := range S3Iterator(input.metaBucket, input.S3MetaBucketPrefix, emptySchema) {
			if r.Err != nil {
				runner.LogError(fmt.Errorf("Error getting S3 list: %s", r.Err))
			} else {
				base := path.Base(r.Key.Key)[0:8]
				// Check if r is in the desired date range.
				if base >= input.StartDate && base <= input.EndDate {
					err := input.grep(r)
					if err != nil {
						runner.LogMessage(fmt.Sprintf("Error reading index: %s", err))
					}
				}
			}
		}
		// All done listing, close the channel
		runner.LogMessage("All done listing. Closing channel")
		close(input.offsetChan)
		wg.Done()
	}()

	// Run a pool of concurrent readers.
	for i = 0; i < input.S3WorkerCount; i++ {
		wg.Add(1)
		go input.fetcher(runner, &wg, i)
	}
	wg.Wait()

	return nil
}

func (input *S3OffsetInput) grep(result S3ListResult) (err error) {
	// Read the file from S3, grep for desired clients.
	// It appears that goamz helpfully gunzips the content for you if the
	// correct headers are set.
	reader, err := input.metaBucket.GetReader(result.Key.Key)
	if err != nil {
		return err
	}
	defer reader.Close()

	lineNum := 0
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		pieces := strings.Split(scanner.Text(), "\t")
		lineNum++
		if len(pieces) != 4 {
			return fmt.Errorf("Error on %s line %d: invalid line. Expected 4 values, found %d.", result.Key.Key, lineNum, len(pieces))
		}

		// Check if this client is in our list.
		_, ok := input.clientids[pieces[1]]
		if !ok {
			continue
		}
		o, err := makeInt(pieces[2])
		if err != nil {
			return err
		}
		l, err := makeInt(pieces[3])
		if err != nil {
			return err
		}
		input.offsetChan <- MessageLocation{pieces[0], o, l}
	}

	fmt.Printf("Read %d lines from %s\n", lineNum, result.Key.Key)
	return scanner.Err()
}

func (input *S3OffsetInput) fetcher(runner pipeline.InputRunner, wg *sync.WaitGroup, workerId uint32) {
	var (
		loc       MessageLocation
		startTime time.Time
		duration  float64
		headers   map[string][]string
	)

	headers = map[string][]string{
		"Range": []string{""},
	}

	fetcherName := fmt.Sprintf("S3Reader%d", workerId)
	deliverer := runner.NewDeliverer(fetcherName)
	defer deliverer.Done()
	splitterRunner := runner.NewSplitterRunner(fetcherName)

	ok := true
	for ok {
		select {
		case loc, ok = <-input.offsetChan:
			if !ok {
				// Channel is closed => we're shutting down, exit cleanly.
				runner.LogMessage("Fetcher all done! shutting down.")
				break
			}

			startTime = time.Now().UTC()

			// Read one message from the given location
			headers["Range"][0] = fmt.Sprintf("bytes=%d-%d", loc.Offset, loc.Offset+loc.Length-1)
			record, err := getClientRecord(input.bucket, &loc, headers)
			if err != nil {
				runner.LogMessage(fmt.Sprintf("Error fetching %s @ %d+%d: %s\n", loc.Key, loc.Offset, loc.Length, err))
				continue
			}
			splitterRunner.DeliverRecord(record, deliverer)
			duration = time.Now().UTC().Sub(startTime).Seconds()
			runner.LogMessage(fmt.Sprintf("Successfully fetched %s in %.2fs ", loc.Key, duration))
		}
	}

	wg.Done()
}

func (input *S3OffsetInput) ReportMsg(msg *message.Message) error {
	message.NewInt64Field(msg, "ProcessMessageCount", atomic.LoadInt64(&input.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageFailures", atomic.LoadInt64(&input.processMessageFailures), "count")
	message.NewInt64Field(msg, "ProcessMessageBytes", atomic.LoadInt64(&input.processMessageBytes), "B")

	return nil
}

// Read all lines from specified file into an array.
func readLines(path string) (map[string]struct{}, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	lines := map[string]struct{}{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines[scanner.Text()] = struct{}{}
	}
	return lines, scanner.Err()
}

// Parse a string as a uint32 value.
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

func init() {
	pipeline.RegisterPlugin("S3OffsetInput", func() interface{} {
		return new(S3OffsetInput)
	})
}

// Read a single client record using a partial read from S3 using the given
// headers, which should contain a "Range: bytes=M-N" header.
func getClientRecord(bucket *s3.Bucket, o *MessageLocation, headers map[string][]string) ([]byte, error) {
	resp, err := bucket.GetResponseWithHeaders(o.Key, headers)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if len(body) != int(o.Length) {
		fmt.Printf("Unexpected body length: %d != %d\n", len(body), o.Length)
	}
	return body, err
}
