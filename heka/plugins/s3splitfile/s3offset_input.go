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
	"io"
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
	clientids    map[string]struct{}
	metaFileName string
	bucket       *s3.Bucket
	metaBucket   *s3.Bucket
	stop         chan bool
	offsetChan   chan MessageLocation
}

type S3OffsetInputConfig struct {
	// So we can default to using ProtobufDecoder.
	Decoder            string
	Splitter           string
	ClientIdListFile   string `toml:"client_id_list"`
	MetaFile           string `toml:"metadata_file"`
	StartDate          string `toml:"start_date"`
	EndDate            string `toml:"end_date"`
	AWSKey             string `toml:"aws_key"`
	AWSSecretKey       string `toml:"aws_secret_key"`
	AWSRegion          string `toml:"aws_region"`
	S3MetaBucket       string `toml:"s3_meta_bucket"`
	S3MetaBucketPrefix string `toml:"s3_meta_bucket_prefix"`
	S3Bucket           string `toml:"s3_bucket"`
	S3Retries          uint32 `toml:"s3_retries"`
	S3ConnectTimeout   uint32 `toml:"s3_connect_timeout"`
	S3ReadTimeout      uint32 `toml:"s3_read_timeout"`
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
		S3Retries:          5,
		S3ConnectTimeout:   60,
		S3ReadTimeout:      60,
		S3WorkerCount:      16,
	}
}

func (input *S3OffsetInput) Init(config interface{}) (err error) {
	conf := config.(*S3OffsetInputConfig)
	input.S3OffsetInputConfig = conf

	if conf.MetaFile != "" {
		// We already have the required metadata. Don't need to fetch it.
		input.metaFileName = conf.MetaFile
	} else if conf.ClientIdListFile != "" {
		// Load clientids from file.
		input.clientids, err = readLines(conf.ClientIdListFile)
		if err != nil {
			return fmt.Errorf("Error reading file %s for 'client_id_list': %s", conf.ClientIdListFile, err)
		}
	} else {
		return fmt.Errorf("Missing parameter: You must specify either 'client_id_list' or 'metadata_file'")
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
	s.ConnectTimeout = time.Duration(conf.S3ConnectTimeout) * time.Second
	s.ReadTimeout = time.Duration(conf.S3ReadTimeout) * time.Second

	// TODO: ensure we can read from (and list, for meta) the buckets.
	input.bucket = s.Bucket(conf.S3Bucket)

	if conf.S3MetaBucket != "" {
		input.metaBucket = s.Bucket(conf.S3MetaBucket)
	} else if conf.MetaFile == "" {
		return fmt.Errorf("Parameter 's3_meta_bucket' is required unless using 'metadata_file'")
	}

	// Remove any excess path separators from the bucket prefix.
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

	if input.metaFileName != "" {
		wg.Add(1)
		go func() {
			reader, err := os.Open(input.metaFileName)
			if err != nil {
				runner.LogMessage(fmt.Sprintf("Error opening metadata file '%s': %s", input.metaFileName, err))
			}
			defer reader.Close()
			err = input.parseMessageLocations(reader, input.metaFileName)
			if err != nil {
				runner.LogMessage(fmt.Sprintf("Error reading metadata: %s", err))
			}
			// All done with metadata, close the channel
			runner.LogMessage("All done with metadata. Closing channel")
			close(input.offsetChan)
			wg.Done()
		}()
	} else if input.metaBucket != nil {
		wg.Add(1)
		go func() {
			runner.LogMessage("Starting S3 list")
		iteratorLoop:
			for r := range S3Iterator(input.metaBucket, input.S3MetaBucketPrefix, emptySchema) {
				select {
				case <-input.stop:
					runner.LogMessage("Stopping S3 list")
					break iteratorLoop
				default:
				}
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
	} else {
		runner.LogMessage("Nothing to do, no metadata available. Closing channel")
		close(input.offsetChan)
		wg.Done()
	}

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
	return input.parseMessageLocations(reader, result.Key.Key)
}

// Not spec-compliant, but should work well enough for our purposes.
func (input *S3OffsetInput) detectFieldSeparator(line string, expectedCount int) (sep string) {
	possible := [...]string{"\t", ",", "|", " "}
	for _, s := range possible {
		pieces := strings.Split(line, s)
		if len(pieces) == expectedCount {
			return s
		}
	}
	// Don't know... default to tab.
	return possible[0]
}

func (input *S3OffsetInput) parseMessageLocations(reader io.Reader, name string) (err error) {
	lineNum := 0
	// TODO: use "encoding/csv" and set .Comma to the detected separator.
	scanner := bufio.NewScanner(reader)
	delim := ""
	expectedTokens := 4
	for scanner.Scan() {
		if lineNum == 0 {
			delim = input.detectFieldSeparator(scanner.Text(), expectedTokens)
		}
		pieces := strings.Split(scanner.Text(), delim)
		if len(pieces) != expectedTokens {
			return fmt.Errorf("Error on %s line %d: invalid line. Expected %d values, found %d.", name, lineNum, expectedTokens, len(pieces))
		}
		lineNum++

		// Skip optional header.
		if pieces[0] == "file_name" {
			continue
		}

		if input.metaFileName == "" {
			// Check if this client is in our list.
			_, ok := input.clientids[pieces[1]]
			if !ok {
				continue
			}
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
	return scanner.Err()
}

func (input *S3OffsetInput) fetcher(runner pipeline.InputRunner, wg *sync.WaitGroup, workerId uint32) {
	var (
		loc       MessageLocation
		startTime time.Time
		duration  float64
		headers   map[string][]string
		record    []byte
		err       error
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
			atomic.AddInt64(&input.processMessageCount, 1)
			atomic.AddInt64(&input.processMessageBytes, int64(loc.Length))
			for attempt := uint32(1); attempt <= input.S3Retries; attempt++ {
				record, err = getClientRecord(input.bucket, &loc, headers)
				if err != nil {
					runner.LogMessage(fmt.Sprintf("Error #%d fetching %s @ %d+%d: %s\n", attempt, loc.Key, loc.Offset, loc.Length, err))
				} else {
					break
				}
			}
			if err != nil {
				atomic.AddInt64(&input.processMessageFailures, 1)
				continue
			}
			splitterRunner.DeliverRecord(record, deliverer)
			duration = time.Now().UTC().Sub(startTime).Seconds()
			runner.LogMessage(fmt.Sprintf("Successfully fetched %s in %.2fs ", loc.Key, duration))

		case <-input.stop:
			runner.LogMessage("Stopping fetcher...")
			for _ = range input.offsetChan {
				// Drain the channel without processing anything.
			}
			ok = false
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
	if err == nil && len(body) != int(o.Length) {
		err = fmt.Errorf("Unexpected body length: %d != %d\n", len(body), o.Length)
	}
	return body, err
}
