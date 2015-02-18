/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
# ***** END LICENSE BLOCK *****/

package s3splitfile

import (
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type S3SplitFileInput struct {
	processFileCount       int64
	processFileFailures    int64
	processMessageCount    int64
	processMessageFailures int64
	processMessageBytes    int64
	remainingDataBytes     int64

	*S3SplitFileInputConfig
	bucket   *s3.Bucket
	schema   Schema
	stop     chan bool
	listChan chan string
}

type S3SplitFileInputConfig struct {
	// So we can default to using ProtobufDecoder.
	Decoder string
	// So we can default to using HekaFramingSplitter.
	Splitter string

	SchemaFile     string `toml:"schema_file"`
	AWSKey         string `toml:"aws_key"`
	AWSSecretKey   string `toml:"aws_secret_key"`
	AWSRegion      string `toml:"aws_region"`
	S3Bucket       string `toml:"s3_bucket"`
	S3BucketPrefix string `toml:"s3_bucket_prefix"`
	S3Retries      uint32 `toml:"s3_retries"`
	S3WorkerCount  uint32 `toml:"s3_worker_count"`
}

func (input *S3SplitFileInput) ConfigStruct() interface{} {
	return &S3SplitFileInputConfig{
		Decoder:        "ProtobufDecoder",
		Splitter:       "HekaFramingSplitter",
		AWSKey:         "",
		AWSSecretKey:   "",
		AWSRegion:      "us-west-2",
		S3Bucket:       "",
		S3BucketPrefix: "",
		S3Retries:      5,
		S3WorkerCount:  10,
	}
}

func (input *S3SplitFileInput) Init(config interface{}) (err error) {
	conf := config.(*S3SplitFileInputConfig)
	input.S3SplitFileInputConfig = conf

	input.schema, err = LoadSchema(conf.SchemaFile)
	if err != nil {
		return fmt.Errorf("Parameter 'schema_file' must be a valid JSON file: %s", err)
	}

	if conf.S3Bucket != "" {
		auth, err := aws.GetAuth(conf.AWSKey, conf.AWSSecretKey, "", time.Now())
		if err != nil {
			return fmt.Errorf("Authentication error: %s\n", err)
		}
		region, ok := aws.Regions[conf.AWSRegion]
		if !ok {
			return fmt.Errorf("Parameter 'aws_region' must be a valid AWS Region")
		}
		s := s3.New(auth, region)
		// TODO: ensure we can read from the bucket.
		input.bucket = s.Bucket(conf.S3Bucket)
	} else {
		input.bucket = nil
	}

	// Remove any excess path separators from the bucket prefix.
	conf.S3BucketPrefix = strings.Trim(conf.S3BucketPrefix, "/")
	if conf.S3BucketPrefix != "" {
		conf.S3BucketPrefix += "/"
	}

	input.stop = make(chan bool)
	input.listChan = make(chan string, 1000)

	return nil
}

func (input *S3SplitFileInput) Stop() {
	close(input.stop)
}

func (input *S3SplitFileInput) Run(runner pipeline.InputRunner, helper pipeline.PluginHelper) error {
	// Begin listing the files (either straight from S3 or from a cache)
	// Write matching filenames on a "lister" channel
	// Read from the lister channel:
	//   - fetch the filename
	//   - read records from it
	//   - write them to a "reader" channel

	var (
		wg sync.WaitGroup
		i  uint32
	)

	wg.Add(1)
	go func() {
		runner.LogMessage("Starting S3 list")
		for r := range S3Iterator(input.bucket, input.S3BucketPrefix, input.schema) {
			if r.Err != nil {
				runner.LogError(fmt.Errorf("Error getting S3 list: %s", r.Err))
			} else {
				runner.LogMessage(fmt.Sprintf("Found: %s", r.Key.Key))
				input.listChan <- r.Key.Key
			}
		}
		// All done listing, close the channel
		runner.LogMessage("All done listing. Closing channel")
		close(input.listChan)
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

// TODO: handle "no such file"
func (input *S3SplitFileInput) readS3File(runner pipeline.InputRunner, d *pipeline.Deliverer, sr *pipeline.SplitterRunner, s3Key string) (err error) {
	runner.LogMessage(fmt.Sprintf("Preparing to read: %s", s3Key))
	if input.bucket == nil {
		runner.LogMessage(fmt.Sprintf("Dude, where's my bucket: %s", s3Key))
		return
	}
	for r := range S3FileIterator(input.bucket, s3Key) {
		record := r.Record
		err := r.Err

		if err != nil && err != io.EOF {
			runner.LogError(fmt.Errorf("Error reading %s: %s", s3Key, err))
			atomic.AddInt64(&input.processMessageFailures, 1)
			return err
		}
		if len(record) > 0 {
			atomic.AddInt64(&input.processMessageCount, 1)
			atomic.AddInt64(&input.processMessageBytes, int64(len(record)))
			(*sr).DeliverRecord(record, *d)
		}
	}

	return
}

func (input *S3SplitFileInput) fetcher(runner pipeline.InputRunner, wg *sync.WaitGroup, workerId uint32) {
	var (
		s3Key     string
		startTime time.Time
		duration  float64
	)

	fetcherName := fmt.Sprintf("S3Reader%d", workerId)
	deliverer := runner.NewDeliverer(fetcherName)
	defer deliverer.Done()
	splitterRunner := runner.NewSplitterRunner(fetcherName)

	ok := true
	for ok {
		select {
		case s3Key, ok = <-input.listChan:
			if !ok {
				// Channel is closed => we're shutting down, exit cleanly.
				// runner.LogMessage("Fetcher all done! shutting down.")
				break
			}

			startTime = time.Now().UTC()
			err := input.readS3File(runner, &deliverer, &splitterRunner, s3Key)
			atomic.AddInt64(&input.processFileCount, 1)
			leftovers := splitterRunner.GetRemainingData()
			lenLeftovers := len(leftovers)
			if lenLeftovers > 0 {
				atomic.AddInt64(&input.remainingDataBytes, int64(lenLeftovers))
				runner.LogError(fmt.Errorf("Trailing data, possible corruption: %d bytes left in stream at EOF: %s", lenLeftovers, s3Key))
			}
			if err != nil && err != io.EOF {
				runner.LogError(fmt.Errorf("Error reading %s: %s", s3Key, err))
				atomic.AddInt64(&input.processFileFailures, 1)
				continue
			}
			duration = time.Now().UTC().Sub(startTime).Seconds()
			runner.LogMessage(fmt.Sprintf("Successfully fetched %s in %.2fs ", s3Key, duration))
		}
	}

	wg.Done()
}

func (input *S3SplitFileInput) ReportMsg(msg *message.Message) error {
	message.NewInt64Field(msg, "ProcessFileCount", atomic.LoadInt64(&input.processFileCount), "count")
	message.NewInt64Field(msg, "ProcessFileFailures", atomic.LoadInt64(&input.processFileFailures), "count")
	message.NewInt64Field(msg, "ProcessMessageCount", atomic.LoadInt64(&input.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageFailures", atomic.LoadInt64(&input.processMessageFailures), "count")
	message.NewInt64Field(msg, "ProcessMessageBytes", atomic.LoadInt64(&input.processMessageBytes), "B")
	message.NewInt64Field(msg, "RemainingDataBytes", atomic.LoadInt64(&input.remainingDataBytes), "B")

	return nil
}

func init() {
	pipeline.RegisterPlugin("S3SplitFileInput", func() interface{} {
		return new(S3SplitFileInput)
	})
}
