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
	"time"
)

type S3SplitFileInput struct {
	*S3SplitFileInputConfig
	decoderChan chan *pipeline.PipelinePack
	bucket      *s3.Bucket
	schema      Schema
	stop        chan bool
	helper      pipeline.PluginHelper
	listChan    chan string
}

type S3SplitFileInputConfig struct {
	TickerInterval uint   `toml:"ticker_interval"`
	DecoderName    string `toml:"decoder"`
	// Type of parser used to break the stream up into messages
	ParserType string `toml:"parser_type"`

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
		TickerInterval: uint(5),
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
		auth := aws.Auth{AccessKey: conf.AWSKey, SecretKey: conf.AWSSecretKey}
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

	input.helper = helper

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

	// Run a pool of concurrent publishers.
	for i = 0; i < input.S3WorkerCount; i++ {
		wg.Add(1)
		go input.fetcher(runner, &wg)
	}
	wg.Wait()

	return nil
}

func (input *S3SplitFileInput) readS3File(runner pipeline.InputRunner, s3Key string) (size int64, recordCount int64, err error) {
	var (
		pack *pipeline.PipelinePack
		dr   pipeline.DecoderRunner
		ok   bool
	)

	runner.LogMessage(fmt.Sprintf("Preparing to read: %s", s3Key))

	parser := pipeline.NewMessageProtoParser()

	dr, ok = input.helper.DecoderRunner(input.DecoderName,
		fmt.Sprintf("%s-%s", runner.Name(), input.DecoderName))
	if !ok {
		runner.LogError(fmt.Errorf("Error getting decoder: %s", input.DecoderName))
		return
	}

	runner.LogMessage("Got a decoder")

	if input.bucket == nil {
		runner.LogMessage(fmt.Sprintf("Dude, where's my bucket: %s", s3Key))
		return
	}

	runner.LogMessage("Getting a reader")
	rc, err := input.bucket.GetReader(s3Key)
	if err != nil {
		runner.LogError(fmt.Errorf("Error getting a reader: %s", err))
	}
	runner.LogMessage("Got a reader")
	defer rc.Close()

	packSupply := runner.InChan()

	done := false
	for !done {
		runner.LogMessage(fmt.Sprintf("Reading message %d from %s", recordCount, s3Key))
		n, record, err := parser.Parse(rc)
		size += int64(n)

		if err != nil {
			runner.LogError(fmt.Errorf("Error reading S3: %s", err))
			if err == io.EOF {
				runner.LogMessage(fmt.Sprintf("Success: Reached EOF in %s at offset: %d, n=%d, len(record)=%d", s3Key, size, n, len(record)))
				if len(record) == 0 {
					runner.LogMessage("At EOF, record was empty.")
					record = parser.GetRemainingData()
					runner.LogMessage(fmt.Sprintf("At EOF, RemainingData was %d", len(record)))
					// We already counted part of this record (n) when we hit
					// EOF. We subtract it here because GetRemainingData()
					// contains the entire final record (including the partial
					// read at EOF).
					//size += int64(len(record) - n)
				} else {
					runner.LogMessage(fmt.Sprintf("At EOF, record was not empty, len=%d.", len(record)))
				}
				done = true
			} else if err == io.ErrShortBuffer {
				runner.LogError(fmt.Errorf("record exceeded MAX_RECORD_SIZE %d", message.MAX_RECORD_SIZE))
				err = nil // non-fatal
				continue
			} else {
				// TODO: retry? Keep a key->offset counter and start over?
				runner.LogError(fmt.Errorf("Bad Error reading S3: %s", err))
				return size, recordCount, err
			}

		}
		if n > 0 && n != len(record) && !done {
			// This is not corruption if it happens in the final record (ie.
			// when done == true). In the case of the last record, we may have
			// already read a partial record earlier, and had to use
			// GetRemainingData() to fetch the whole thing.
			runner.LogMessage(fmt.Sprintf("Corruption detected in %s at offset: %d bytes: %d. n=%d, len(record)=%d\n", s3Key, size, n-len(record), n, len(record)))
		}
		// if len(record) == 0 && !done {
		if len(record) == 0 {
			// Why does this happen before EOF?
			// This happens if we read some data, but not enough to create a full record.
			runner.LogMessage(fmt.Sprintf("zero-length record in %s at offset: %d n=%d\n", s3Key, size, n))
			continue
		}

		recordCount += 1
		runner.LogMessage(fmt.Sprintf("%s: Message %d at offset: %d, length: %d\n", s3Key, recordCount, size-int64(len(record)), len(record)))
		pack = <-packSupply
		headerLen := int(record[1]) + message.HEADER_FRAMING_SIZE
		messageLen := len(record) - headerLen
		// TODO: signed messages?
		if messageLen > cap(pack.MsgBytes) {
			pack.MsgBytes = make([]byte, messageLen)
		}
		pack.MsgBytes = pack.MsgBytes[:messageLen]
		copy(pack.MsgBytes, record[headerLen:])

		// TODO: support a `matcher`?
		// if err = proto.Unmarshal(record[headerLen:], msg); err != nil {
		// 	runner.LogError(fmt.Errorf("Error unmarshalling message in '%s' at offset: %d error: %s\n", s3Key, size, err))
		// 	pack.Recycle()
		// 	continue
		// }

		// if !match.Match(msg) {
		// 	continue
		// }
		// matched += 1

		dr.InChan() <- pack
	}

	return
}

func (input *S3SplitFileInput) fetcher(runner pipeline.InputRunner, wg *sync.WaitGroup) {
	var (
		s3Key        string
		startTime    time.Time
		duration     float64
		downloadMB   float64
		downloadRate float64
	)

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
			size, count, err := input.readS3File(runner, s3Key)
			if err != nil {
				runner.LogError(fmt.Errorf("Error reading %s: %s", s3Key, err))
				continue
			}
			duration = time.Now().UTC().Sub(startTime).Seconds()

			downloadMB = float64(size) / 1024.0 / 1024.0
			if duration > 0 {
				downloadRate = downloadMB / duration
			} else {
				downloadRate = 0
			}

			runner.LogMessage(fmt.Sprintf("Successfully fetched %d records, %dB, %.2fMB in %.2fs (%.2fMB/s): %s", count, size, downloadMB, duration, downloadRate, s3Key))
		}
	}

	wg.Done()
}

func init() {
	pipeline.RegisterPlugin("S3SplitFileInput", func() interface{} {
		return new(S3SplitFileInput)
	})
}
