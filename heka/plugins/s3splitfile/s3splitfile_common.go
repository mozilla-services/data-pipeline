/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
# ***** END LICENSE BLOCK *****/

package s3splitfile

import (
	"encoding/json"
	"fmt"
	"github.com/crowdmob/goamz/s3"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	"io"
	"io/ioutil"
	"math"
	"regexp"
)

type PublishAttempt struct {
	Name              string
	AttemptsRemaining uint32
}

// Encapsulates the directory-splitting schema
type Schema struct {
	Fields       []string
	FieldIndices map[string]int
	Dims         map[string]DimensionChecker
}

// Determine whether a given value is acceptable for a given field, and if not
// return a default value instead.
func (s *Schema) GetValue(field string, value string) (rvalue string, err error) {
	checker, ok := s.Dims[field]
	if !ok {
		return value, fmt.Errorf("No such field: '%s'", field)
	}
	if checker.IsAllowed(value) {
		return value, nil
	} else {
		return "OTHER", nil
	}
}

// Extract all dimensions from the given pack.
func (s *Schema) GetDimensions(pack *PipelinePack) (dimensions []string) {
	dims := make([]string, len(s.Fields))
	for i, _ := range dims {
		dims[i] = "UNKNOWN"
	}

	// TODO: add support for top-level message fields (Timestamp, etc)
	remaining := len(dims)
	for _, field := range pack.Message.Fields {
		if remaining == 0 {
			break
		}

		idx, ok := s.FieldIndices[field.GetName()]
		if ok {
			remaining -= 1
			inValues := field.GetValueString()
			if len(inValues) > 0 {
				// We use the first available value, even if several have been
				// provided.
				v, err := s.GetValue(field.GetName(), inValues[0])
				if err != nil {
					fmt.Printf("How did this happen? %s", err)
				}
				dims[idx] = v
			} // Else there were no values, leave this field as unknown.
		}
	}

	return dims
}

// Interface for calculating whether a particular value is acceptable
// as-is, or if it should be replaced with a default value.
type DimensionChecker interface {
	IsAllowed(v string) bool
}

// Accept any value at all.
type AnyDimensionChecker struct {
}

func (adc AnyDimensionChecker) IsAllowed(v string) bool {
	return true
}

// Accept a specific list of values, anything not in the list
// will not be accepted
type ListDimensionChecker struct {
	// Use a map instead of a list internally for fast lookups.
	allowed map[string]struct{}
}

func (ldc ListDimensionChecker) IsAllowed(v string) bool {
	_, ok := ldc.allowed[v]
	return ok
}

// Factory for creating a ListDimensionChecker using a list instead of a map
func NewListDimensionChecker(allowed []string) *ListDimensionChecker {
	dimMap := map[string]struct{}{}
	for _, a := range allowed {
		dimMap[a] = struct{}{}
	}
	return &ListDimensionChecker{dimMap}
}

// If both are specified, accept any value between `min` and `max` (inclusive).
// If one of the bounds is missing, only enforce the other. If neither bound is
// present, accept all values.
type RangeDimensionChecker struct {
	min string
	max string
}

func (rdc RangeDimensionChecker) IsAllowed(v string) bool {
	// Min and max are optional, so treat them separately.
	// TODO: ensure that Go does string comparisons in the fashion expected
	//       by this code.
	if rdc.min != "" && rdc.min > v {
		return false
	}

	if rdc.max != "" && rdc.max < v {
		return false
	}

	return true
}

// Pattern to use for sanitizing path/file components.
var sanitizePattern = regexp.MustCompile("[^a-zA-Z0-9_/.]")

// Given a string, return a sanitized version that can be used safely as part
// of a filename (for example).
func SanitizeDimension(dim string) (cleaned string) {
	return sanitizePattern.ReplaceAllString(dim, "_")
}

// Load a schema from the given file name.  The file is expected to contain
// valid JSON describing a hierarchy of dimensions, each of which specifies
// what values are "allowed" for that dimension.
// Example schema:
//   {
//     "version": 1,
//     "dimensions": [
//       { "field_name": "submissionDate", "allowed_values": {
//           { "min": "20140120", "max": "20140125" }
//       },
//       { "field_name": "sourceName",     "allowed_values": "*" },
//       { "field_name": "sourceVersion",  "allowed_values": "*" },
//       { "field_name": "reason",         "allowed_values":
//           [ "idle-daily","saved-session" ]
//       },
//       { "field_name": "appName",        "allowed_values":
//           [ "Firefox", "Fennec", "Thunderbird", "FirefoxOS", "B2G" ]
//       },
//       { "field_name": "appUpdateChannel",
//         "allowed_values":
//           [ "default", "nightly", "aurora", "beta", "release", "esr" ]
//       },
//       { "field_name": "appVersion",     "allowed_values": "*" }
//     ]
//   }
func LoadSchema(schemaFileName string) (schema Schema, err error) {
	// Placeholder for parsing JSON
	type JSchemaDimension struct {
		Field_name     string
		Allowed_values interface{}
	}

	// Placeholder for parsing JSON
	type JSchema struct {
		Version    int32
		Dimensions []JSchemaDimension
	}

	schemaBytes, err := ioutil.ReadFile(schemaFileName)
	if err != nil {
		return
	}

	var js JSchema

	err = json.Unmarshal(schemaBytes, &js)
	if err != nil {
		return
	}

	fields := make([]string, len(js.Dimensions))
	fieldIndices := map[string]int{}
	dims := map[string]DimensionChecker{}
	schema = Schema{fields, fieldIndices, dims}

	for i, d := range js.Dimensions {
		schema.Fields[i] = d.Field_name
		schema.FieldIndices[d.Field_name] = i
		switch d.Allowed_values.(type) {
		case string:
			if d.Allowed_values.(string) == "*" {
				schema.Dims[d.Field_name] = AnyDimensionChecker{}
			} else {
				schema.Dims[d.Field_name] = NewListDimensionChecker([]string{d.Allowed_values.(string)})
			}
		case []interface{}:
			allowed := make([]string, len(d.Allowed_values.([]interface{})))
			for i, v := range d.Allowed_values.([]interface{}) {
				allowedValue, ok := v.(string)
				if !ok {
					return schema, fmt.Errorf("Entries in 'allowed_values' for field '%s' must be strings", d.Field_name)
				}
				allowed[i] = allowedValue
			}
			schema.Dims[d.Field_name] = NewListDimensionChecker(allowed)
		case map[string]interface{}:
			vrange := d.Allowed_values.(map[string]interface{})

			vMin, okMin := vrange["min"]
			vMax, okMax := vrange["max"]

			if !okMin && !okMax {
				return schema, fmt.Errorf("Range for field '%s' must have at least one of 'min' or 'max'", d.Field_name)
			}

			ok := false
			minStr := ""
			if okMin {
				minStr, ok = vMin.(string)
				if !ok {
					return schema, fmt.Errorf("Value of 'min' for field '%s' must be a string", d.Field_name)
				}
			}

			maxStr := ""
			if okMax {
				maxStr, ok = vMax.(string)
				if !ok {
					return schema, fmt.Errorf("Value of 'max' for field '%s' must be a string (it was %+v)", d.Field_name, vMax)
				}
			}
			schema.Dims[d.Field_name] = RangeDimensionChecker{minStr, maxStr}
		}
	}
	return
}

var suffixes = [...]string{"", "K", "M", "G", "T", "P"}

// Return a nice, human-readable representation of the given number of bytes.
func PrettySize(bytes int64) string {
	fBytes := float64(bytes)
	sIdx := 0
	for i, _ := range suffixes {
		sIdx = i
		if fBytes < math.Pow(1024.0, float64(sIdx+1)) {
			break
		}
	}

	pretty := fBytes / math.Pow(1024.0, float64(sIdx))
	return fmt.Sprintf("%.2f%sB", pretty, suffixes[sIdx])
}

// Maximum number of S3 List results to fetch at once.
const listBatchSize = 1000

// Maximum number of S3 Record results to queue at once.
const fileBatchSize = 300

// Encapsulates the result of a List operation, allowing detection of errors
// along the way.
type S3ListResult struct {
	Key s3.Key
	Err error
}

// List the contents of the given bucket, sending matching filenames to a
// channel which can be read by the caller.
func S3Iterator(bucket *s3.Bucket, prefix string, schema Schema) <-chan S3ListResult {
	keyChannel := make(chan S3ListResult, listBatchSize)
	go FilterS3(bucket, prefix, 0, schema, keyChannel)
	return keyChannel
}

// Recursively descend into an S3 directory tree, filtering based on the given
// schema, and sending results on the given channel. The `level` parameter
// indicates how far down the tree we are, and is used to determine which schema
// field we use for filtering.
func FilterS3(bucket *s3.Bucket, prefix string, level int, schema Schema, kc chan S3ListResult) {
	// Update the marker as we encounter keys / prefixes. If a response is
	// truncated, the next `List` request will start from the next item after
	// the marker.
	marker := ""

	// Keep listing if the response is incomplete (there are more than
	// `listBatchSize` entries or prefixes)
	done := false
	for !done {
		response, err := bucket.List(prefix, "/", marker, listBatchSize)
		if err != nil {
			fmt.Printf("Error listing: %s\n", err)
			// TODO: retry?
			kc <- S3ListResult{s3.Key{}, err}
		}

		if !response.IsTruncated {
			// Response is not truncated, so we're done.
			done = true
		}

		if level >= len(schema.Fields) {
			// We are past all the dimensions - encountered items are now
			// S3 key names. We ignore any further prefixes and assume that the
			// specified schema is correct/complete.
			for _, k := range response.Contents {
				marker = k.Key
				kc <- S3ListResult{k, nil}
			}
		} else {
			// We are still looking at prefixes. Recursively list each one that
			// matches the specified schema's allowed values.
			for _, pf := range response.CommonPrefixes {
				// Get just the last piece of the prefix to check it as a
				// dimension. If we have '/foo/bar/baz', we just want 'baz'.
				stripped := pf[len(prefix) : len(pf)-1]
				allowed := schema.Dims[schema.Fields[level]].IsAllowed(stripped)
				marker = pf
				if allowed {
					FilterS3(bucket, pf, level+1, schema, kc)
				}
			}
		}
	}

	if level == 0 {
		// We traverse the tree in depth-first order, so once we've reached the
		// end at the root (level 0), we know we're done.
		// Note that things could be made faster by parallelizing the recursive
		// listing, but we would need some other mechanism to know when to close
		// the channel?
		close(kc)
	}
	return
}

// Encapsulates a single record within an S3 file, allowing detection of errors
// along the way.
type S3Record struct {
	BytesRead int
	Record    []byte
	Err       error
}

// List the contents of the given bucket, sending matching filenames to a
// channel which can be read by the caller.
func S3FileIterator(bucket *s3.Bucket, s3Key string) <-chan S3Record {
	recordChannel := make(chan S3Record, fileBatchSize)
	go ReadS3File(bucket, s3Key, recordChannel)
	return recordChannel
}

func makeS3Record(bytesRead int, data []byte, err error) (result S3Record) {
	r := S3Record{}
	r.BytesRead = bytesRead
	r.Err = err
	r.Record = make([]byte, len(data))
	copy(r.Record, data)
	return r
}

// TODO: duplicated from heka-cat
func makeSplitterRunner() (SplitterRunner, error) {
	splitter := &HekaFramingSplitter{}
	config := splitter.ConfigStruct()
	err := splitter.Init(config)
	if err != nil {
		return nil, fmt.Errorf("Error initializing HekaFramingSplitter: %s", err)
	}
	srConfig := CommonSplitterConfig{}
	sRunner := NewSplitterRunner("HekaFramingSplitter", splitter, srConfig)
	return sRunner, nil
}

func ReadS3File(bucket *s3.Bucket, s3Key string, recordChan chan S3Record) {
	defer close(recordChan)
	sRunner, err := makeSplitterRunner()
	if err != nil {
		recordChan <- S3Record{0, []byte{}, err}
		return
	}
	reader, err := bucket.GetReader(s3Key)
	if err != nil {
		recordChan <- S3Record{0, []byte{}, err}
		return
	}
	defer reader.Close()

	var size int64

	done := false
	for !done {
		n, record, err := sRunner.GetRecordFromStream(reader)
		size += int64(n)

		if err != nil {
			if err == io.EOF {
				lenRemaining := len(sRunner.GetRemainingData())
				if lenRemaining > 0 {
					// There was a partial message at the end of the stream.
					// Discard the leftover bytes.
					fmt.Printf("At EOF, len(remaining data) was %d\n", lenRemaining)
				}

				done = true
			} else if err == io.ErrShortBuffer {
				recordChan <- makeS3Record(n, record, fmt.Errorf("record exceeded MAX_RECORD_SIZE %d", message.MAX_RECORD_SIZE))
				continue
			} else {
				// Some other kind of error occurred.
				// TODO: retry? Keep a key->offset counter and start over?
				recordChan <- makeS3Record(n, record, err)
				done = true
				continue
			}

		}
		if len(record) == 0 && !done {
			// This may happen if we did not read enough data to make a full
			// record.
			continue
		}

		recordChan <- makeS3Record(n, record, err)
	}

	return
}
