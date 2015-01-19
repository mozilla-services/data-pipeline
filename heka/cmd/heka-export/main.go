/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Mark Reid (mark@mozilla.com)
# ***** END LICENSE BLOCK *****/

/*

A command-line utility for exporting heka output files to Amazon S3.

*/
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
)

type Progress struct {
	Count int64
	Bytes int64
	Errors int32
}

// Generate a function to use for uploading files as we walk the file tree from
// `base`.
func makeupload(base string, pattern *regexp.Regexp, bucket *s3.Bucket, bucketPrefix string, dryRun bool, progress *Progress) func(string, os.FileInfo, error) error {
	// Remove any excess path separators from the bucket prefix.
	bucketPrefix = strings.Trim(bucketPrefix, "/")

	// Get a canonical version of the base dir
	cleanBase := filepath.Clean(base)

	// Create a closure for the upload function.
	return func(path string, fi os.FileInfo, err error) (errOut error) {
		if err != nil {
			return err
		}

		if fi.IsDir() {
			return nil
		}
		//fmt.Printf("Found an item: %s\n", path)

		baseName := filepath.Base(path)
		if !pattern.MatchString(baseName) {
			//fmt.Printf("Item does not match pattern\n")
			return nil
		}

		// Make sure we're comparing apples to apples when stripping off the
		// base path. Use the canonical versions of both.
		cleanPath := filepath.Clean(path)
		relPath := cleanPath[len(cleanBase)+1:]

		// If we encounter a file
		//    /path/to/base/foo/bar/baz
		//    <--- base ---><-- rel -->
		// and our bucket prefix is `hello/files`, our file in S3 will be at
		//    s3://bucket-name/hello/files/foo/bar/baz
		//                     <- prefix -><-- rel -->
		s3Path := fmt.Sprintf("%s/%s", bucketPrefix, relPath)

		// Update progress Count whether we were successful or not.
		progress.Count += 1

		if dryRun {
			fmt.Printf("Dry run. Not uploading item to %s\n", s3Path)
			return
		}

		fmt.Printf("Uploading item to: %s\n", s3Path)
		reader, err := os.Open(path)
		if err != nil {
			fmt.Printf("Error opening %s for reading: %s\n", path, err)
			progress.Errors++
			return err
		}

		err = bucket.PutReader(s3Path, reader, fi.Size(), "binary/octet-stream", s3.BucketOwnerFull, s3.Options{})
		if err != nil {
			progress.Errors++
			return err
		}

		// Count the bytes for this file as progress if there were
		// no upoad errors.
		progress.Bytes += fi.Size()

		err = reader.Close()
		if err != nil {
			fmt.Printf("Error closing %s: %s\n", path, err)
			progress.Errors++
			errOut = err
		}

		// Now remove the file locally.
		err = os.Remove(path)
		if err != nil {
			fmt.Printf("Error removing %s: %s\n", path, err)
			progress.Errors++
			errOut = err
		}

		return
	}
}


func main() {
    flagBase := flag.String("base-dir", "/", "Base directory in which to look for files to export")
    flagPattern := flag.String("pattern", ".*", "Filenames must match this regular expression to be uploaded")
    flagBucket := flag.String("bucket", "default-bucket", "S3 Bucket name")
    flagBucketPrefix := flag.String("bucket-prefix", "", "S3 Bucket path prefix")
    flagAWSKey := flag.String("aws-key", "DUMMY", "AWS Key")
    flagAWSSecretKey := flag.String("aws-secret-key", "DUMMY", "AWS Secret Key")
    flagAWSRegion := flag.String("aws-region", "us-west-2", "AWS Region")
    flagLoop := flag.Bool("loop", false, "Run in a loop and keep watching for more files to export")
    flagDryRun := flag.Bool("dry-run", false, "Don't actually do anything, just output what would be done")
	flag.Parse()

	if flag.NArg() != 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	var err error
	baseStat, err := os.Stat(*flagBase)
	if err != nil || !baseStat.IsDir() {
		fmt.Printf("base-dir: %s\n", err)
		os.Exit(2)
	}

	pattern, err := regexp.Compile(*flagPattern)
	if err != nil {
		fmt.Printf("pattern: %s\n", err)
		os.Exit(3)
	}

	// fmt.Printf("Base:%s  Pattern:%s  Bucket: s3://%s/%s  AWSKey:%s / %s  Region:%s  Dry Run:%t  Loop:%t\n",
	// 	*flagBase, *flagPattern, *flagBucket, *flagBucketPrefix, *flagAWSKey, *flagAWSSecretKey, *flagAWSRegion, *flagDryRun, *flagLoop)

	var progress Progress
	var rate float64
	var uploadMB float64

	var b *s3.Bucket
	if !*flagDryRun {
		auth := aws.Auth{AccessKey: *flagAWSKey, SecretKey: *flagAWSSecretKey}
		s := s3.New(auth, aws.Regions[*flagAWSRegion])
		b = s.Bucket(*flagBucket)
	} else {
		// b declared and not used :(
		_ = b
	}

	for true {
		progress = Progress{}
		startTime := time.Now().UTC()
		err = filepath.Walk(*flagBase, makeupload(*flagBase, pattern, b, *flagBucketPrefix, *flagDryRun, &progress))
		if err != nil {
			fmt.Printf("Error reading files from %s: %s\n", *flagBase, err)
		}

		if progress.Count > 0 {
			uploadMB = float64(progress.Bytes) / 1024.0 / 1024.0
			duration := time.Now().UTC().Sub(startTime).Seconds()

			if duration > 0 {
				rate = uploadMB / duration
			} else {
				rate = 0
			}
			fmt.Printf("Uploaded %d files containing %.2fMB in %.02fs (%.02fMB/s). Encountered %d errors.\n", progress.Count, uploadMB, duration, rate, progress.Errors)
		} else {
			// We didn't upload any files.
			if !*flagLoop {
				fmt.Println("Nothing to upload")
			} else {
				// Only sleep if we didn't find anything to upload. If we did upload
				// something, we want to try again right away.
				fmt.Println("Waiting for files to upload...")
				time.Sleep(10 * time.Second)
			}
		}

		if !*flagLoop {
			break
		}
	}
}
