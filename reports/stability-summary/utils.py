import boto3
from gzip import GzipFile
from cStringIO import StringIO
import sys
import csv

class S3CompressedWriter(object):
    def __init__(self, bucket, path, mimetype='text/plain'):
        self.bucket = bucket
        self.path = path
        self.mimetype = mimetype
        self._buffer = None

    def __enter__(self):
        self._buffer = StringIO();
        self._writer = GzipFile(mode="wb", fileobj=self._buffer)
        return self._writer

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value is None:
            self._writer.close()
            self._buffer.seek(0)
            s3 = boto3.resource('s3')
            s3.Object(self.bucket, self.path).put(Body=self._buffer, ContentEncoding='gzip', ContentType=self.mimetype)
        self._buffer = None

    def __del__(self):
        assert self._buffer is None

def S3CompressedReader(bucket, path):
    s3 = boto3.resource('s3')
    r = s3.Object(bucket, path).get()
    body = StringIO(r['Body'].read())
    return GzipFile(mode="rb", fileobj=body)

def HeaderCSVReader(fd, *args, **kwargs):
    """
    Read CSV data from `fd`, separating the header list from the data.
    """
    reader = csv.reader(fd, *args, **kwargs)
    header = reader.next()
    return header, reader
