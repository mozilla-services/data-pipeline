package sqs3

import (
    "encoding/json"
    "errors"
    "fmt"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/awserr"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/service/sqs"
    "github.com/mozilla-services/heka/message"
    "github.com/mozilla-services/heka/pipeline"
    "io"
)

type Sqs3Input struct {
    *Sqs3InputConfig
    stop    chan bool
    runner  pipeline.InputRunner
    sqs       *sqs.SQS
    s3        *s3.S3
    queue_url *string
}

type Sqs3InputConfig struct {
    TickerInterval uint   `toml:"ticker_interval"`
    SqsQueue       string `toml:"sqs_queue"`
}

func (input *Sqs3Input) ConfigStruct() interface{} {
    return &Sqs3InputConfig{TickerInterval: uint(5)}
}

func (input *Sqs3Input) Init(config interface{}) error {
    conf := config.(*Sqs3InputConfig)
    input.Sqs3InputConfig = conf
    input.stop = make(chan bool)
    return nil
}

func (input *Sqs3Input) Stop() {
    close(input.stop)
}

func (input *Sqs3Input) packDecorator(pack *pipeline.PipelinePack) {
    field, err := message.NewField("SqsQueue", int(input.SqsQueue), "")
    if err != nil {
        input.runner.LogError(
            fmt.Errorf("can't add 'SqsQueue' field: %s", err.Error()))
    } else {
        pack.Message.AddField(field)
    }
}

func (input *Sqs3Input) Run(runner pipeline.InputRunner,
    helper pipeline.PluginHelper) error {

    // initialize
    input.runner = runner
    input.sqs = sqs.New(session.New())
    input.s3 = s3.New(session.New())
    queue_url, err := get_queue(input.sqs, input.SqsQueue)
    if err != nil { return err }
    input.queue_url = queue_url
    //input.hostname = helper.PipelineConfig().Hostname()
    tickChan := runner.Ticker()
    sRunner := runner.NewSplitterRunner("")
    if !sRunner.UseMsgBytes() {
        sRunner.SetPackDecorator(input.packDecorator)
    }
    defer sRunner.Done()

    for {
        select {
        case <-input.stop:
            return nil
        case <-tickChan:
        }

        receipt_handle, bucket, key, err := receive_from_queue(input.sqs, input.queue_url)
        if err != nil {
            runner.LogError(fmt.Errorf("Error reading queue: %s", err.Error()))
            continue
        }

        o, _, err := get_object(input.s3, bucket, key)
        if err != nil {
            runner.LogError(fmt.Errorf("Error opening file: %s", err.Error()))
            if aws_err := awserr.Error(err); aws_err != nil {
                if aws_err.Code == "NoSuchBucket" or aws_err.Code == "NoSuchKey" {
                    delete_message(input.sqs, input.queue_url, receipt_handle)
                }
            }
            continue
        }
        for err == nil {
            err = sRunner.SplitStream(o, nil)
            if err != io.EOF && err != nil {
                runner.LogError(fmt.Errorf("Error reading file: %s", err.Error()))
            }
        }
        o.Close()
    }
}

func init() {
    pipeline.RegisterPlugin("Sqs3Input", func() interface{} {
        return new(Sqs3Input)
    })
}

// test for internal functions
/*
func Test(queue string) error {
    sqs_con := sqs.New(session.New())
    s3_con := s3.New(session.New())

    for {
        queue_url, err := get_queue(sqs_con, queue)
        if err != nil { return err }

        receipt_handle, bucket, key, err := receive_from_queue(sqs_con, queue_url)
        if err != nil { return err }

        o, err := get_object(s3_con, bucket, key)
        if err != nil { return err }

        err = dump_object(o)
        if err != nil { return err }
        o.Close()

        err = delete_message(sqs_con, queue_url, receipt_handle)
        if err != nil { return err }
    }
}

// only used by Test()
func dump_object(o io.Reader) error {
    buf := make([]byte, 10)
    for {
        _, err := o.Read(buf)
        fmt.Printf("%s", string(buf))
        if err == io.EOF { return nil }
        if err != nil { return err }
    }
}
*/

// helper functions

type SqsBody struct {
    Records []struct {
        EventName string
        S3        struct {
            Bucket struct {
                Name string
            }
            Object struct {
                Key  string
                Size int
            }
        }
    }
}

func get_queue(svc *sqs.SQS, queue string) (*string, error) {
    params := &sqs.GetQueueUrlInput{
        QueueName: aws.String(queue),
    }
    resp, err := svc.GetQueueUrl(params)
    if err != nil { return nil, err }
    return resp.QueueUrl, nil
}

func receive_from_queue(svc *sqs.SQS, queue_url *string) (*string, *string, *string, error) {
    // get sqs message
    params := &sqs.ReceiveMessageInput{
        QueueUrl: queue_url,
        MaxNumberOfMessages: aws.Int64(1),
        VisibilityTimeout: aws.Int64(1),
    }
    resp, err := svc.ReceiveMessage(params)
    if err != nil { return nil, nil, nil, err }

    // error on empty queue
    if len(resp.Messages) == 0 { return nil, nil, nil, errors.New("queue is empty") }

    receipt_handle := resp.Messages[0].ReceiptHandle
    body := resp.Messages[0].Body

    // unmarshal sqs message body
    data := &SqsBody{}
    err = json.Unmarshal([]byte(*body), &data)
    if err != nil { return nil, nil, nil, err }

    bucket := &data.Records[0].S3.Bucket.Name
    key := &data.Records[0].S3.Object.Key
    return receipt_handle, bucket, key, nil
}

func get_object(svc *s3.S3, bucket *string, key *string) (io.ReadCloser, error) {
    params := &s3.GetObjectInput{
        Bucket: bucket,
        Key:    key,
    }
    resp, err := svc.GetObject(params)
    if err != nil { return nil, err }
    return resp.Body, nil
}

func delete_message(svc *sqs.SQS, queue_url *string, receipt_handle *string) error {
    params := &sqs.DeleteMessageInput{
        QueueUrl: queue_url,
        ReceiptHandle: receipt_handle,
    }
    _, err := svc.DeleteMessage(params)
    if err != nil { return err }
    return nil
}
