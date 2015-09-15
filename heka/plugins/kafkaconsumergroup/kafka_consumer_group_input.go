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
#   Mike Trinkala (trink@mozilla.com)
#   Rob Miller (rmiller@mozilla.com)
#   Wesley Dawson (whd@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

package kafkaconsumergroup

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

type KafkaConsumerGroupInputConfig struct {
	Splitter string

	// Client Config
	MetadataRetries            int    `toml:"metadata_retries"`
	WaitForElection            uint32 `toml:"wait_for_election"`
	BackgroundRefreshFrequency uint32 `toml:"background_refresh_frequency"`

	// Broker Config
	MaxOpenRequests int    `toml:"max_open_reqests"`
	DialTimeout     uint32 `toml:"dial_timeout"`
	ReadTimeout     uint32 `toml:"read_timeout"`
	WriteTimeout    uint32 `toml:"write_timeout"`

	// Consumer Config
	Partition                 int32
	Group                     string
	DefaultFetchSize          int32    `toml:"default_fetch_size"`
	MinFetchSize              int32    `toml:"min_fetch_size"`
	MaxMessageSize            int32    `toml:"max_message_size"`
	MaxWaitTime               uint32   `toml:"max_wait_time"`
	ConsumerGroup             string   `toml:"consumer_group"`
	Topics                    []string `toml:"topics"`
	ZookeeperConnectionString string   `toml:"zookeeper_connection_string"`
	OffsetMethod              string   `toml:"offset_method"` // Newest, Oldest
	EventBufferSize           int      `toml:"event_buffer_size"`
	LogSarama                 bool     `toml:"log_sarama"`
}

type KafkaConsumerGroupInput struct {
	processMessageCount    int64
	processMessageFailures int64

	config         *KafkaConsumerGroupInputConfig
	consumerConfig *consumergroup.Config
	client         *sarama.Client
	consumer       *consumergroup.ConsumerGroup
	pConfig        *pipeline.PipelineConfig
	ir             pipeline.InputRunner
	stopChan       chan bool
	name           string
}

func (k *KafkaConsumerGroupInput) ConfigStruct() interface{} {
	return &KafkaConsumerGroupInputConfig{
		Splitter:                   "NullSplitter",
		MetadataRetries:            3,
		WaitForElection:            250,
		BackgroundRefreshFrequency: 10 * 60 * 1000,
		MaxOpenRequests:            4,
		DialTimeout:                60 * 1000,
		ReadTimeout:                60 * 1000,
		WriteTimeout:               60 * 1000,
		DefaultFetchSize:           1024 * 32,
		MinFetchSize:               1,
		MaxWaitTime:                250,
		OffsetMethod:               "Oldest",
		EventBufferSize:            16,
		LogSarama:                  false,
	}
}

func (k *KafkaConsumerGroupInput) SetPipelineConfig(pConfig *pipeline.PipelineConfig) {
	k.pConfig = pConfig
}

func (k *KafkaConsumerGroupInput) SetName(name string) {
	k.name = name
}

func (k *KafkaConsumerGroupInput) Init(config interface{}) (err error) {
	k.config = config.(*KafkaConsumerGroupInputConfig)
	if len(k.config.ConsumerGroup) == 0 {
		return fmt.Errorf("consumer_group required")
	}
	if len(k.config.Topics) == 0 {
		return fmt.Errorf("topics required")
	}
	if len(k.config.ZookeeperConnectionString) == 0 {
		return fmt.Errorf("zookeeper_connection_string required")
	}

	// FIXME heka's logging infrastructure can probably be used for this
	// contains useful information for debugging consumer group partition
	// changes
	if k.config.LogSarama {
		sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	}

	k.consumerConfig = consumergroup.NewConfig()
	switch k.config.OffsetMethod {
	case "Newest":
		k.consumerConfig.Offsets.Initial = sarama.OffsetNewest
	case "Oldest":
		k.consumerConfig.Offsets.Initial = sarama.OffsetOldest
	default:
		return fmt.Errorf("invalid offset_method: %s", k.config.OffsetMethod)
	}

	k.consumerConfig.Offsets.ProcessingTimeout = 10 * time.Second

	k.consumerConfig.Config.Metadata.Retry.Max = k.config.MetadataRetries
	k.consumerConfig.Config.Metadata.Retry.Backoff = time.Duration(k.config.WaitForElection) * time.Millisecond
	k.consumerConfig.Config.Metadata.RefreshFrequency = time.Duration(k.config.BackgroundRefreshFrequency) * time.Millisecond

	k.consumerConfig.Config.Net.MaxOpenRequests = k.config.MaxOpenRequests
	k.consumerConfig.Config.Net.DialTimeout = time.Duration(k.config.DialTimeout) * time.Millisecond
	k.consumerConfig.Config.Net.ReadTimeout = time.Duration(k.config.ReadTimeout) * time.Millisecond
	k.consumerConfig.Config.Net.WriteTimeout = time.Duration(k.config.WriteTimeout) * time.Millisecond

	k.consumerConfig.Config.Consumer.Fetch.Default = k.config.DefaultFetchSize
	k.consumerConfig.Config.Consumer.Fetch.Min = k.config.MinFetchSize
	k.consumerConfig.Config.Consumer.Fetch.Max = k.config.MaxMessageSize
	k.consumerConfig.Config.Consumer.MaxWaitTime = time.Duration(k.config.MaxWaitTime) * time.Millisecond
	k.consumerConfig.Config.ChannelBufferSize = k.config.EventBufferSize

	var zookeeperNodes []string
	zookeeperNodes, k.consumerConfig.Zookeeper.Chroot = kazoo.ParseConnectionString(k.config.ZookeeperConnectionString)
	if len(zookeeperNodes) == 0 {
		return fmt.Errorf("unable to parse zookeeper_connection_string")
	}

	consumer, err := consumergroup.JoinConsumerGroup(k.config.ConsumerGroup, k.config.Topics, zookeeperNodes, k.consumerConfig)
	if err != nil {
		return
	}
	k.consumer = consumer
	k.stopChan = make(chan bool)
	return
}

func (k *KafkaConsumerGroupInput) addField(pack *pipeline.PipelinePack, name string,
	value interface{}, representation string) {

	if field, err := message.NewField(name, value, representation); err == nil {
		pack.Message.AddField(field)
	} else {
		k.ir.LogError(fmt.Errorf("can't add '%s' field: %s", name, err.Error()))
	}
}

func (k *KafkaConsumerGroupInput) Run(ir pipeline.InputRunner, h pipeline.PluginHelper) (err error) {
	sRunner := ir.NewSplitterRunner("")

	defer func() {
		if err := k.consumer.Close(); err != nil {
			k.ir.LogError(fmt.Errorf("error closing the consumer: %s", err.Error()))
		}
		sRunner.Done()
	}()
	k.ir = ir

	go func() {
		for err := range k.consumer.Errors() {
			// this isn't necessarily a process message failure, as the failure
			// channel is async
			// atomic.AddInt64(&k.processMessageFailures, 1)
			ir.LogError(err)
		}
	}()

	var (
		hostname = k.pConfig.Hostname()
		event    *sarama.ConsumerMessage
		ok       bool
		n        int
	)

	packDec := func(pack *pipeline.PipelinePack) {
		pack.Message.SetType("heka.kafka")
		pack.Message.SetLogger(k.name)
		pack.Message.SetHostname(hostname)
		k.addField(pack, "Key", event.Key, "")
		k.addField(pack, "Topic", event.Topic, "")
		k.addField(pack, "Partition", event.Partition, "")
		k.addField(pack, "Offset", event.Offset, "")
	}
	if !sRunner.UseMsgBytes() {
		sRunner.SetPackDecorator(packDec)
	}

	offsets := make(map[string]map[int32]int64)
	for {
		select {
		case event, ok = <-k.consumer.Messages():
			if !ok {
				return
			}

			if offsets[event.Topic] == nil {
				offsets[event.Topic] = make(map[int32]int64)
			}

			if offsets[event.Topic][event.Partition] != 0 && offsets[event.Topic][event.Partition] != event.Offset-1 {
				ir.LogError(fmt.Errorf("unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n",
					event.Topic, event.Partition,
					offsets[event.Topic][event.Partition]+1, event.Offset,
					event.Offset-offsets[event.Topic][event.Partition]+1))
			}

			atomic.AddInt64(&k.processMessageCount, 1)

			if n, err = sRunner.SplitBytes(event.Value, nil); err != nil {
				ir.LogError(fmt.Errorf("processing message from topic %s: %s",
					event.Topic, err))
			}
			if n > 0 && n != len(event.Value) {
				ir.LogError(fmt.Errorf("extra data dropped in message from topic %s",
					event.Topic))
			}

			offsets[event.Topic][event.Partition] = event.Offset
			k.consumer.CommitUpto(event)
		case <-k.stopChan:
			return
		}
	}
}

func (k *KafkaConsumerGroupInput) Stop() {
	close(k.stopChan)
}

func (k *KafkaConsumerGroupInput) ReportMsg(msg *message.Message) error {
	message.NewInt64Field(msg, "ProcessMessageCount",
		atomic.LoadInt64(&k.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageFailures",
		atomic.LoadInt64(&k.processMessageFailures), "count")
	return nil
}

func (k *KafkaConsumerGroupInput) CleanupForRestart() {
	return
}

func init() {
	pipeline.RegisterPlugin("KafkaConsumerGroupInput", func() interface{} {
		return new(KafkaConsumerGroupInput)
	})
}
