/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014-2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Mike Trinkala (trink@mozilla.com)
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

package kafkaconsumergroup

import (
	"testing"

	. "github.com/mozilla-services/heka/pipeline"
)

func TestEmptyZookeeperConnectionString(t *testing.T) {
	pConfig := NewPipelineConfig(nil)
	ki := new(KafkaConsumerGroupInput)
	ki.SetPipelineConfig(pConfig)
	config := ki.ConfigStruct().(*KafkaConsumerGroupInputConfig)
	config.ConsumerGroup = "test"
	config.Topics = []string{"test"}
	err := ki.Init(config)

	errmsg := "zookeeper_connection_string required"
	if err.Error() != errmsg {
		t.Errorf("Expected: %s, received: %s", errmsg, err)
	}
}

func TestBadZookeeperConnectionString(t *testing.T) {
	pConfig := NewPipelineConfig(nil)
	ki := new(KafkaConsumerGroupInput)
	ki.SetPipelineConfig(pConfig)
	config := ki.ConfigStruct().(*KafkaConsumerGroupInputConfig)
	config.ConsumerGroup = "test"
	config.Topics = []string{"test"}
	config.ZookeeperConnectionString = "::"
	err := ki.Init(config)

	errmsg := "zk: could not connect to a server"
	if err.Error() != errmsg {
		t.Errorf("Expected: %s, received: %s", errmsg, err)
	}
}

func TestInvalidOffsetMethod(t *testing.T) {
	pConfig := NewPipelineConfig(nil)
	ki := new(KafkaConsumerGroupInput)
	ki.SetName("test")
	ki.SetPipelineConfig(pConfig)

	config := ki.ConfigStruct().(*KafkaConsumerGroupInputConfig)
	config.ConsumerGroup = "test"
	config.Topics = []string{"test"}
	config.ZookeeperConnectionString = "localhost:2181"
	config.OffsetMethod = "last"
	err := ki.Init(config)

	errmsg := "invalid offset_method: last"
	if err.Error() != errmsg {
		t.Errorf("Expected: %s, received: %s", errmsg, err)
	}
}

func TestEmptyInputTopics(t *testing.T) {
	pConfig := NewPipelineConfig(nil)
	ki := new(KafkaConsumerGroupInput)
	ki.SetPipelineConfig(pConfig)
	config := ki.ConfigStruct().(*KafkaConsumerGroupInputConfig)
	config.ConsumerGroup = "test"
	config.ZookeeperConnectionString = "localhost:2181"
	err := ki.Init(config)

	errmsg := "topics required"
	if err.Error() != errmsg {
		t.Errorf("Expected: %s, received: %s", errmsg, err)
	}
}

func TestMissingConsumerGroup(t *testing.T) {
	pConfig := NewPipelineConfig(nil)
	ki := new(KafkaConsumerGroupInput)
	ki.SetPipelineConfig(pConfig)
	config := ki.ConfigStruct().(*KafkaConsumerGroupInputConfig)
	config.Topics = []string{"test"}
	config.ZookeeperConnectionString = "localhost:2181"
	err := ki.Init(config)

	errmsg := "consumer_group required"
	if err.Error() != errmsg {
		t.Errorf("Expected: %s, received: %s", errmsg, err)
	}
}
