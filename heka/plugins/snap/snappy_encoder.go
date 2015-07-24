/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
# ***** END LICENSE BLOCK *****/

package snap

import (
	"github.com/golang/snappy"
	. "github.com/mozilla-services/heka/pipeline"
)

// SnappyEncoder compresses the Message bytes using snappy compression. Each
// message is compressed separately.
type SnappyEncoder struct {
}

func (re *SnappyEncoder) Init(config interface{}) (err error) {
	return
}

func (re *SnappyEncoder) Encode(pack *PipelinePack) (output []byte, err error) {
	output = snappy.Encode(nil, pack.MsgBytes)
	return output, nil
}

func init() {
	RegisterPlugin("SnappyEncoder", func() interface{} {
		return new(SnappyEncoder)
	})
}
