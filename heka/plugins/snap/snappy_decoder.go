/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
# ***** END LICENSE BLOCK *****/

package snap

import (
	"github.com/golang/snappy/snappy"
	. "github.com/mozilla-services/heka/pipeline"
)

// SnappyDecoder decompresses snappy-compressed Message bytes.
type SnappyDecoder struct {
}

func (re *SnappyDecoder) Init(config interface{}) (err error) {
	return
}

func (re *SnappyDecoder) Decode(pack *PipelinePack) (packs []*PipelinePack, err error) {
	output, decodeErr := snappy.Decode(nil, pack.MsgBytes)

	packs = []*PipelinePack{pack}
	if decodeErr == nil {
		// Replace bytes with decoded data
		pack.MsgBytes = output
	}
	// If there is an error decoding snappy, maybe it wasn't compressed. We'll
	// return the original data and try to proceed.
	return
}

func init() {
	RegisterPlugin("SnappyDecoder", func() interface{} {
		return new(SnappyDecoder)
	})
}
