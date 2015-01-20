-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
_PRESERVATION_VERSION = 1

require "circular_buffer"
local alert         = require "alert"
local annotation    = require "annotation"
local anomaly       = require "anomaly"

local title             = "FHR Requests"
local rows              = read_config("rows") or 14400
local sec_per_row       = read_config("sec_per_row") or 60
local anomaly_config    = anomaly.parse_config(read_config("anomaly_config"))
annotation.set_prune(title, rows * sec_per_row * 1e9)

cbuf = circular_buffer.new(rows, 1, sec_per_row)
cbuf:set_header(1, "Requests")

function process_message ()
    cbuf:add(read_message("Timestamp"), 1, 1)
    return 0
end

function timer_event(ns)
    if anomaly_config then
        if not alert.throttled(ns) then
            local msg, annos = anomaly.detect(ns, title, cbuf, anomaly_config)
            if msg then
                annotation.concat(title, annos)
                alert.send(ns, msg)
            end
        end
        inject_payload("cbuf", title, annotation.prune(title, ns), cbuf)
    else
        inject_payload("cbuf", title, cbuf)
    end
end
