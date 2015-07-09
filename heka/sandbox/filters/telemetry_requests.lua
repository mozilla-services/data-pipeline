-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
_PRESERVATION_VERSION = 1

require "circular_buffer"

local title             = "Telemetry Requests"
local rows              = read_config("rows") or 14400
local sec_per_row       = read_config("sec_per_row") or 60

cbuf = circular_buffer.new(rows, 1, sec_per_row, true)
cbuf:set_header(1, "Requests")

function process_message ()
    cbuf:add(read_message("Timestamp"), 1, 1)
    return 0
end

function timer_event(ns)
    inject_payload("cbuf", title, cbuf:format("cbuf"))
    inject_payload("cbufd", title, cbuf:format("cbufd"))
end
