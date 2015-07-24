-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Request Counts by Normalized Channel

*Example Heka Configuration*

.. code-block:: ini

    [CountByNormalizedChannel]
    type = "SandboxFilter"
    filename = "lua_filters/count_by_normalized_channel.lua"
    message_matcher = "Logger == 'fx' && Type == 'executive_summary' && Fields[vendor] == 'Mozilla' && Fields[app] == 'Firefox'"
    ticker_interval = 60
    preserve_data = true

--]]

require "circular_buffer"
fx = require "fx"

local rows = read_config("rows") or 1440
local sec_per_row = read_config("sec_per_row") or 60

local nchannels = fx.get_channel_count()
local channel_counter = circular_buffer.new(rows, nchannels, sec_per_row, true)
for i=1,nchannels do
    -- Circular buffer columns are one-based, channel ids are zero-based.
    channel_counter:set_header(i, fx.get_channel_name(i - 1))
end

function process_message()
    local ts = read_message("Timestamp")
    local normalized = fx.normalize_channel(read_message("Fields[channel]"))

    -- Need to add one to account for "Other" (which comes back as zero)
    local column_id = fx.get_channel_id(normalized) + 1
    channel_counter:add(ts, column_id, 1)
    return 0
end

local title = "Counts by Normalized Channel"
function timer_event(ns)
    inject_payload("cbuf", title, channel_counter:format("cbuf"))
    inject_payload("cbufd", title, channel_counter:format("cbufd"))
end
