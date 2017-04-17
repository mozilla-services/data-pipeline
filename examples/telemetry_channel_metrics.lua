-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Monitor telemetry traffic by channel

*Example Heka Configuration*

.. code-block:: ini

    [ByChannelHour]
    type = "SandboxFilter"
    ticker_interval = 10
    preserve_data = false
    message_matcher = "Logger == 'telemetry'"

        [ByChannelHour.config]
        # Modify to adjust the aggregation
        rows = 1440
        sec_per_row = 3600

--]]
require "circular_buffer"

local rows = read_config("rows") or 1440
local sec_per_row = read_config("sec_per_row") or 60

channel_names = {"nightly", "aurora", "beta", "release", "other"}
field_names = {"submissions", "errors", "aborted-session", "environment-change", "shutdown", "daily", "saved-session", "other"}

channel_data = {}
field_index = {}

for i, channel in ipairs(channel_names) do
    channel_data[channel] = circular_buffer.new(rows, #field_names, sec_per_row)
    for j, field in ipairs(field_names) do
        field_index[field] = channel_data[channel]:set_header(j, field, "count")
    end
end

function process_message()
    local ts = read_message("Timestamp")
    
    local stream = read_message("Type")
    
    local channel = read_message("Fields[appUpdateChannel]") or "UNKNOWN"
    local reason = read_message("Fields[reason]") or "UNKNOWN"
    
    local data = channel_data[channel]
    if not data then
        data = channel_data["other"]
    end
    
    data:add(ts, field_index["submissions"], 1)
    if stream == 'telemetry.error' then
        data:add(ts, field_index["errors"], 1)
    end
    
    if field_index[reason] then
        data:add(ts, field_index[reason], 1)
    else
        data:add(ts, field_index["other"], 1)
    end
    
    return 0
end

function timer_event(ns)
    for name, buffer in pairs(channel_data) do
        inject_payload("cbuf", name, buffer)
    end
end