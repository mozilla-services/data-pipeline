-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Monitor DNT header status

*Example Heka Configuration*

.. code-block:: ini

    [DNT Usage]
    type = "SandboxFilter"
    filename = "examples/monitor_dnt.lua"
    message_matcher = "Type == 'telemetry'"
    ticker_interval = 10
    preserve_data = true
--]]

require "circular_buffer"

local rows = read_config("rows") or 2880
local sec_per_row = read_config("sec_per_row") or 60
local ON  = 1
local OFF = 2
local UNK = 3

local c = circular_buffer.new(rows, 3, sec_per_row, true)
c:set_header(ON, "DNT On")
c:set_header(OFF, "DNT Off")
c:set_header(UNK, "DNT Unknown")

function process_message ()
    local ts = read_message("Timestamp")
    local item = read_message("Fields[DNT]") or "UNKNOWN"

    if item == "1" then
        c:add(ts, ON, 1)
    elseif item == "0" then
        c:add(ts, OFF, 1)
    else
        c:add(ts, UNK, 1)
    end

    return 0
end

function timer_event(ns)
    inject_payload("cbuf", "DNT Status", c)
end
