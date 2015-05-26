-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Firefox Duplicate Telemetry Submission Report

*Example Heka Configuration*

.. code-block:: ini

    [FirefoxDuplicates]
    type = "SandboxFilter"
    filename = "lua_filters/firefox_duplicates.lua"
    message_matcher = "Logger == 'fx' && Type == 'executive_summary'"
    output_limit = 0
    memory_limit = 0
    instruction_limit = 0
    ticker_interval = 0
    preserve_data = false
    timer_event_on_shutdown = true

        [FirefoxDuplicates.config]
        items = 100000000
--]]

require "bloom_filter"
require "circular_buffer"
local fx = require "fx"

local items = read_config("items") or 1000000
local probability = read_config("probability") or 0.01
bf = bloom_filter.new(items, probability)

local cols = fx.get_channel_count()
cb  = circular_buffer.new(180, cols, 60*60*24)
for i=1, cols do
    cb:set_header(i, fx.get_channel_name(i-1))
end

cids = {}

function process_message()
    local did = read_message("Fields[documentId]")
    if type(did) == "string" then
        if not bf:add(did) then
            local ts  = read_message("Timestamp")
            local channel = read_message("Fields[channel]")
            cb:add(ts, fx.get_channel_id(channel) + 1, 1)

            local cid = read_message("Fields[clientId]")
            if type(cid) == "string" then
                cids[cid] = true
            end
        end
    end
    return 0
end


function timer_event(ns)
    inject_payload("cbuf", "graph", cb)

    local found = false
    for k,_ in pairs(cids) do
        add_to_payload(k, "\n")
        found = true
    end

    if found then
        inject_payload("txt", "clients")
        cids = {}
    end
end
