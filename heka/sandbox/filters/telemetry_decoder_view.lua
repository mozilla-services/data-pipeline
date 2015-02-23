-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Creates a summary view of the TelemetryDecoder Statistics.
--]]

require "bloom_filter"
require "circular_buffer"
require "cjson"
require "string"

bf                  = bloom_filter.new(3*1e6, 0.01) -- todo up the size when not under the sandbox manager
cb                  = circular_buffer.new(2880, 3, 60)
local TOTAL         = cb:set_header(1, "Total")
local FAILURES      = cb:set_header(2, "Failures")
local DUPLICATES    = cb:set_header(3, "Duplicates")
id_count            = {} -- array of decoder ids and the last seen count
id_failures         = {} -- array of decoder ids and the last seen failure count


local function update_delta(ts, col, id, parray, cur)
    local previous = parray[id]
    if previous then
        if type(cur) == "number" then
            if cur > previous then
                local delta = cur - previous
                parray[id] = cur
                cb:add(ts, col, delta)
            elseif cur < previous then -- system restart
                parray[id] = cur
                cb:add(ts, col, cur)
            end
        end
    else
        if type(cur) == "number" then
            parray[id] = cur
            cb:set(ts, col, 0)
        end
    end
end

----

function process_message ()
    local typ = read_message("Type")
    local ts = read_message("Timestamp")

    if typ == "heka.all-report" then
        local ok, json = pcall(cjson.decode, read_message("Payload"))
        if not ok then return -1, json end

        local t = json.decoders
        if not t then
            return -1, "No Decoders found"
        end

        for i,v in ipairs(t) do
            if not v.Name then
                return -1, "Decoder is missing its name"
            end

            local id = string.match(v.Name, "^TelemetryKafkaInput(%d+)")
            if not id then
                return -1, "Telemetry decoder is missing its identifier"
            else
                id = tonumber(id)
            end

            -- todo we may want to break this out by ProcessMessage*-TelemetryDecoder
            if type(v.ProcessMessageCount) == "table" then
                update_delta(ts, TOTAL, id, id_count, v.ProcessMessageCount.value)
            end

            if type(v.ProcessMessageFailures) == "table" then
                update_delta(ts, FAILURES, id, id_failures, v.ProcessMessageFailures.value)
            end
        end
    elseif typ == "telemetry" then
        local did = read_message("Fields[DocumentID]")
        if not did then
            return -1, "No DocumentID"
        end

        local added = bf:add(did)
        if not added then
            cb:add(ts, DUPLICATES, 1)
        end
    end

    return 0
end

last_cleared = nil

function timer_event(ns)
    if last_cleared and ns - last_cleared >= 1e9 * 86400 * 2 then
        bf.clear()
        last_cleared = ns
    elseif not last_cleared then
        last_cleared = ns
    end

    inject_payload("cbuf", "Telemetry Decoder Statistics", cb)
end
