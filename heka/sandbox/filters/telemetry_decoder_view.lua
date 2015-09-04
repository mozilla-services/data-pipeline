-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Creates a summary view of the TelemetryDecoder Statistics.

    [TelemetryStats]
    type = "SandboxFilter"
    message_matcher = "Type == 'telemetry' || Type == 'heka.all-report'"
    filename = "lua_filters/telemetry_decoder_view.lua"
    memory_limit = 120000000
    output_limit = 256000
    ticker_interval = 60
    preserve_data = true
--]]

require "bloom_filter"
require "circular_buffer"
require "cjson"
require "string"
local alert         = require "alert"

local SEC_PER_ROW   = 60
local ROWS          = 2880

local items         = read_config("bloom_items") or 3*1e6
local probability   = read_config("bloom_probability") or 0.01
local decoder_match = read_config("decoder_match") or "^TelemetryKafkaInput(%d+)"
bf                  = bloom_filter.new(items, probability)
cb                  = circular_buffer.new(ROWS, 3, SEC_PER_ROW, true)
local TOTAL         = cb:set_header(1, "Total")
local FAILURES      = cb:set_header(2, "Failures")
local DUPLICATES    = cb:set_header(3, "Duplicates")
id_count            = {} -- array of decoder ids and the last seen count
id_failures         = {} -- array of decoder ids and the last seen failure count

local alert_throttle    = read_config("alert_throttle") or 3600
alert.set_throttle(alert_throttle * 1e9)


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
            cb:set(ts, col, 0/0) -- advance the buffer with a NaN entry
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

            local id = string.match(v.Name, decoder_match)
            if id then
                id = tonumber(id)

                if type(v["ProcessMessageCount-TelemetryDecoder"]) == "table" then
                    update_delta(ts, TOTAL, id, id_count, v["ProcessMessageCount-TelemetryDecoder"].value)
                end

                if type(v["ProcessMessageFailures-TelemetryDecoder"]) == "table" then
                    update_delta(ts, FAILURES, id, id_failures, v["ProcessMessageFailures-TelemetryDecoder"].value)
                end
            end
        end
    elseif typ == "telemetry" then
        local did = read_message("Fields[documentId]")
        if not did then
            return -1, "No documentId"
        end

        local added = bf:add(did)
        if not added then
            cb:add(ts, DUPLICATES, 1)
        end
    end

    return 0
end

last_cleared = nil

local title = "Telemetry Decoder Statistics"
function timer_event(ns)
    if last_cleared and ns - last_cleared >= 1e9 * ROWS * SEC_PER_ROW then
        bf:clear()
        last_cleared = ns
    elseif not last_cleared then
        last_cleared = ns
    end

    if not cb:get(ns, 1) then
        cb:add(ns, 1, 0/0) -- always advance the buffer/graph using a NaN value
    end

    local sum, samples = cb:compute("sum", 1, cb:current_time() - (SEC_PER_ROW * 1e9))
    if samples == 0 then
        alert.send(ns, "no new data")
    end
    inject_payload("cbuf", title, cb:format("cbuf"))
    inject_payload("cbufd", title, cb:format("cbufd"))
end
