-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Creates a summary view of the TelemetryDecoder Statistics.
--]]

require "os"
require "bloom_filter"
require "circular_buffer"
require "cjson"
require "string"
require "table"

local SEC_PER_ROW   = 60
local ROWS          = 2880

local items         = read_config("bloom_items") or 3*1e6
local probability   = read_config("bloom_probability") or 0.01
bf                  = bloom_filter.new(items, probability)
cb                  = circular_buffer.new(ROWS, 3, SEC_PER_ROW)
local TOTAL         = cb:set_header(1, "Total")
local FAILURES      = cb:set_header(2, "Failures")
local DUPLICATES    = cb:set_header(3, "Duplicates")
id_count            = {} -- array of decoder ids and the last seen count
id_failures         = {} -- array of decoder ids and the last seen failure count

local max_entries = 24 * 180
local sec_in_hour = 60 * 60
report_json = {}

local function create_hour(hour, t)
    if #t == 0 or hour > t[#t].time_t then -- only advance the hour, gaps are ok but should not occur
        if #t == max_entries then
            table.remove(t, 1)
        end
        t[#t+1] = {time_t = hour,
                   date = os.date('!%Y-%m-%dT%H:%M:%SZ', hour),
                   total_pings = 0,
                   malformed_pings = 0,
                   duplicate_pings = 0}
        return #t
    end
    return nil
end

local function find_hour(hour, t)
    for i = #t, 1, -1 do
        local time_t = t[i].time_t
        if hour > time_t then
            return nil
        elseif hour == time_t then
            return i
        end
    end
end

local function set(t, h, field, v)
    local idx = find_hour(h, t)
    if not idx then
        idx = create_hour(h, t)
    end
    if t[idx][field] then
        t[idx][field] = v
    end
end

local function pre_initialize()
    local t = os.time()
    t = t - (t % sec_in_hour)
    for i = t - ((max_entries-1) * sec_in_hour), t, sec_in_hour do
        create_hour(i, report_json)
    end
end
pre_initialize()

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

            local id = string.match(v.Name, "^TelemetryKafkaInput(%d+)")
            if not id then
                return -1, "Telemetry decoder is missing its identifier"
            else
                id = tonumber(id)
            end

            if type(v["ProcessMessageCount-TelemetryDecoder"]) == "table" then
                update_delta(ts, TOTAL, id, id_count, v["ProcessMessageCount-TelemetryDecoder"].value)
            end

            if type(v["ProcessMessageFailures-TelemetryDecoder"]) == "table" then
                update_delta(ts, FAILURES, id, id_failures, v["ProcessMessageFailures-TelemetryDecoder"].value)
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

function timer_event(ns)
    if last_cleared and ns - last_cleared >= 1e9 * ROWS * SEC_PER_ROW then
        bf:clear()
        last_cleared = ns
    elseif not last_cleared then
        last_cleared = ns
    end

    local e = cb:current_time()
    local s = e - (e % (sec_in_hour * 1e9))
    local sec = s / 1e9

    set (report_json, sec, "total_pings", cb:compute("sum", TOTAL, s, e))
    set (report_json, sec, "malformed_pings", cb:compute("sum", FAILURES, s, e))
    set (report_json, sec, "duplicate_pings", cb:compute("sum", DUPLICATES, s, e))

    inject_payload("cbuf", "Telemetry Decoder Statistics", cb)
    inject_payload("json", "Telemetry Decoder Report JSON", cjson.encode(report_json))
end
