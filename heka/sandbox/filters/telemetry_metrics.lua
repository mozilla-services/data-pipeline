-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
JSON output for TelemetryDecoder Statistics.
--]]

local cbufd = require "cbufd"
require "cjson"
require "os"
require "table"

metrics = {
      total_pings          = {}
    , malformed_pings      = {}
    , duplicate_pings      = {}
}

local max_days = 180
local sec_in_day = 60 * 60 * 24

local function create_day(day, t, n)
    if #t == 0 or day > t[#t].time_t then -- only advance the day, gaps are ok but should not occur
        if #t == max_days then
            table.remove(t, 1)
        end
        t[#t+1] = {time_t = day, date = os.date("%F", day), n = n}
        return #t
    end
    return nil
end

local function find_day(day, t)
    for i = #t, 1, -1 do
        local time_t = t[i].time_t
        if day > time_t then
            return nil
        elseif day == time_t then
            return i
        end
    end
end

local function update_adu(day, adu, prev, cur)
    local idx = find_day(day, adu)
    if idx then
        adu[idx].n = cur
    else
        idx = create_day(day, adu, cur)
        if idx and idx > 1 then
            local t = adu[idx-1]
            if t.time_t == day - sec_in_day then
                t.n = prev
            end
        end
    end
end

local function pre_initialize()
    local t = os.time()
    t = t - (t % sec_in_day)
    for i = t - ((max_days-1) * sec_in_day), t, sec_in_day do
        create_day(i, metrics.total_pings, 0)
        create_day(i, metrics.malformed_pings, 0)
        create_day(i, metrics.duplicate_pings, 0)
    end
end
pre_initialize()

function process_message ()
    local payload = read_message("Payload")
    local ok, header = pcall(cjson.decode, payload:match("^([^\n]+)"))
    if not ok then return -1 end

    local prevt, prevf, prevd, curt, curf, curd = payload:match("(.+)\t(.+)\t(.+)\n(.+)\t(.+)\t(.+)\n$")
    prevt = tonumber(prevt) or 0
    if prevt ~= prevt then prevt = 0 end
    prevf = tonumber(prevf) or 0
    if prevf ~= prevf then prevf = 0 end
    prevd = tonumber(prevd) or 0
    if prevd ~= prevd then prevd = 0 end
    curt = tonumber(curt) or 0
    if curt ~= curt then curt = 0 end
    curf = tonumber(curf) or 0
    if curf ~= curf then curf = 0 end
    curd = tonumber(curd) or 0
    if curd ~= curd then curd = 0 end

    local day = header.time + header.seconds_per_row * (header.rows - 1)
    update_adu(day, metrics.total_pings, prevt, curt)
    update_adu(day, metrics.malformed_pings, prevf, curf)
    update_adu(day, metrics.duplicate_pings, prevd, curd)
    return 0
end

function timer_event(ns)
    for k,v in pairs(metrics) do
        inject_payload("json", "pipeline_" .. k, cjson.encode({[k] = v}))
    end
end
