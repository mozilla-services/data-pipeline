-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Firefox Usage Hours

Config:

- mode (string, optional, default "match")
    Sets the subsessionLength extraction mode to 'match' or 'parse'. Match will
    simply search for the uptime key/value anywhere in the string.  Parse will
    JSON decoded the entire message to extract this one value.

*Example Heka Configuration*

.. code-block:: ini

    [FirefoxUsage]
    type = "SandboxFilter"
    filename = "lua_filters/firefox_usage.lua"
    message_matcher = "Type == 'telemetry'"
    ticker_interval = 60
    preserve_data = true
--]]

require "circular_buffer"
require "cjson"
require "math"
require "os"
require "string"

local DAYS = 30
local SEC_IN_DAY = 60 * 60 * 24
local floor = math.floor
local date = os.date

day_cb  = circular_buffer.new(DAYS, 1, SEC_IN_DAY)
day_cb:set_header(1, "Active Hours")
current_day = -1

local month_names = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
local MONTHS = #month_names
months = {}
for i=1,MONTHS do
    months[i] = 0
end
current_month = -1

local function clear_months(s, n)
    for i = 1, n do
        s = s + 1
        if s > MONTHS then s = 1 end
        months[s] = 0
    end
end

local function update_month(ts, uptime, day_changed)
    local month = current_month
    if current_month == -1 or day_changed then
        local t = date("*t", ts / 1e9)
        month = tonumber(t.month)
        if current_month == -1 then current_month = month end
    end

    local delta = month - current_month
    if delta > 0 then
        clear_months(current_month, delta)
        current_month = month
    elseif delta < -1 then -- if older than a month roll over the year
        clear_months(current_month, MONTHS + delta)
        current_month = month
    end
    months[month] = months[month] + uptime
end

local function parse()
    local json = read_message("Payload")
    local ok, json = pcall(cjson.decode, json)
    if not ok then
        return -1, json
    end

    if type(json.payload) ~= "table" then
        return -1, "Missing payload object"
    end

    if type(json.payload.info) ~= "table" then
        return -1, "Missing payload.info object"
    end

    local uptime = json.payload.info.subsessionLength
    if type(uptime) ~= "number" then
        return -1, "Missing payload.info.subsessionLength"
    end
    uptime = uptime / 3600-- convert to hours

    local ts  = read_message("Timestamp")
    local day = floor(ts / (SEC_IN_DAY * 1e9))
    local day_changed = day ~= current_day
    if day > current_day then
        current_day = day
    end

    day_cb:add(ts, 1, uptime)
    update_month(ts, uptime, day_changed)

    return 0
end

local function match()
    local json = read_message("Payload")
    local uptime = string.match(json, '"subsessionLength":%s*(%d+%.?%d*)')
    if not uptime then
        return -1, "Missing uptime"
    end
    uptime = tonumber(uptime) / 3600 -- convert to hours

    local ts  = read_message("Timestamp")
    local day = floor(ts / (SEC_IN_DAY * 1e9))
    local day_changed = day ~= current_day
    if day > current_day then
        current_day = day
    end

    day_cb:add(ts, 1, uptime)
    update_month(ts, uptime, day_changed)

    return 0
end

----

local mode = read_config("mode") or "match"
if mode == "match" then
    process_message = match
elseif mode == "parse" then
    process_message = parse
else
    error("Invalid configuration mode: " .. mode)
end

function timer_event(ns)
    inject_payload("cbuf", "Firefox Daily Active Hours", day_cb)

    local json = {}
    local idx = current_month
    if idx == -1 then idx = 0 end

    for i=1,MONTHS do
        idx = idx + 1
        if idx > MONTHS then idx = 1 end
        json[i] = {[month_names[idx]] = months[idx]}
    end
    inject_payload("json", "Firefox Monthly Active Hours", cjson.encode(json))
end
