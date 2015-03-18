-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Firefox Usage Hours

*Example Heka Configuration*

.. code-block:: ini

    [FirefoxUsage]
    type = "SandboxFilter"
    filename = "lua_filters/firefox_usage.lua"
    message_matcher = "Type == 'telemetry' && Fields[docType] == 'main' && Fields[appName] == 'Firefox' && Fields[appVendor] == 'Mozilla'"
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

local month_names = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug",
    "Sep", "Oct", "Nov", "Dec"}
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

local function update_month(ts, uptime, day_changed, day_advanced)
    local month = current_month
    if current_month == -1 or day_changed then
        local t = date("*t", ts / 1e9)
        month = tonumber(t.month)
        if current_month == -1 then current_month = month end
    end

    if day_advanced then
        local delta = month - current_month
        if delta > 0 then
            clear_months(current_month, delta)
            current_month = month
        elseif delta < 0 then -- roll over the year
            clear_months(current_month, MONTHS + delta)
            current_month = month
        end
    end
    months[month] = months[month] + uptime
end

----

function process_message()
    local json = read_message("Fields[payload.info]")
    local ok, json = pcall(cjson.decode, json)
    if not ok then
        return -1, json
    end

    local uptime = json.subsessionLength
    if type(uptime) ~= "number" or uptime < 0 or uptime >= 180 * SEC_IN_DAY then
        return -1, "missing/invalid subsessionLength"
    end
    if uptime == 0 then return 0 end

    uptime = uptime / 3600 -- convert to hours

    local ts  = read_message("Timestamp")
    local day = floor(ts / (SEC_IN_DAY * 1e9))
    local day_changed = day ~= current_day
    local day_advanced = false
    if day > current_day then
        current_day = day
        day_advanced = true
    elseif current_day - day > 360 * SEC_IN_DAY then
        return -1, "data is too old"
    end

    day_cb:add(ts, 1, uptime)
    update_month(ts, uptime, day_changed, day_advanced)

    return 0
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
