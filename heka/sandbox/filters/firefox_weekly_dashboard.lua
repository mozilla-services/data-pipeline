-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Firefox Weekly Dashboard

*Example Heka Configuration*

.. code-block:: ini

    [FirefoxWeeklyDashboard]
    type = "SandboxFilter"
    filename = "lua_filters/firefox_weekly_dashboard.lua"
    message_matcher = "(Logger == 'fx' && Type == 'executive_summary') || (Type == 'telemetry' && Fields[docType] == 'crash')"
    output_limit = 8000000
    memory_limit = 2000000000
    ticker_interval = 0
    preserve_data = false
    timer_event_on_shutdown = true

        [FirefoxWeeklyDashboard.config]
        items = 100000000
--]]
_PRESERVATION_VERSION = 1

fx = require "fx" -- this must be global when we are pulling in other fx submodules
require "fx.executive_report"
require "math"
require "os"
require "string"
require "table"

local WEEKS = 52
local DAY_OFFSET = 4 -- start the week on Sunday and correct for the Unix epoch landing on a Thursday
local SEC_IN_DAY = 60 * 60 * 24
local SEC_IN_WEEK = SEC_IN_DAY * 7
local floor = math.floor
local date = os.date
local format = string.format
local items = read_config("items") or 1000

weeks = {}
current_week = -1
for i=1,WEEKS do
    weeks[i] = {}
end
fx_cids = fx.executive_report.new(items)

local function get_row(week, geo, channel, _os)
    local idx = week % WEEKS + 1
    local w = weeks[idx]
    local key = format("%d,%d,%d", geo, channel, _os)
    local r = w[key]
    if not r then
        local ds = date("%Y-%m-%d", week * SEC_IN_WEEK - (DAY_OFFSET * SEC_IN_DAY))
        -- date, actives, hours, inactives, new_records, five_of_seven, total_records, crashes, default, google, bing, yahoo, other
        r = {ds, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
        w[key] = r
    end
    return r
end

local function clear_weeks(s, e)
    for i = s + 1, e do
        local idx = i % WEEKS + 1
        weeks[idx] = {}
    end
end

local function update_week(ts, cid, day)
    local week = floor((day + DAY_OFFSET) / 7)
    if current_week == -1 then current_week = week end

    local delta = week - current_week
    if delta > 0 and delta < WEEKS then
        fx_cids:report(weeks[current_week % WEEKS + 1])
        clear_weeks(current_week, week)
        current_week = week
    elseif delta >= WEEKS then
        error("data gap over 52 weeks")
    elseif delta < 0 then
        error("data is in the past, this report doesn't back fill")
    end

    local country = fx.get_country_id(read_message("Fields[country]"))
    local channel = fx.get_channel_id(read_message("Fields[channel]"))
    local _os     = fx.get_os_id(read_message("Fields[os]"))

    local r = get_row(week, country, channel, _os)
    if r then
        if read_message("Type") == "executive_summary" then
            local dflt = fx.get_boolean_value(read_message("Fields[default]"))
            fx_cids:add(cid, country, channel, _os, (day + DAY_OFFSET) % 7, dflt)
            r[3]  = r[3]  + (tonumber(read_message("Fields[hours]")) or 0)
            r[10] = r[10] + (tonumber(read_message("Fields[google]")) or 0)
            r[11] = r[11] + (tonumber(read_message("Fields[bing]")) or 0)
            r[12] = r[12] + (tonumber(read_message("Fields[yahoo]")) or 0)
            r[13] = r[13] + (tonumber(read_message("Fields[other]")) or 0)
        else -- crash report
            r[8] = r[8] + 1
        end
    end
end

----

function process_message()
    local ts  = read_message("Timestamp")
    local cid = read_message("Fields[clientId]")
    if type(cid) == "string" then
        local day = floor(ts / (SEC_IN_DAY * 1e9))
        update_week(ts, cid, day)
    end
    return 0
end


function timer_event(ns)
    if current_week == -1 then return end

    fx_cids:report(weeks[current_week % WEEKS + 1])
    add_to_payload("geo,channel,os,date,actives,hours,inactives,new_records,five_of_seven,total_records,crashes,default,google,bing,yahoo,other\n")
    local country, channel, _os
    for i,t in ipairs(weeks) do
        for k,v in pairs(t) do
            country, channel, _os = k:match("(%d+),(%d+),(%d+)")
            add_to_payload(fx.get_country_name(tonumber(country)), ",",
                           fx.get_channel_name(tonumber(channel)), ",",
                           fx.get_os_name(tonumber(_os)), ",",
                           table.concat(v, ","), "\n")
        end
    end
    inject_payload("csv", "firefox_weekly_data")
end
