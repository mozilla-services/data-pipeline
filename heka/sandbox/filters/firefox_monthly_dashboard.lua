-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Firefox Monthly Dashboard

*Example Heka Configuration*

.. code-block:: ini

    [FirefoxMonthlyDashboard]
    type = "SandboxFilter"
    filename = "lua_filters/firefox_monthly_dashboard.lua"
    message_matcher = "(Logger == 'fx' && Type == 'executive_summary') || (Type == 'telemetry' && Fields[docType] == 'crash')"
    output_limit = 8000000
    memory_limit = 2000000000
    ticker_interval = 0
    preserve_data = false
    timer_event_on_shutdown = true

        [FirefoxMonthlyDashboard.config]
        items = 100000000
--]]


fx = require "fx" -- this must be global when we are pulling in other fx submodules
require "fx.executive_report"
require "math"
require "os"
require "string"
require "table"

local SEC_IN_DAY = 60 * 60 * 24
local floor = math.floor
local date = os.date
local format = string.format
local items = read_config("items") or 1000

local month_names = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug",
    "Sep", "Oct", "Nov", "Dec"}
local MONTHS = #month_names
months = {}
current_month = -1
current_day = -1
for i=1,MONTHS do
    months[i] = {}
end
fx_cids = fx.executive_report.new(items)


local function get_row(ts, month, geo, channel, _os)
    local m = months[month]
    local key = format("%d,%d,%d", geo, channel, _os)
    local r = m[key]
    if not r then
        local ds = date("%Y-%m-01", ts / 1e9)
        -- date, actives, hours, inactives, new_records, five_of_seven, total_records, crashes, default, google, bing, yahoo, other
        r = {ds, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
        m[key] = r
    end
    return r
end


local function clear_months(s, n)
    for i = 1, n do
        s = s + 1
        if s > MONTHS then s = 1 end
        months[s] = {}
    end
end


local function update_month(ts, cid, day_changed)
    local month = current_month
    if current_month == -1 or day_changed then
        local t = date("*t", ts / 1e9)
        month = tonumber(t.month)
        if current_month == -1 then current_month = month end
    end

    local delta = month - current_month
    if delta > 0 then
        fx_cids:report(months[current_month])
        clear_months(current_month, delta)
        current_month = month
    elseif delta < 0 then -- roll over the year
        fx_cids:report(months[current_month])
        clear_months(current_month, MONTHS + delta)
        current_month = month
    end

    local msgType = read_message("Type")
    local _os = fx.get_os_id(read_message("Fields[os]"))
    local country, channel
    if msgType == "executive_summary" then
        country = fx.get_country_id(read_message("Fields[country]"))
        channel = fx.get_channel_id(read_message("Fields[channel]"))
    else
        country = fx.get_country_id(read_message("Fields[geoCountry]"))
        channel = fx.get_channel_id(read_message("Fields[appUpdateChannel]"))
    end

    local r = get_row(ts, month, country, channel, _os)
    if r then
        if msgType == "executive_summary" then
            local dflt = fx.get_boolean_value(read_message("Fields[default]"))
            fx_cids:add(cid, country, channel, _os, 0, dflt)
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
        local day_changed = day ~= current_day
        if day < current_day then
            error("data is in the past, this report doesn't back fill")
        end
        current_day = day
        update_month(ts, cid, day_changed)
    end
    return 0
end


function timer_event(ns)
    if current_month == -1 then return end

    fx_cids:report(months[current_month])
    add_to_payload("geo,channel,os,date,actives,hours,inactives,new_records,five_of_seven,total_records,crashes,default,google,bing,yahoo,other\n")
    local country, channel, _os
    for i,t in ipairs(months) do
        for k,v in pairs(t) do
            country, channel, _os = k:match("(%d+),(%d+),(%d+)")
            add_to_payload(fx.get_country_name(tonumber(country)), ",",
                           fx.get_channel_name(tonumber(channel)), ",",
                           fx.get_os_name(tonumber(_os)), ",",
                           table.concat(v, ","), "\n")
        end
    end
    inject_payload("csv", "firefox_monthly_data")
end
