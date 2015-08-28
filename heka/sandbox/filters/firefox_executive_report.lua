-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Firefox Executive Report

*Example Heka Configuration*

.. code-block:: ini

    [FirefoxExecutiveReport]
    type = "SandboxFilter"
    filename = "lua_filters/firefox_executive_report.lua"
    message_matcher = "Logger == 'fx' && Type == 'executive_summary' && Fields[vendor] == 'Mozilla' && Fields[app] == 'Firefox'"
    output_limit = 0
    memory_limit = 0
    ticker_interval = 0
    preserve_data = false
    timer_event_on_shutdown = true

        [FirefoxExecutiveReport.config]
        items = 100000000
        # rollup_interval = "day" # day|week|month
        # rollup_os       = "Windows" # this will rollup the Windows OS versions (set the message_matcher accordingly)
        # finalize_on_exit = false # forces the current interval (most likely incomplete data) to be rolled up
--]]
_PRESERVATION_VERSION = 2

fx = require "fx" -- this must be global when we are pulling in other fx submodules
require "fx.executive_report"
require "math"
require "os"
require "string"
require "table"

local DAYS        = 31
local WEEKS       = 52
local MONTHS      = 12
local DAY_OFFSET  = 4 -- start the week on Sunday and correct for the Unix epoch landing on a Thursday
local SEC_IN_DAY  = 60 * 60 * 24
local SEC_IN_WEEK = SEC_IN_DAY * 7

local items             = read_config("items") or 1000
local rollup_os         = read_config("rollup_os")
local rollup_interval   = read_config("rollup_interval") or "day"
local finalize_on_exit  = read_config("finalize_on_exit")

local floor  = math.floor
local date   = os.date
local format = string.format

fx_cids             = fx.executive_report.new(items)
intervals           = {}
current_day         = -1
current_interval    = -1

local get_os_id
local get_os_name
if rollup_os == "Windows" then
    get_os_id = function() return fx.get_os_win_id(read_message("Fields[osVersion]")) end
    get_os_name = fx.get_os_win_name
else
    get_os_id = function() return fx.get_os_id(read_message("Fields[os]")) end
    get_os_name = fx.get_os_name
end


local function get_row(interval, time_t_fmt, time_t)
    local country = fx.get_country_id(read_message("Fields[country]"))
    local channel = fx.get_channel_id(read_message("Fields[channel]"))
    local _os     = get_os_id()

    local partition = format("%d,%d,%d", country, channel, _os)
    local r = interval[partition]
    if not r then
        local time_str = date(time_t_fmt, time_t)
        -- date, actives, hours, inactives, new_records, five_of_seven, total_records, crashes, default, google, bing, yahoo, other
        r = {time_str, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
        interval[partition] = r
    end
    return r, country, channel, _os
end


local function update_row(r, cid, country, channel, _os, dow)
    local dflt = fx.get_boolean_value(read_message("Fields[default]"))
    fx_cids:add(cid, country, channel, _os, dow, dflt)

    local doc_type = read_message("Fields[docType]")
    if doc_type == "main" then
        r[3]  = r[3]  + (tonumber(read_message("Fields[hours]")) or 0)
        r[10] = r[10] + (tonumber(read_message("Fields[google]")) or 0)
        r[11] = r[11] + (tonumber(read_message("Fields[bing]")) or 0)
        r[12] = r[12] + (tonumber(read_message("Fields[yahoo]")) or 0)
        r[13] = r[13] + (tonumber(read_message("Fields[other]")) or 0)
    elseif doc_type == "crash" then
        r[8] = r[8] + 1
    end
end


local function clear_intervals(s, e, size)
    for i = s + 1, e do
        local idx = i % size + 1
        intervals[idx] = {}
    end
    current_interval = e
end


local function update_day(ts, cid, day)
    if current_interval == -1 then current_interval = day end

    local delta = day - current_interval
    if delta > 0 and delta < DAYS then
        fx_cids:report(intervals[current_interval % DAYS + 1])
        clear_intervals(current_interval, day, DAYS)
    elseif delta >= DAYS then
        error(string.format("data gap over %d days", DAYS))
    end

    local t = intervals[day % DAYS + 1]
    local r, country, channel, _os = get_row(t, "%Y-%m-%d", ts / 1e9)
    if r then
        update_row(r, cid, country, channel, _os, 0)
    end
end


local function update_week(_, cid, day)
    local week = floor((day + DAY_OFFSET) / 7)
    if current_interval == -1 then current_interval = week end

    local delta = week - current_interval
    if delta > 0 and delta < WEEKS then
        fx_cids:report(intervals[current_interval % WEEKS + 1])
        clear_intervals(current_interval, week, WEEKS)
    elseif delta >= WEEKS then
        error(string.format("data gap over %d weeks", WEEKS))
    end

    local interval = intervals[week % WEEKS + 1]
    local r, country, channel, _os = get_row(interval, "%Y-%m-%d", week * SEC_IN_WEEK - (DAY_OFFSET * SEC_IN_DAY))
    if r then
        -- The use of submission date changes the meaning of the day of the week
        -- calculation, it now represents the days the user interacted with the
        -- telemetry system. The V2 analysis reported on the user provided date
        -- which is activityTimestamp in the executive summary.
        -- See: https://docs.google.com/document/d/1mLP4DY-FIQHof6Nxh2ioVQ-ZvvlnIZ_6yLqYp8idXG4
        update_row(r, cid, country, channel, _os, (day + DAY_OFFSET) % 7)
    end
end


local function update_month(ts, cid, _, day_changed)
    local month = current_interval
    if current_interval == -1 or day_changed then
        local t = date("*t", ts / 1e9)
        month = (tonumber(t.year) - 1) * 12 + tonumber(t.month)
        if current_interval == -1 then current_interval = month end
    end

    local delta = month - current_interval
    if delta > 0 and delta < MONTHS then
        fx_cids:report(intervals[current_interval % MONTHS + 1])
        clear_intervals(current_interval, month, MONTHS)
    elseif delta >= MONTHS then
        error(string.format("data gap over %d months", MONTHS))
    end

    local t = intervals[month % MONTHS + 1]
    local r, country, channel, _os = get_row(t, "%Y-%m-01", ts / 1e9)
    if r then
        update_row(r, cid, country, channel, _os, 0)
    end
end


local update_interval
if rollup_interval == "day" then
    for i=1, DAYS do
        intervals[i] = {}
    end
    update_interval = update_day
elseif rollup_interval == "week" then
    for i=1, WEEKS do
        intervals[i] = {}
    end
    update_interval = update_week
elseif rollup_interval == "month" then
    for i=1, MONTHS do
        intervals[i] = {}
    end
    update_interval = update_month
else
    error("invalid rollup_interval: " .. rollup_interval)
end


----


function process_message()
    local ts  = read_message("Timestamp") -- use the submission date https://docs.google.com/document/d/1mLP4DY-FIQHof6Nxh2ioVQ-ZvvlnIZ_6yLqYp8idXG4
    local cid = read_message("Fields[clientId]")
    if type(cid) == "string" then
        local day = floor(ts / (SEC_IN_DAY * 1e9))
        local day_changed = day ~= current_day
        if day < current_day then
            error("data is in the past, this report doesn't back fill")
        end
        current_day = day
        update_interval(ts, cid, day, day_changed)
    end
    return 0
end


function timer_event(ns)
    if current_interval == -1 then return end

    if finalize_on_exit then
        fx_cids:report(intervals[current_interval % #intervals + 1])
    end

    add_to_payload("geo,channel,os,date,actives,hours,inactives,new_records,five_of_seven,total_records,crashes,default,google,bing,yahoo,other\n")
    local country, channel, _os
    for i,t in ipairs(intervals) do
        for k,v in pairs(t) do
            country, channel, _os = k:match("(%d+),(%d+),(%d+)")
            add_to_payload(fx.get_country_name(tonumber(country)), ",",
                           fx.get_channel_name(tonumber(channel)), ",",
                           get_os_name(tonumber(_os)), ",",
                           table.concat(v, ","), "\n")
        end
    end
    inject_payload("csv", rollup_interval)
end
