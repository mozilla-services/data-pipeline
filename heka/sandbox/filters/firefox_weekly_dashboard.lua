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
    message_matcher = "Logger == 'fx' && Type == 'executive_summary'"
    output_limit = 8000000
    memory_limit = 2000000000
    ticker_interval = 0
    preserve_data = false
    timer_event_on_shutdown = true

        [FirefoxWeeklyDashboard.config]
        items = 100000000
--]]

require "fxcf"
require "math"
require "os"
require "string"
require "table"

local WEEKS = 52
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
fx_cids = fxcf.new(items)

local function get_row(week, geo, channel, _os)
    local idx = week % WEEKS + 1
    local w = weeks[idx]
    local key = format("%d,%d,%d", geo, channel, _os)
    local r = w[key]
    if not r then
        local ds = date("%Y-%m-%d", week * SEC_IN_WEEK - (3 * SEC_IN_DAY))
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

local country_names = {
"Other","AD","AE","AF","AG","AI","AL","AM","AO","AQ","AR","AS","AT","AU","AW","AX","AZ","BA","BB","BD","BE","BF","BG","BH","BI","BJ","BL","BM","BN","BO","BQ","BR","BS","BT","BV","BW","BY","BZ","CA","CC","CD","CF","CG","CH","CI","CK","CL","CM","CN","CO","CR","CU","CV","CW","CX","CY","CZ","DE","DJ","DK","DM","DO","DZ","EC","EE","EG","EH","ER","ES","ET","FI","FJ","FK","FM","FO","FR","GA","GB","GD","GE","GF","GG","GH","GI","GL","GM","GN","GP","GQ","GR","GS","GT","GU","GW","GY","HK","HM","HN","HR","HT","HU","ID","IE","IL","IM","IN","IO","IQ","IR","IS","IT","JE","JM","JO","JP","KE","KG","KH","KI","KM","KN","KP","KR","KW","KY","KZ","LA","LB","LC","LI","LK","LR","LS","LT","LU","LV","LY","MA","MC","MD","ME","MF","MG","MH","MK","ML","MM","MN","MO","MP","MQ","MR","MS","MT","MU","MV","MW","MX","MY","MZ","NA","NC","NE","NF","NG","NI","NL","NO","NP","NR","NU","NZ","OM","PA","PE","PF","PG","PH","PK","PL","PM","PN","PR","PS","PT","PW","PY","QA","RE","RO","RS","RU","RW","SA","SB","SC","SD","SE","SG","SH","SI","SJ","SK","SL","SM","SN","SO","SR","SS","ST","SV","SX","SY","SZ","TC","TD","TF","TG","TH","TJ","TK","TL","TM","TN","TO","TR","TT","TV","TW","TZ","UA","UG","UM","US","UY","UZ","VA","VC","VE","VG","VI","VN","VU","WF","WS","YE","YT","ZA","ZM","ZW"}
local country_codes = {}
for i, v in ipairs(country_names) do
    country_codes[v] = i -1
end

local function get_country_id()
    local id = country_codes[read_message("Fields[geo]") or "Other"]
    if not id then id = 0 end
    return id
end


local channel_names = {"Other", "release", "beta", "nightly", "aurora"}
local channel_codes = {}
for i, v in ipairs(channel_names) do
    channel_codes[v] = i -1
end


local function get_channel_id()
    local id = channel_codes[read_message("Fields[channel]") or "Other"]
    if not id then id = 0 end
    return id
end


local os_names = {"Other", "Windows", "Mac", "Linux"}
local os_codes = {}
for i, v in ipairs(os_names) do
    os_codes[v] = i -1
end


local function get_os_id()
    local id = os_codes[read_message("Fields[os]") or "Other"]
    if not id then id = 0 end
    return id
end


local function get_default()
    local dflt = read_message("Fields[default]")
    if type(dflt) == "boolean" then
        return dflt
    end
    return false;
end


local function update_week(ts, cid, day)
    local week = floor((day + 3) / 7) -- align the week on Monday
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

    local country = get_country_id()
    local channel = get_channel_id()
    local _os = get_os_id()
    local dflt = get_default()

    fx_cids:add(cid, country, channel, _os, (day + 3) % 7, dflt)
    local r = get_row(week, country, channel, _os)
    if r then
        r[3]  = r[3]  + (tonumber(read_message("Fields[hours]")) or 0)
        r[10] = r[10] + (tonumber(read_message("Fields[google]")) or 0)
        r[11] = r[11] + (tonumber(read_message("Fields[bing]")) or 0)
        r[12] = r[12] + (tonumber(read_message("Fields[yahoo]")) or 0)
        r[13] = r[13] + (tonumber(read_message("Fields[other]")) or 0)
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
    fx_cids:report(weeks[current_week % WEEKS + 1])
    add_to_payload("geo,channel,os,date,actives,hours,inactives,new_records,five_of_seven,total_records,crashes,default,google,bing,yahoo,other\n")
    local country, channel, _os
    for i,t in ipairs(weeks) do
        for k,v in pairs(t) do
            country, channel, _os = k:match("(%d+),(%d+),(%d+)")
            add_to_payload(country_names[tonumber(country) + 1], ",",
                           channel_names[tonumber(channel) + 1], ",",
                           os_names[tonumber(_os) + 1], ",",
                           table.concat(v, ","), "\n")
        end
    end
    inject_payload("csv", "firefox_weekly_data")
end
