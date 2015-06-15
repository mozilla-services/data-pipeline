-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Firefox Plugin Hangs

*Example Heka Configuration*

.. code-block:: ini

    [FirefoxPluginHangs]
    type = "SandboxFilter"
    filename = "lua_filters/firefox_plugin_hangs.lua"
    message_matcher = "Logger == 'fx' && Type == 'executive_summary'"
    ticker_interval = 60
    preserve_data = true
--]]


require "circular_buffer"
require "string"
local fx = require "fx"

channels = {}

local BUILD_IDS  = 6
local DAYS       = 180
local SEC_IN_DAY = 60 * 60 * 24

local function set_header(cb, col, id)
    cb:set_header(col, id, "hangs/hour", "none")
end

local channel_cnt = fx.get_channel_count()
for i=1, channel_cnt do
    local hph = circular_buffer.new(DAYS, BUILD_IDS, SEC_IN_DAY)   -- hangs per hour
    local hours = circular_buffer.new(DAYS, BUILD_IDS, SEC_IN_DAY) -- total hours of use
    local hangs = circular_buffer.new(DAYS, BUILD_IDS, SEC_IN_DAY) -- total number of hangs
    channels[fx.get_channel_name(i-1)] = {hph = hph, hours = hours, hangs = hangs, ids = {}}
    for j=1, BUILD_IDS do
        set_header(hph, j, "unknown")
    end
end

local function find_build_id(channel, id)
    local col = nil
    local min_col = nil

    for i=1, BUILD_IDS do
        if not channel.ids[i] then
            channel.ids[i] = id
            col = i
            set_header(channel.hph, col, id)
            break
        end

        if channel.ids[i] == id then
            col = i
            break
        elseif channel.ids[i] < id then
            if not min_col then
                min_col = i
            elseif channel.ids[i] < channel.ids[min_col] then
                min_col = i
            end
        end
    end

    if not col and min_col then
        col = min_col
        local t = channel.hph:current_time()
        for i=1, DAYS do -- clear the column to NaN
            channel.hph:set(t, col, 0/0)
            channel.hours:set(t, col, 0/0)
            channel.hangs:set(t, col, 0/0)
            t = t - SEC_IN_DAY * 1e9
        end
        channel.ids[col] = id
        set_header(channel.hph, col, id)
    end

    return col
end

local function get_build_id_col(channel, bid)
    local id = string.match(bid, "^(%d%d%d%d%d%d%d%d)")
    if not id then return nil end

    return find_build_id(channel, id)
end

local function get_hours()
    local hours = read_message("Fields[hours]")
    if type(hours) ~= "number" or hours < 0 or hours >= 180 * 24 then
        return 0
    end
    return hours
end

----

function process_message ()
    local bid = read_message("Fields[buildId]")
    if type(bid) ~= "string" then return -1, "invalid buildId" end

    local channel = channels[fx.normalize_channel(read_message("Fields[channel]"))]
    local col = get_build_id_col(channel, bid)
    if col then
        local hangs = read_message("Fields[hangs]")
        if type(hangs) ~= "number" then return -1, "invalid hangs" end
        if hangs < 1 then return 0 end

        local ts = read_message("Timestamp")
        local h = channel.hours:add(ts, col, get_hours())
        local t = channel.hangs:add(ts, col, hangs)
        if t and h then
            channel.hph:set(ts, col, t/h)
        end
    end
    return 0
end


function timer_event(ns)
    for k,v in pairs(channels) do
        inject_payload("cbuf", k, v.hph)
    end
end
