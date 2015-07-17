-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Firefox Channel Switching

*Example Heka Configuration*

.. code-block:: ini

    [FirefoxChannelSwitching]
    type = "SandboxFilter"
    filename = "lua_filters/firefox_channel_switching.lua"
    message_matcher = "Logger == 'fx' && Type == 'executive_summary'"
    memory_limit = 1000000000
    ticker_interval = 60
    preserve_data = true

        [FirefoxChannelSwitching.config]
        anomaly_config = 'mww_nonparametric("nightly", 3, 3, 4, 0.6) mww_nonparametric("beta", 3, 3, 4, 0.6)'
--]]
_PRESERVATION_VERSION = 1

local fx = require "fx"
require "circular_buffer"
require "cuckoo_filter"
local l = require "lpeg"
require "string"

local alert             = require "alert"
local annotation        = require "annotation"
local anomaly           = require "anomaly"
local anomaly_config    = anomaly.parse_config(read_config("anomaly_config"))

local rows        = read_config("rows") or 180
local sec_per_row = read_config("sec_per_row") or 60*60*24
local COL_NEW     = 1
local COL_IN      = 2
local COL_OUT     = 3

local function create_cbuf()
    local cb = circular_buffer.new(rows, COL_OUT, sec_per_row, true)
    cb:set_header(COL_NEW   , "new")
    cb:set_header(COL_IN    , "switched in")
    cb:set_header(COL_OUT   , "switched out")
    return cb
end

channels = {
    {name = "release"        , cb = create_cbuf(), cf = cuckoo_filter.new(100e6)},
    {name = "beta"           , cb = create_cbuf(), cf = cuckoo_filter.new(10e6)},
    {name = "nightly"        , cb = create_cbuf(), cf = cuckoo_filter.new(1e6)},
    -- aurora uses a different profile so we do not expect to see any switches
    {name = "aurora", cb = create_cbuf(), cf = cuckoo_filter.new(1e6)},
    {name = "Other"          , cb = create_cbuf(), cf = cuckoo_filter.new(100e6)},
}
local CHANNELS_SIZE = #channels

function process_message()
    local cid = read_message("Fields[clientId]")
    if not cid then return -1, "missing clientId" end

    local chan = read_message("Fields[appUpdateChannel]")
    if not chan then return -1, "missing appUpdateChannel" end

    chan = fx.normalize_channel(chan)

    local ts = read_message("Timestamp")
    local matched, added, deleted = nil, false, false
    for i=1, CHANNELS_SIZE do
        local v = channels[i]
        if v.name == chan then
            added = v.cf:add(cid)
            matched = v
        else
            if v.cf:delete(cid) then
                v.cb:add(ts, COL_OUT, 1)
                deleted = true
            end
        end

    end

    if added then
        if deleted then
            matched.cb:add(ts, COL_IN, 1)
        else
            matched.cb:add(ts, COL_NEW, 1)
        end
    end

    return 0
end

function timer_event(ns)
    for i,v in ipairs(channels) do
        if anomaly_config then
            if not alert.throttled(ns) then
                local msg, annos = anomaly.detect(ns, v.name, v.cb, anomaly_config)
                if msg then
                    alert.queue(ns, msg)
                    annotation.concat(v.name, annos)
                end
            end
            local a = annotation.prune(v.name, ns)
            if a then
                inject_payload("cbuf", v.name, a, v.cb:format("cbuf"))
            else
                inject_payload("cbuf", v.name, v.cb:format("cbuf"))
            end
        else
            inject_payload("cbuf", v.name, v.cb:format("cbuf"))
        end
        inject_payload("cbufd", v.name, v.cb:format("cbufd"))
    end
    alert.send_queue(ns)
end
