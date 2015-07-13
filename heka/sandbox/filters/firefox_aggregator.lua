-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Collects the cbufd, txt, and json output from multiple instances of an upstream
Firefox sandbox filter (the filters should all be the same version at least with
respect to their cbuf output). The purpose is to recreate the view at a larger
scope in each level of the aggregation
i.e., host view -> datacenter view -> service level view.

Config:

- enable_delta (bool, optional, default false)
    Specifies whether or not this aggregator should generate cbuf deltas.

- anomaly_config(string) - (see :ref:`sandbox_anomaly_module`)
    A list of anomaly detection specifications.  If not specified no anomaly
    detection/alerting will be performed.

- preservation_version (uint, optional, default 0)
    If `preserve_data = true` is set in the SandboxFilter configuration, then
    this value should be incremented every time the `enable_delta`
    configuration is changed to prevent the plugin from failing to start
    during data restoration.

*Example Heka Configuration*

.. code-block:: ini

    [PluginNameAggregator]
    type = "SandboxFilter"
    message_matcher = "Logger =~ /^PluginName_/ && (Fields[payload_type] == 'cbufd' || Fields[payload_type] == 'json' || Fields[payload_type] == 'txt')"
    ticker_interval = 60
    filename = "lua_filters/firefox_aggregator.lua"
    preserve_data = true
--]]
_PRESERVATION_VERSION = read_config("preservation_version") or 0

require "cjson"
local alert     = require "alert"
local agg       = require "agg"
local annotation= require "annotation"
local anomaly   = require "anomaly"
local cbufd     = require "cbufd"
require "circular_buffer"

local enable_delta = read_config("enable_delta") or false
local anomaly_config = anomaly.parse_config(read_config("anomaly_config"))
local last_update = 0
local MAX_TTL = 24*60*60*1e9

cbufs = {}
hosts = {}

local function init_cbuf(payload_name, h)
    local cb = circular_buffer.new(h.rows, h.columns, h.seconds_per_row, enable_delta)
    for i,v in ipairs(h.column_info) do
        cb:set_header(i, v.name, v.unit, v.aggregation)
    end
    annotation.set_prune(payload_name, h.rows * h.seconds_per_row * 1e9)

    cbufs[payload_name] = {cb = cb, last_update = 0}
    return cbufs[payload_name]
end

local function update_cbuf(cb, data)
    for i,v in ipairs(data) do
        for col, value in ipairs(v) do
            if value == value then -- NaN test, only aggregrate numbers
                local n, u, agg = cb:get_header(col)
                if  agg == "sum" then
                    cb:add(v.time, col, value)
                elseif agg == "min" or agg == "max" then
                    cb:set(v.time, col, value)
                end
            end
        end
    end
end

----

function process_message()
    local ts = read_message("Timestamp")
    if last_update < ts then last_update = ts end

    local payload = read_message("Payload")
    local payload_name = read_message("Fields[payload_name]") or ""
    local payload_type = read_message("Fields[payload_type]")

    if payload_type == "cbufd" then
        local data = cbufd.grammar:match(payload)
        if not data then return -1, "cbufd parse failed" end

        local ok, header = pcall(cjson.decode, data.header)
        if not ok then return -1, "malformed cbufd header" end

        local cbt = cbufs[payload_name]
        if not cbt then
            ok, cbt = pcall(init_cbuf, payload_name, header)
            if not ok then return -1, "invalid cbufd header" end
        end

        if not pcall(update_cbuf, cbt.cb, data) then
            return -1, "invalid cbufd data"
        end

        cbt.last_update = last_update
    elseif payload_type == "txt" or payload_type == "json" then
        local hostname = read_message("Hostname") or "unknown"

        local pt = hosts[payload_type]
        if not pt then
            pt = {}
            hosts[payload_type] = pt
        end

        local pn = pt[payload_name]
        if not pn then
            pn = {}
            pt[payload_name] = pn
        end

        local hn = pn[hostname]
        if not hn then
            hn = {}
            pn[hostname] = hn
        end

        hn.last_update = last_update
        hn.payload = payload
    else
        return -1, "unexpected payload type: " .. payload_type
    end

    return 0
end

function timer_event(ns)
    for k,v in pairs(cbufs) do
        if anomaly_config then
            if not alert.throttled(ns) then
                local msg, annos = anomaly.detect(ns, k, v.cb, anomaly_config)
                if msg then
                    alert.queue(ns, msg)
                    annotation.concat(k, annos)
                end
            end
            inject_payload("cbuf", k, annotation.prune(k, ns), v.cb:format("cbuf"))
        else
            inject_payload("cbuf", k, v.cb:format("cbuf"))
        end

        if enable_delta then
            inject_payload("cbufd", k, v.cb:format("cbufd"))
        end

        if v.last_update + MAX_TTL < last_update then
            cbufs[k] = nil
        end
    end
    alert.send_queue(ns)

    for payload_type, pt in pairs(hosts) do
        if payload_type == "txt" then
            for payload_name, pn in pairs(pt) do
                for hostname, hn in pairs(pn) do
                    add_to_payload(hn.payload)
                    if hn.last_update + MAX_TTL < last_update then
                        pn[hostname] = nil
                    end
                end
                inject_payload(payload_type, payload_name)
            end
        elseif payload_type == "json" then
            for payload_name, pn in pairs(pt) do
                local json
                for hostname, hn in pairs(pn) do
                    local ok, tmp = pcall(cjson.decode, hn.payload)
                    if ok then
                        json = agg.merge_objects(json, tmp)
                    end

                    if hn.last_update + MAX_TTL < last_update then
                        pn[hostname] = nil
                    end
                end
                inject_payload(payload_type, payload_name, cjson.encode(json))
            end
        end
    end
end
