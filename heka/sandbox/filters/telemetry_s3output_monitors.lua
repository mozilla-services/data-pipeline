-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Monitors the ProcessFileFailures in the S3 outputs

Config:

*Example Heka Configuration*

.. code-block:: ini

    [TelemetryS3OutputMonitors]
    type = "SandboxFilter"
    filename = "lua_filters/telemetry_s3output_monitors.lua"
    ticker_interval = 60
    preserve_data = false # should always be reset on Heka restarts
    message_matcher = "Type == 'heka.all-report'"
--]]

require "cjson"
require "string"
local alert = require "alert"

local plugins        = {}

local function find_plugin(name)
    local p = plugins[name]
    if not p then
        p = {last_alert = 0, last_value = 0}
        plugins[name] = p
    end
    return p
end

function process_message ()
    local ok, json = pcall(cjson.decode, read_message("Payload"))
    if not ok then return -1, json end
    if type(json.outputs) ~= "table" then return -1, "missing outputs array" end

    for i,v in ipairs(json.outputs) do
        if type(v) ~= "table" then return -1, "invalid output object" end
        if type(v.ProcessFileFailures) == "table" then -- confirm this plugin has instrumentation
            if not v.Name then return -1, "missing plugin Name" end
            local p = find_plugin(v.Name)
            local n = v.ProcessFileFailures.value
            if type(n) == "number" and n > p.last_value then
                p.msg = string.format("%s ProcessFileFailures has increased to %d", v.Name, n)
                p.last_value = n
            end
        end
    end
    return 0
end

function timer_event(ns)
    for k,v in pairs(plugins) do
        if v.msg then
            if ns - v.last_alert > 60 * 60 * 1e9 then -- manual throttling (one alert per plugin per hour)
                alert.queue(0, v.msg)
                v.last_alert = ns
            end
        end
        v.msg = nil
    end
    alert.send_queue(0)
end
