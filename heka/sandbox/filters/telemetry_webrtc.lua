-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Derived stream for webrtc. https://bugzilla.mozilla.org/show_bug.cgi?id=1231410

*Example Heka Configuration*

.. code-block:: ini

    [TelemetryWebRTC]
    type = "SandboxFilter"
    filename = "lua_filters/telemetry_webrtc.lua"
    message_matcher = "Type == 'telemetry' && Logger == 'telemetry'"
    ticker_interval = 0
    preserve_data = false

--]]

require 'cjson'

local function check_payload (payload)
    local w = payload['webrtc'] or {}
    local i = w['IceCandidatesStats'] or {}
    if next(i['webrtc'] or {}) or next(i['loop'] or {}) then
        return true
    end
    return false
end

function process_message()
    local ok, json = pcall(cjson.decode, read_message("Payload"))
    if not ok then return -1, json end
    local p = json['payload'] or {}
    local found = check_payload(p)
    if not found then
        -- check child payloads for E10s
        local children = read_message("Fields[payload.childPayloads]")
        if not children then return 0 end
        local ok, json = pcall(cjson.decode, children)
        if not ok then return -1, children end
        if type(json) ~= "table" then return -1 end
        for i, child in ipairs(json) do
            found = check_payload(child)
            if found then break end
        end
    end

    if found then
        local raw = read_message("raw")
        inject_message(raw)
    end
    return 0
end

function timer_event(ns)
    -- no op
end
