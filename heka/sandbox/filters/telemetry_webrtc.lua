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

function process_message()
    local raw = read_message("raw")
    local payload, json = pcall(cjson.decode, read_message("Payload"))
    if not payload then return -1, json end
    local p = json['payload'] or {}
    local w = p['webrtc'] or {}
    local i = w['IceCandidatesStats'] or {}
    if next(i['webrtc'] or {}) or next(i['loop'] or {}) then
        inject_message(raw)
        return 0
    end
    -- FIXME I'm guessing we should also examine child payloads
    -- local payload, json = pcall(cjson.decode, read_message("Fields[payload.childPayloads]"))
    -- if not payload then return -1, json end
    return 0
end

function timer_event(ns)
    -- no op
end
