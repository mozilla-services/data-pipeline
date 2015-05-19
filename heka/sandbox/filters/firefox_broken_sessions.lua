-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Firefox Telemetry Broken Session Analysis

*Example Heka Configuration*

.. code-block:: ini

    [FirefoxBrokenSessions]
    type = "SandboxFilter"
    filename = "lua_filters/firefox_broken_sessions.lua"
    message_matcher = "Type == 'telemetry' && Fields[docType] == 'main'"
    output_limit = 8000000
    memory_limit = 2000000000
    ticker_interval = 0
    preserve_data = false
    timer_event_on_shutdown = true

        [FirefoxBrokenSessions.config]
        items = 100000000
--]]

require "cjson"
require "fx.broken_sessions"
require "table"

local items = read_config("items") or 1000
fx_sids = fx.broken_sessions.new(items)
cnts = {0, 0, 0, 0, 0, 0, 0}
cids = {}

function process_message()
    local cid = read_message("Fields[clientId]")
    if type(cid) ~= "string" then return -1, "no clientId" end

    local json = read_message("Fields[payload.info]")
    local ok, json = pcall(cjson.decode, json)
    if not ok then return -1, json end

    if type(json.sessionId) ~= "string" then return -1, "no sessionId" end

    local ret = 0
    if type(json.subsessionCounter) == "number" then
        ret = fx_sids:add(json.sessionId, json.subsessionCounter)
        cnts[ret+2] = cnts[ret+2] + 1

        if ret > 2 then -- only store the error conditions
            local status = cids[cid]
            if status then
                if ret > status then
                    cids[cid] = ret
                end
            else
                cids[cid] = ret
            end
        end
    end

    if json.reason == "shutdown" or json.reason == "aborted-session" or ret == 3 then
        fx_sids:delete(json.sessionId)
    end
    return 0
end


local returns = {"too many", "duplicate", "missing"}

function timer_event(ns)
    for k,v in pairs(cids) do
        add_to_payload(k, " = ", returns[v - 2], "\n")
    end
    inject_payload("txt", "clientIds")
    cids = {}

    add_to_payload("Add Failed\tAdded\tUpdated\tOut of Order\tToo Many\tDuplicates\tMissing\tActive\n")
    inject_payload("tsv", "stats", table.concat(cnts, "\t"), "\t", fx_sids:count(), "\n")
end
