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
    message_matcher = "Logger == 'fx' && Type == 'executive_summary'"
    output_limit = 8000000
    memory_limit = 2000000000
    ticker_interval = 0
    preserve_data = false
    timer_event_on_shutdown = true

        [FirefoxBrokenSessions.config]
        items = 100000000
--]]

require "fx.broken_sessions"
require "table"

local items = read_config("items") or 1000
fx_sids = fx.broken_sessions.new(items)
cnts = {0, 0, 0, 0, 0, 0, 0}
cids = {}

function process_message()
    local cid = read_message("Fields[clientId]")
    if type(cid) ~= "string" then return -1, "no clientId" end

    local sid = read_message("Fields[sessionId]")
    if type(sid) ~= "string" then return -1, "no sessionId" end

    local ssc = read_message("Fields[subsessionCounter]")
    if type(ssc) ~= "number" then return -1, "no subsessionCounter" end

    local ret = fx_sids:add(sid, ssc)
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

    local reason = read_message("Fields[reason]")
    if reason == "shutdown" or reason == "aborted-session" or ret == 3 then
        fx_sids:delete(sid)
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
