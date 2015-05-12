-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Convert the massive unified telemetry submission into the small sub-set of data
required to power the executive dashboard.

See: https://bugzilla.mozilla.org/show_bug.cgi?id=1155871

*Example Heka Configuration*

.. code-block:: ini

    [TelemetryInput]
    # File or S3 input
    decoder = "MultiDecoder"

    [MultiDecoder]
    subs = ["ProtobufDecoder", "ExecutiveSummary"] # use for S3 telemetry data
    #subs = ["TelemetryDecoder", "ExecutiveSummary"] # use for S3 landfill data
    cascade_strategy = "all"
    log_sub_errors = true

    [TelemetryDecoder]
    type = "SandboxDecoder"
    filename = "lua_decoders/extract_telemetry_dimensions.lua"
    memory_limit = 100000000
    output_limit = 1000000

    [ExecutiveSummary]
    type = "SandboxDecoder"
    filename = "lua_decoders/extract_executive_summary.lua"
    memory_limit = 100000000
--]]

require "cjson"
local l = require "lpeg"
require "string"

local SEC_IN_HOUR = 60 * 60
local SEC_IN_DAY = SEC_IN_HOUR * 24

local normalize_channel =
l.C"release" * -1 +
l.C"beta" +
(l.P("nightly") * -1 + "nightly-cck-") / "nightly" +
l.C"aurora" * -1 +
l.Cc"Other"

local function anywhere (p)
  return lpeg.P{ p + 1 * lpeg.V(1) }
end

local normalize_os =
(l.P"Windows" + "WINNT") / "Windows" +
l.P"Darwin" / "Mac" +
(anywhere"Linux" + anywhere"BSD" + anywhere"SunOS") / "Linux" +
l.Cc"Other"


local function get_search_counts()
    -- google, bing, yahoo, other
    local cnts = {0, 0, 0, 0}
    local json = read_message("Fields[payload.keyedHistograms]")
    if not json then return cnts end

    local ok, khist = pcall(cjson.decode, json)
    if not ok then return cnts end
    if type(khist.SEARCH_COUNTS) ~= "table" then return cnts end

    for k, v in pairs(khist.SEARCH_COUNTS) do
        for i, e in ipairs({"[Gg]oogle", "[Bb]ing", "[Yy]ahoo", "."}) do
            if string.match(k, e) then
                if type(v.sum) ~= "number" then return cnts end
                cnts[i] = cnts[i] + v.sum
                break
            end
        end
    end
    return cnts;
end


local function get_hours()
    local json = read_message("Fields[payload.info]")
    local ok, json = pcall(cjson.decode, json)
    if not ok then return 0 end
    local uptime = json.subsessionLength

    if type(uptime) ~= "number" or uptime < 0 or uptime >= 180 * SEC_IN_DAY then
        return 0
    end
    uptime = uptime / SEC_IN_HOUR -- convert to hours
    return uptime
end


local function is_default_browser()
    local json = read_message("Fields[environment.settings]")
    local ok, json = pcall(cjson.decode, json)
    if not ok then return false end

    local default = json.isDefaultBrowser
    if type(default) == "boolean" then
        return default
    end
    return false
end


----

local msg = {
    Timestamp   = nil,
    Logger      = "fx",
    Type        = "executive_summary",
    Fields      = {
        {name = "clientId"  , value = ""},
        {name = "documentId", value = ""},
        {name = "geo"       , value = ""},
        {name = "channel"   , value = ""},
        {name = "os"        , value = ""},
        {name = "hours"     , value = 0},
        {name = "crashes"   , value = 0, value_type = 2},
        {name = "default"   , value = false},
        {name = "google"    , value = 0, value_type = 2},
        {name = "bing"      , value = 0, value_type = 2},
        {name = "yahoo"     , value = 0, value_type = 2},
        {name = "other"     , value = 0, value_type = 2},
    }
}


function process_message()
    msg.Timestamp = read_message("Timestamp")

    local cid = read_message("Fields[clientId]")
    if type(cid) ~= "string" then return 0 end
    msg.Fields[1].value = cid

    local did = read_message("Fields[documentId]")
    if type(did) ~= "string" then return 0 end
    msg.Fields[2].value = did

    local geo = read_message("Fields[geoCountry]") or "Other"
    if geo == "??" then geo = "Other" end
    msg.Fields[3].value = geo

    local channel = read_message("Fields[appUpdateChannel]") or "Other"
    channel = normalize_channel:match(channel)
    msg.Fields[4].value = channel

    local _os = read_message("Fields[os]") or "Other"
    _os = normalize_os:match(_os)
    msg.Fields[5].value = _os

    msg.Fields[6].value = get_hours()

    -- msg.Fields[7].value = get_crashes()
    -- todo need the crash data
    -- https://bugzilla.mozilla.org/show_bug.cgi?id=1121013

    msg.Fields[8].value = is_default_browser()

    local cnts = get_search_counts()
    msg.Fields[9].value     = cnts[1] -- google
    msg.Fields[10].value    = cnts[2] -- bing
    msg.Fields[11].value    = cnts[3] -- yahoo
    msg.Fields[12].value    = cnts[4] -- other

    inject_message(msg)
    return 0
end
