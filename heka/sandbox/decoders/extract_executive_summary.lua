-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Convert the massive unified telemetry submission into the small sub-set of data
required to power the executive dashboard. This decoder MUST NOT return failure
due to the way the Heka MultiDecoder is implemented.

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
    [TelemetryDecoder.config]
    duplicate_original = false # This MUST be true when processing live data
    # since we need to duplicate the original telemetry message AND output the
    # new summary message.  When processing landfill or telemetry data from S3
    # it should be set to false.

    [ExecutiveSummary]
    type = "SandboxDecoder"
    filename = "lua_decoders/extract_executive_summary.lua"
    memory_limit = 100000000
--]]

require "cjson"
local fx = require "fx"
require "string"

local duplicate_original = read_config("duplicate_original")
local SEC_IN_HOUR = 60 * 60
local SEC_IN_DAY = SEC_IN_HOUR * 24

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
        clientId            = {value = ""},
        documentId          = {value = ""},
        geo                 = {value = ""},
        channel             = {value = ""},
        os                  = {value = ""},
        hours               = {value = 0},
        default             = {value = false},
        google              = {value = 0, value_type = 2},
        bing                = {value = 0, value_type = 2},
        yahoo               = {value = 0, value_type = 2},
        other               = {value = 0, value_type = 2},
        reason              = {value = ""},
        sessionId           = {value = ""},
        subsessionCounter   = {value = 0, value_type = 2},
        buildId             = {value = ""},
        pluginHangs         = {value = 0, value_type = 2},
    }
}


function process_message()
    if read_message("Type") ~= "telemetry"
    or read_message("Fields[docType]") ~= "main" then
        return 0
    end

    if duplicate_original then
        inject_message(read_message("raw"))
    end

    msg.Timestamp = read_message("Timestamp")

    local cid = read_message("Fields[clientId]")
    if type(cid) ~= "string" then return 0 end
    msg.Fields.clientId.value = cid

    local did = read_message("Fields[documentId]")
    if type(did) ~= "string" then return 0 end
    msg.Fields.documentId.value = did

    local geo = read_message("Fields[geoCountry]") or "Other"
    if geo == "??" then geo = "Other" end
    msg.Fields.geo.value = geo

    msg.Fields.channel.value = fx.normalize_channel(read_message("Fields[appUpdateChannel]"))

    msg.Fields.os.value = fx.normalize_os(read_message("Fields[os]"))

    msg.Fields.hours.value = get_hours()

    msg.Fields.default.value = is_default_browser()

    local cnts = get_search_counts()
    msg.Fields.google.value = cnts[1]
    msg.Fields.bing.value   = cnts[2]
    msg.Fields.yahoo.value  = cnts[3]
    msg.Fields.other.value  = cnts[4]

    msg.Fields.reason.value             = ""
    msg.Fields.sessionId.value          = ""
    msg.Fields.subsessionCounter.value  = 0
    msg.Fields.buildId.value            = ""
    msg.Fields.pluginHangs.value        = 0

    -- add session information for broken session monitoring
    local reason = read_message("Fields[reason]")
    if type(reason) == "string" then
        msg.Fields.reason.value = reason
    end

    local json = read_message("Fields[payload.info]")
    local ok, json = pcall(cjson.decode, json)
    if ok then
        if type(json.sessionId) == "string" then
            msg.Fields.sessionId.value = json.sessionId
        end
        if type(json.subsessionCounter) == "number" then
            msg.Fields.subsessionCounter.value = json.subsessionCounter
        end
    end

    -- add plugin hang information
    local bid = read_message("Fields[appBuildId]")
    if type(bid) == "string" then
        msg.Fields.buildId.value = bid
    end

    json = read_message("Fields[payload.keyedHistograms]")
    ok, json = pcall(cjson.decode, json)
    if ok then
        local t = json.SUBPROCESS_ABNORMAL_ABORT
        if type(t) == "table" then
            t = t.plugin
            if type(t) == "table" then
                local sum = t.sum
                if type(sum) == "number" and sum > 0 then
                    msg.Fields.pluginHangs.value = sum
                end
            end
        end
    end

    pcall(inject_message, msg)
    return 0
end
