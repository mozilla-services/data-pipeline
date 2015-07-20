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
require "os"
require "string"

local duplicate_original = read_config("duplicate_original")
local SEC_IN_HOUR = 60 * 60
local SEC_IN_DAY = SEC_IN_HOUR * 24

local function get_search_counts(khist, fields)
    if type(khist.SEARCH_COUNTS) ~= "table" then return end

    local cnts = {0, 0, 0, 0}
    for k, v in pairs(khist.SEARCH_COUNTS) do
        for i, e in ipairs({"[Gg]oogle", "[Bb]ing", "[Yy]ahoo", "."}) do
            if string.match(k, e) then
                if type(v.sum) == "number" then
                    cnts[i] = cnts[i] + v.sum
                end
                break
            end
        end
    end

    fields.google.value = cnts[1]
    fields.bing.value   = cnts[2]
    fields.yahoo.value  = cnts[3]
    fields.other.value  = cnts[4]
end


local function get_hours(info)
    local uptime = info.subsessionLength

    if type(uptime) ~= "number" or uptime < 0 or uptime >= 180 * SEC_IN_DAY then
        return 0
    end
    return uptime / SEC_IN_HOUR -- convert to hours
end


local function is_default_browser()
    local json = read_message("Fields[environment.settings]")
    local ok, settings = pcall(cjson.decode, json)
    if not ok then return false end

    local default = settings.isDefaultBrowser
    if type(default) == "boolean" then
        return default
    end
    return false
end


----
local crash_fields = {
    submissionDate      = {value = ""},
    clientId            = {value = ""},
    documentId          = {value = ""},
    country             = {value = ""},
    channel             = {value = ""},
    os                  = {value = ""},
    default             = {value = false},
    reason              = {value = ""},
    buildId             = {value = ""},
}

local main_fields = {
    submissionDate      = crash_fields.submissionDate,
    clientId            = crash_fields.clientId,
    documentId          = crash_fields.documentId,
    country             = crash_fields.country,
    channel             = crash_fields.channel,
    os                  = crash_fields.os,
    default             = crash_fields.default,
    reason              = crash_fields.reason,
    buildId             = crash_fields.buildId,
    hours               = {value = 0},
    google              = {value = 0, value_type = 2},
    bing                = {value = 0, value_type = 2},
    yahoo               = {value = 0, value_type = 2},
    other               = {value = 0, value_type = 2},
    sessionId           = {value = ""},
    subsessionCounter   = {value = 0, value_type = 2},
    pluginHangs         = {value = 0, value_type = 2},
}

local msg = {
    Timestamp   = nil,
    Logger      = "fx",
    Type        = "executive_summary",
    Fields      = main_fields,
}


function process_message()
    if read_message("Type") ~= "telemetry" then return 0 end

    local doc_type = read_message("Fields[docType]")
    if doc_type == "main" then
        msg.Fields = main_fields
    elseif doc_type == "crash" then
        msg.Fields = crash_fields
    else
        return 0
    end

    if duplicate_original then
        inject_message(read_message("raw"))
    end

    msg.Timestamp = read_message("Timestamp")
    msg.Fields.submissionDate = os.date("%Y%m%d", msg.Timestamp / 1e9)

    local cid = read_message("Fields[clientId]")
    if type(cid) ~= "string" then return 0 end
    msg.Fields.clientId.value = cid

    local did = read_message("Fields[documentId]")
    if type(did) ~= "string" then return 0 end
    msg.Fields.documentId.value = did

    msg.Fields.country.value = fx.normalize_country(read_message("Fields[geoCountry]"))
    msg.Fields.channel.value = fx.normalize_channel(read_message("Fields[appUpdateChannel]"))
    msg.Fields.os.value      = fx.normalize_os(read_message("Fields[os]"))
    msg.Fields.default.value = is_default_browser()

    msg.Fields.reason.value = ""
    local reason = read_message("Fields[reason]")
    if type(reason) == "string" then
        msg.Fields.reason.value = reason
    elseif doc_type == "crash" then
        msg.Fields.reason.value = "es.crash"
    end

    msg.Fields.buildId.value = ""
    local bid = read_message("Fields[appBuildId]")
    if type(bid) == "string" then
        msg.Fields.buildId.value = bid
    end

    if doc_type == "main" then
        msg.Fields.hours.value              = 0
        msg.Fields.google.value             = 0
        msg.Fields.bing.value               = 0
        msg.Fields.yahoo.value              = 0
        msg.Fields.other.value              = 0
        msg.Fields.sessionId.value          = ""
        msg.Fields.subsessionCounter.value  = 0
        msg.Fields.pluginHangs.value        = 0

        local json = read_message("Fields[payload.info]")
        local ok, info = pcall(cjson.decode, json)
        if ok then
            msg.Fields.hours.value = get_hours(info)

            if type(info.sessionId) == "string" then
                msg.Fields.sessionId.value = info.sessionId
            end

            if type(info.subsessionCounter) == "number" then
                msg.Fields.subsessionCounter.value = info.subsessionCounter
            end
        end

        json = read_message("Fields[payload.keyedHistograms]")
        local ok, khist = pcall(cjson.decode, json)
        if ok then
            get_search_counts(khist, msg.Fields)

            -- add plugin hang information
            local t = khist.SUBPROCESS_ABNORMAL_ABORT
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
    end

    pcall(inject_message, msg)
    return 0
end
