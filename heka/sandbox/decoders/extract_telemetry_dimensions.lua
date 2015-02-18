-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "string"
require "cjson"
require "hash"
require "os"
local gzip = require "gzip"

local msg = {
Timestamp   = nil,
Type        = "telemetry",
Payload     = nil,
Fields      = { sourceName = "telemetry" }
}

local UNK_DIM = "UNKNOWN"
local UNK_GEO = "??"

function uncompress(payload)
    local b1, b2 = string.byte(payload, 1, 2)

    if b1 == 0x1f and b2 == 0x8b then  -- test for gzip magic header bytes
        local ok, result = pcall(gzip.decompress, payload)
        if not ok then
            return false, result
        end
        return true, result
    end

    return true, payload
end

function sample(id, sampleRange)
    if not id then
        return nil
    end
    if type(id) ~= "string" then
        return nil
    end

    return hash.crc32(id) % sampleRange
end

function process_message()
    -- Attempt to uncompress the payload if it is gzipped
    local ok
    ok, msg.Payload = uncompress(read_message("Payload"))
    if not ok then return -1, msg.Payload end

    -- Attempt to parse the payload as JSON
    local parsed
    ok, parsed = pcall(cjson.decode, msg.Payload)
    if not ok then return -1, parsed end

    if parsed.ver then
        -- Old-style telemetry
        msg.Fields.sourceVersion    = tostring(parsed.ver)

        local info = parsed.info
        if type(info) ~= "table" then return -1, "missing info object" end

        -- Get some more dimensions
        msg.Fields.docType          = info.reason or UNK_DIM
        msg.Fields.appName          = info.appName or UNK_DIM
        msg.Fields.appVersion       = info.appVersion or UNK_DIM
        msg.Fields.appUpdateChannel = info.appUpdateChannel or UNK_DIM

        -- Do not want default values for these.
        msg.Fields.appBuildId       = info.appBuildID
        msg.Fields.os               = info.OS
        msg.Fields.vendor           = info.vendor
        msg.Fields.clientId         = parsed.clientID
    elseif parsed.version then
        -- New-style telemetry, see http://mzl.la/1zobT1S
        msg.Fields.sourceVersion    = tostring(parsed.version)
        msg.Fields.docType          = parsed.type or UNK_DIM

        local app = parsed.application
        if type(app) ~= "table" then return -1, "missing 'application' object" end

        -- Get some more dimensions
        msg.Fields.appName          = app.name or UNK_DIM
        msg.Fields.appVersion       = app.version or UNK_DIM
        msg.Fields.appUpdateChannel = app.channel or UNK_DIM

        -- Do not want default values for these.
        msg.Fields.appBuildId       = app.buildId
        msg.Fields.vendor           = app.vendor
        msg.Fields.clientId         = parsed.clientId

        msg.Fields.os = nil
        if parsed.environment and
           parsed.environment.system and
           parsed.environment.system.os then
            msg.Fields.os = parsed.environment.system.os.name
        end
    end

    -- Carry forward geolocation (country, at least)
    msg.Fields.geoCountry = read_message("Fields[geoCountry]") or UNK_GEO

    -- Carry forward timestamp.
    msg.Timestamp = read_message("Timestamp")

    msg.Fields.submissionDate = os.date("%Y%m%d", msg.Timestamp / 1e9)

    msg.Fields.sampleId = sample(msg.Fields.clientId, 100)

    -- Send new message along
    inject_message(msg)

    return 0
end
