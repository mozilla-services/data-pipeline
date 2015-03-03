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
EnvVersion  = 1
}

local UNK_DIM = "UNKNOWN"
local UNK_GEO = "??"
local environment_objects = {"build", "settings", "profile", "partner", "system", "addons"}
local main_ping_objects = {"info", "simpleMeasurements", "histograms", "keyedHistograms", "chromeHangs", "threadHangStats", "log", "fileIOReports", "lateWrites", "addonDetails", "addonHistograms", "UIMeasurements", "slowSQL", "slowSQLstartup", "childPayloads"}

local function split_objects(root, section, objects)
    if type(root) ~= "table" then return end

    for i, name in ipairs(objects) do
        if type(root[name]) == "table" then
            local ok, json = pcall(cjson.encode, root[name])
            if ok then
                msg.Fields[string.format("%s.%s", section, name)] = json
            end
        end
    end
end

local function uncompress(payload)
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

local function sample(id, sampleRange)
    if type(id) ~= "string" then
        return nil
    end

    return hash.crc32(id) % sampleRange
end

function process_message()
    -- Attempt to uncompress the payload if it is gzipped.
    local payload = read_message("Payload")
    local ok, json = uncompress(payload)
    if not ok then return -1, json end
    -- This size check should match the output_limit config param. We want to
    -- check the size early to avoid parsing JSON if we don't have to.
    if string.len(json) > 2097152 then
        return -1, "Uncompressed Payload too large: " .. string.len(json)
    end

    -- Attempt to parse the payload as JSON.
    local parsed
    ok, parsed = pcall(cjson.decode, json)
    if not ok then return -1, parsed end

    -- Carry forward the dimensions from the submission URL Path. Overwrite
    -- them later with values from the parsed JSON Payload if available.
    -- These fields should match the ones specified in the namespace_config for
    -- the "telemetry" endpoint of the HTTP Edge Server.
    msg.Fields                  = { sourceName = "telemetry" }
    msg.Fields.documentId       = read_message("Fields[documentId]")
    msg.Fields.docType          = read_message("Fields[docType]")
    msg.Fields.appName          = read_message("Fields[appName]")
    msg.Fields.appVersion       = read_message("Fields[appVersion]")
    msg.Fields.appUpdateChannel = read_message("Fields[appUpdateChannel]")
    msg.Fields.appBuildId       = read_message("Fields[appBuildId]")

    if parsed.ver then
        -- Old-style telemetry.
        msg.Payload = json
        msg.Fields.sourceVersion    = tostring(parsed.ver)

        local info = parsed.info
        if type(info) ~= "table" then return -1, "missing info object" end

        -- Get some more dimensions.
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
        if parsed.type == "main" then
            msg.Payload = payload -- keep the gzipped payload since we are moving most of the content into fields
            split_objects(parsed.environment, "environment", environment_objects)
            split_objects(parsed.payload, "main", main_ping_objects)
        else
            msg.Payload = json
        end
        msg.Fields.sourceVersion    = tostring(parsed.version)
        msg.Fields.docType          = parsed.type or UNK_DIM

        local app = parsed.application
        if type(app) ~= "table" then return -1, "missing 'application' object" end

        -- Get some more dimensions.
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

    -- Carry forward more incoming fields.
    msg.Fields.geoCountry = read_message("Fields[geoCountry]") or UNK_GEO
    msg.Timestamp         = read_message("Timestamp")
    msg.Fields.Host       = read_message("Fields[Host]")

    msg.Fields.submissionDate = os.date("%Y%m%d", msg.Timestamp / 1e9)

    msg.Fields.sampleId = sample(msg.Fields.clientId, 100)

    -- Send new message along.
    local err
    ok, err = pcall(inject_message, msg)
    if not ok then
        return -1, err
    end
    return 0
end
