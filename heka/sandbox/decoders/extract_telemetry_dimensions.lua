-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "string"
require "cjson"
require "hash"
require "os"
require "table"
local gzip = require "gzip"
local dt = require("date_time")

local UNK_DIM = "UNKNOWN"
local UNK_GEO = "??"

local environment_objects = {
    "addons",
    "build",
    "partner",
    "profile",
    "settings",
    "system"
    }

local main_ping_objects = {
    "addonDetails",
    "addonHistograms",
    "childPayloads", -- only present with e10s
    "chromeHangs",
    "fileIOReports",
    "histograms",
    "info",
    "keyedHistograms",
    "lateWrites",
    "log",
    "simpleMeasurements",
    "slowSQL",
    "slowSQLstartup",
    "threadHangStats",
    "UIMeasurements"
    }

-- telemetry messages should not contain duplicate keys so this function
-- replaces/removes the first key that exists or adds a new key to the end
local function update_field(fields, name, value)
    if value then value = {name = name, value = value} end

    for i,v in ipairs(fields) do
        if name == v.name then
            if value then
                fields[i] = value
            else
                table.remove(fields, i)
            end
            return
        end
    end

    if value then fields[#fields + 1] = value end
end

local function split_objects(fields, root, section, objects)
    if type(root) ~= "table" then return end

    for i, name in ipairs(objects) do
        if type(root[name]) == "table" then
            local ok, json = pcall(cjson.encode, root[name])
            if ok then
                update_field(fields, string.format("%s.%s", section, name), json)
                root[name] = nil -- remove extracted objects
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

local function parse_creation_date(date)
   if type(date) ~= "string" then return nil end

   local t = dt.rfc3339:match(date)
   if not t then
      return nil
   end

   return dt.time_to_ns(t) -- The timezone of the ping has always zero UTC offset
end

local function process_json(msg, json, parsed)
    local clientId
    if parsed.ver then
        -- Old-style telemetry.
        local info = parsed.info
        if type(info) ~= "table" then return "missing info object" end

        msg.Payload = json
        update_field(msg.Fields, "sourceVersion", tostring(parsed.ver))

        -- Get some more dimensions.
        update_field(msg.Fields, "docType"           , info.reason or UNK_DIM)
        update_field(msg.Fields, "appName"           , info.appName or UNK_DIM)
        update_field(msg.Fields, "appVersion"        , info.appVersion or UNK_DIM)
        update_field(msg.Fields, "appUpdateChannel"  , info.appUpdateChannel or UNK_DIM)

        -- Do not want default values for these.
        update_field(msg.Fields, "appBuildId", info.appBuildID)
        update_field(msg.Fields, "os"        , info.OS)
        update_field(msg.Fields, "appVendor" , info.vendor)
        update_field(msg.Fields, "reason"    , info.reason)
        clientId = parsed.clientID -- uppercase ID is correct
        update_field(msg.Fields, "clientId"  , clientId)
    elseif parsed.version then
        -- New-style telemetry, see http://mzl.la/1zobT1S
        local app = parsed.application
        if type(app) ~= "table" then return "missing application object" end

        local cts = parse_creation_date(parsed.creationDate)
        if not cts then return "missing creationDate" end
        update_field(msg.Fields, "creationTimestamp", cts)

        if type(parsed.payload) == "table" and
           type(parsed.payload.info) == "table" then
               update_field(msg.Fields, "reason", parsed.payload.info.reason)
        end

        if type(parsed.environment) == "table" and
           type(parsed.environment.system) == "table" and
           type(parsed.environment.system.os) == "table" then
               update_field(msg.Fields, "os", parsed.environment.system.os.name)
        end

        update_field(msg.Fields, "sourceVersion", tostring(parsed.version))
        update_field(msg.Fields, "docType"      , parsed.type or UNK_DIM)

        -- Get some more dimensions.
        update_field(msg.Fields, "appName"           , app.name or UNK_DIM)
        update_field(msg.Fields, "appVersion"        , app.version or UNK_DIM)
        update_field(msg.Fields, "appUpdateChannel"  , app.channel or UNK_DIM)

        -- Do not want default values for these.
        update_field(msg.Fields, "appBuildId", app.buildId)
        update_field(msg.Fields, "appVendor" , app.vendor)
        clientId = parsed.clientId
        update_field(msg.Fields, "clientId"  , clientId)

        -- restructure the main ping message
        if parsed.type == "main" then
            split_objects(msg.Fields, parsed.environment, "environment", environment_objects)
            split_objects(msg.Fields, parsed.payload, "payload", main_ping_objects)
            local ok, json = pcall(cjson.encode, parsed) -- re-encode the remaining data
            if not ok then return json end
            msg.Payload = json
        else
            msg.Payload = json
        end
    end
    update_field(msg.Fields, "sampleId", sample(clientId, 100))
    return nil -- processing was successful
end

local function send_message(msg, phase, err)
    if err then
        msg.Type = "telemetry.error"
        update_field(msg.Fields, "DecodeErrorType", phase)
        update_field(msg.Fields, "DecodeError", err)
    end
    local ok, err = pcall(inject_message, msg)
    if not ok then
        return -1, err
    end
    return 0
end

function process_message()
    local raw = read_message("raw")
    local ok, msg = pcall(decode_message, raw)
    if not ok then return -1, msg end

    msg.Type = "telemetry"
    msg.EnvVersion = 1
    if not msg.Fields then msg.Fields = {} end

    update_field(msg.Fields, "sourceName", "telemetry")
    update_field(msg.Fields, "submissionDate", os.date("%Y%m%d", msg.Timestamp / 1e9))

    -- Attempt to uncompress the payload if it is gzipped.
    local ok, json = uncompress(msg.Payload)
    if not ok then return send_message(msg, "gzip", json) end

    -- This size check should match the output_limit config param. We want to
    -- check the size early to avoid parsing JSON if we don't have to.
    local size = string.len(json)
    if size > 2097152 then
        return send_message(msg, "size", "Uncompressed Payload too large: " .. size)
    end
    update_field(msg.Fields, "Size", size)

    local ok, parsed = pcall(cjson.decode, json)
    if not ok then return send_message(msg, "json", parsed) end

    -- Extract additional fields from the json
    local err = process_json(msg, json, parsed)
    if err then return send_message(msg, "payload", err) end

    return send_message(msg)
end
