-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "string"
require "cjson"
require "hash"
require "os"
local gzip = require "gzip"
local dt = require("date_time")
require "table"
require 'geoip.city'

-- Split a path into components. Multiple consecutive separators do not
-- result in empty path components.
-- Examples:
--   /foo/bar      ->   {"foo", "bar"}
--   ///foo//bar/  ->   {"foo", "bar"}
--   foo/bar/      ->   {"foo", "bar"}
--   /             ->   {}
local sep = lpeg.P("/")
local elem = lpeg.C((1 - sep)^1)
local path_grammar = lpeg.Ct(elem^0 * (sep^0 * elem)^0)

local function split_path(s)
    if not s then
        return {}
    end
    return lpeg.match(path_grammar, s)
end

local city_db = assert(geoip.city.open(read_config("geoip_city_db")))
local UNK_GEO = "??"

function get_geo_country(xff, remote_addr)
    local country
    if xff then
        local first_addr = string.match(xff, "([^, ]+)")
        if first_addr then
            country = city_db:query_by_addr(first_addr, "country_code")
        end
    end
    if country then return country end
    if remote_addr then
        country = city_db:query_by_addr(remote_addr, "country_code")
    end
    return country or UNK_GEO
end


local ok, ns_config = pcall(cjson.decode, read_config("namespace_config"))
if not ok then return -1, ns_config end

local msg = {
Timestamp   = nil,
Type        = "telemetry",
Payload     = nil,
EnvVersion  = 1
}

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

local function split_objects(root, section, objects)
    if type(root) ~= "table" then return end

    for i, name in ipairs(objects) do
        if type(root[name]) == "table" then
            local ok, json = pcall(cjson.encode, root[name])
            if ok then
                msg.Fields[string.format("%s.%s", section, name)] = json
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

function parse_creation_date(date)
   if type(date) ~= "string" then return nil end

   local t = dt.rfc3339:match(date)
   if not t then
      return nil
   end

   return dt.time_to_ns(t) -- The timezone of the ping has always zero UTC offset
end

function process_message()
    ------------------------------------
    -- Reset Fields, since different namespaces may use different fields.
    msg.Fields = {}

    -- Carry forward payload some incoming fields.
    msg.Timestamp = read_message("Timestamp")
    msg.EnvVersion = read_message("EnvVersion")

    -- Note: 'Hostname' is the host name of the server that received the
    -- message, while 'Host' is the name of the HTTP endpoint the client
    -- used (such as "incoming.telemetry.mozilla.org").
    msg.Hostname    = read_message("Hostname")
    msg.Fields.Host = read_message("Fields[Host]")
    msg.Fields.DNT  = read_message("Fields[DNT]")
    msg.Fields.Date = read_message("Fields[Date]")

    -- Path should be of the form:
    --     ^/submit/namespace/id[/extra/path/components]$
    local path = read_message("Fields[Path]")
    local components = split_path(path)

    -- Skip this message: Not enough path components.
    if not components or #components < 3 then
        return -1, "Not enough path components"
    end

    local submit = table.remove(components, 1)
    -- Skip this message: Invalid path prefix.
    if submit ~= "submit" then
        return -1, string.format("Invalid path prefix: '%s' in %s", submit, path)
    end

    local namespace = table.remove(components, 1)
    -- Get namespace configuration, look up params, override Logger if needed.
    local cfg = ns_config[namespace]

    -- Skip this message: Invalid namespace.
    if not cfg then
        return -1, string.format("Invalid namespace: '%s' in %s", namespace, path)
    end

    msg.Logger = namespace
    -- Override Logger if specified.
    if cfg["logger"] then msg.Logger = cfg["logger"] end

    -- This documentId is what we should use to de-duplicate submissions.
    msg.Fields.documentId = table.remove(components, 1)

    local num_components = #components
    if num_components > 0 then
        local dims = cfg["dimensions"]
        if dims ~= nil and #dims >= num_components then
            for i=1,num_components do
                msg.Fields[dims[i]] = components[i]
            end
        else
            -- Didn't have dimension spec, or had too many components.
            msg.Fields.PathComponents = components
        end
    end

    -- Insert geo info.
    local xff = read_message("Fields[X-Forwarded-For]")
    local remote_addr = read_message("Fields[RemoteAddr]")
    msg.Fields.geoCountry = get_geo_country(xff, remote_addr)

    -- Remove the PII Bugzilla 1143818
    msg.Fields["X-Forwarded-For"] = nil
    msg.Fields["RemoteAddr"] = nil
    ------------------------------------


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
    msg.Fields.sourceName = "telemetry"
    msg.Logger                  = "telemetry"
    msg.Fields.documentId       = msg.Fields.documentId       or read_message("Fields[documentId]")
    msg.Fields.docType          = msg.Fields.docType          or read_message("Fields[docType]")
    msg.Fields.appName          = msg.Fields.appName          or read_message("Fields[appName]")
    msg.Fields.appVersion       = msg.Fields.appVersion       or read_message("Fields[appVersion]")
    msg.Fields.appUpdateChannel = msg.Fields.appUpdateChannel or read_message("Fields[appUpdateChannel]")
    msg.Fields.appBuildId       = msg.Fields.appBuildId       or read_message("Fields[appBuildId]")

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
        msg.Fields.appVendor        = info.vendor
        msg.Fields.reason           = info.reason
        msg.Fields.clientId         = parsed.clientID
    elseif parsed.version then
        -- New-style telemetry, see http://mzl.la/1zobT1S

        -- pull out/verify the data/schema before any restructuring
        local app = parsed.application
        if type(app) ~= "table" then
            return -1, "missing application object"
        end

        if type(parsed.payload) == "table" and
           type(parsed.payload.info) == "table" then
            msg.Fields.reason = parsed.payload.info.reason
        end

        msg.Fields.creationTimestamp = parse_creation_date(parsed.creationDate)
        if not msg.Fields.creationTimestamp then
           return -1, "missing creationDate"
        end

        if type(parsed.environment) == "table" and
           type(parsed.environment.system) == "table" and
           type(parsed.environment.system.os) == "table" then
            msg.Fields.os = parsed.environment.system.os.name
        end

        msg.Fields.sourceVersion    = tostring(parsed.version)
        msg.Fields.docType          = parsed.type or UNK_DIM

        -- Get some more dimensions.
        msg.Fields.appName          = app.name or UNK_DIM
        msg.Fields.appVersion       = app.version or UNK_DIM
        msg.Fields.appUpdateChannel = app.channel or UNK_DIM

        -- Do not want default values for these.
        msg.Fields.appBuildId       = app.buildId
        msg.Fields.appVendor        = app.vendor
        msg.Fields.clientId         = parsed.clientId

        -- restructure the main ping message
        if parsed.type == "main" then
            split_objects(parsed.environment, "environment", environment_objects)
            split_objects(parsed.payload, "payload", main_ping_objects)
            local ok, json = pcall(cjson.encode, parsed) -- re-encode the remaining data
            if not ok then return -1, json end
            msg.Payload = json
        else
            msg.Payload = json
        end
    end

    -- Carry forward more incoming fields.
    msg.Fields.geoCountry = msg.Fields.geoCountry or read_message("Fields[geoCountry]") or UNK_GEO
    msg.Timestamp         = msg.Timestamp         or read_message("Timestamp")
    msg.Fields.Host       = msg.Fields.Host       or read_message("Fields[Host]")
    msg.Fields.DNT        = msg.Fields.DNT        or read_message("Fields[DNT]")
    msg.Fields.clientDate = msg.Fields.clientDate or read_message("Fields[Date]")

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
