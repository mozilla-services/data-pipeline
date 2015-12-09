-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- See https://wiki.mozilla.org/CloudServices/DataPipeline/HTTPEdgeServerSpecification

require "cjson"
require "lpeg"
require "os"
require "string"
require "table"
require "math"
require "geoip.city"

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
-- Track the hour to facilitate reopening city_db hourly.
local hour = math.floor(os.time() / 3600)

function get_geo_field(xff, remote_addr, field_name, default_value)
    local geo
    if xff then
        local first_addr = string.match(xff, "([^, ]+)")
        if first_addr then
            geo = city_db:query_by_addr(first_addr, field_name)
        end
    end
    if geo then return geo end
    if remote_addr then
        geo = city_db:query_by_addr(remote_addr, field_name)
    end
    return geo or default_value
end

function get_geo_country(xff, remote_addr)
    return get_geo_field(xff, remote_addr, "country_code", UNK_GEO)
end

function get_geo_country(xff, remote_addr)
    return get_geo_field(xff, remote_addr, "city", UNK_GEO)
end

-- Load the namespace configuration externally.
-- Note that if the config contains invalid JSON, we will discard any messages
-- we receive with the following error:
--    FATAL: process_message() function was not found
local ok, ns_config = pcall(cjson.decode, read_config("namespace_config"))
if not ok then return -1, ns_config end

-- This is a copy of the raw message that will be passed through as-is.
local landfill_msg = {
    Timestamp  = nil,
    Type       = nil,
    Hostname   = nil,
    Pid        = nil,
    Logger     = nil,
    EnvVersion = nil,
    Severity   = nil,
    Fields     = nil
}

-- This is the modified message that knows about namespaces and so forth.
local main_msg = {
    Timestamp   = nil,
    Type        = "http_edge_incoming",
    EnvVersion  = nil,
    Hostname    = nil,
    Fields      = nil
}

function process_message()
    -- First, copy the current message as-is.
    landfill_msg.Timestamp  = read_message("Timestamp")
    landfill_msg.Type       = read_message("Type")
    landfill_msg.Hostname   = read_message("Hostname")
    landfill_msg.Pid        = read_message("Pid")
    -- UUID is auto-generated and meaningless anyways
    landfill_msg.Logger     = read_message("Logger")
    landfill_msg.EnvVersion = read_message("EnvVersion")
    landfill_msg.Severity   = read_message("Severity")
    -- Now copy the fields:
    landfill_msg.Fields = {}
    while true do
        local value_type, name, value, representation, count = read_next_field()
        if not name then break end
        -- Keep the first occurence only (we want the value supplied by the
        -- HttpListenInput, not the user-supplied one if we have to choose).
        if not landfill_msg.Fields[name] then
            landfill_msg.Fields[name] = value
        end
    end
    landfill_msg.Fields.submissionDate = os.date("%Y%m%d", landfill_msg.Timestamp / 1e9)
    landfill_msg.Fields.submission = {value = read_message("Payload"), value_type = 1} -- Bugzilla 1204589

    -- Reopen city_db once an hour.
    local current_hour = math.floor(os.time() / 3600)
    if current_hour > hour then
        city_db:close()
        city_db = assert(geoip.city.open(read_config("geoip_city_db")))
        hour = current_hour
    end
    -- Insert geo info.
    local xff = landfill_msg.Fields["X-Forwarded-For"]
    local remote_addr = landfill_msg.Fields["RemoteAddr"]
    landfill_msg.Fields.geoCountry = get_geo_country(xff, remote_addr)
    landfill_msg.Fields.geoCity = get_geo_city(xff, remote_addr)

    -- Remove the PII Bugzilla 1143818
    landfill_msg.Fields["X-Forwarded-For"] = nil
    landfill_msg.Fields["RemoteAddr"] = nil

    local landfill_status, landfill_err = pcall(inject_message, landfill_msg)
    if not landfill_status then
        return -1, landfill_err
    end

    -- Reset Fields, since different namespaces may use different fields.
    main_msg.Fields = {}

    -- Carry forward some incoming headers.
    main_msg.Timestamp = landfill_msg.Timestamp
    main_msg.EnvVersion = landfill_msg.EnvVersion

    -- Note: 'Hostname' is the host name of the server that received the
    -- message, while 'Host' is the name of the HTTP endpoint the client
    -- used (such as "incoming.telemetry.mozilla.org").
    main_msg.Hostname              = landfill_msg.Hostname
    main_msg.Fields.Host           = landfill_msg.Fields.Host
    main_msg.Fields.DNT            = landfill_msg.Fields.DNT
    main_msg.Fields.Date           = landfill_msg.Fields.Date
    main_msg.Fields.geoCountry     = landfill_msg.Fields.geoCountry
    main_msg.Fields.submission     = landfill_msg.Fields.submission
    main_msg.Fields.submissionDate = landfill_msg.Fields.submissionDate

    -- Path should be of the form:
    --     ^/submit/namespace/id[/extra/path/components]$
    local path = landfill_msg.Fields.Path
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

    main_msg.Logger = namespace
    local dataLength = string.len(main_msg.Fields.submission.value)
    -- Skip this message: submission too large.
    if dataLength > cfg.max_data_length then
        return -1, string.format("submission data too large: %d > %d", dataLength, cfg.max_data_length)
    end

    local pathLength = string.len(path)
    -- Skip this message: Path too long.
    if pathLength > cfg.max_path_length then
        return -1, string.format("Path too long: %d > %d", pathLength, cfg.max_path_length)
    end
    -- Override Logger if specified.
    if cfg["logger"] then main_msg.Logger = cfg["logger"] end

    -- This documentId is what we should use to de-duplicate submissions.
    main_msg.Fields.documentId = table.remove(components, 1)

    local num_components = #components
    if num_components > 0 then
        local dims = cfg["dimensions"]
        if dims ~= nil and #dims >= num_components then
            for i=1,num_components do
                main_msg.Fields[dims[i]] = components[i]
            end
        else
            -- Didn't have dimension spec, or had too many components.
            main_msg.Fields.PathComponents = components
        end
    end

    -- Send new message along.
    local main_status, main_err = pcall(inject_message, main_msg)
    if not main_status then
        return -1, main_err
    end

    return 0
end
