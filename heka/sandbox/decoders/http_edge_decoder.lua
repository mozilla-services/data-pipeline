-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- See https://wiki.mozilla.org/CloudServices/DataPipeline/HTTPEdgeServerSpecification

require "cjson"
require "lpeg"
require "string"
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

function get_geo_country()
    local country
    local ipaddr = read_message("Fields[X-Forwarded-For]")
    if ipaddr then
        country = city_db:query_by_addr(ipaddr, "country_code")
    end
    if country then return country end
    ipaddr = read_message("Fields[RemoteAddr]")
    if ipaddr then
        country = city_db:query_by_addr(ipaddr, "country_code")
    end
    return country or UNK_GEO
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
    Payload    = nil,
    EnvVersion = nil,
    Severity   = nil,
    Fields     = nil
}

-- This is the modified message that knows about namespaces and so forth.
local main_msg = {
    Timestamp   = nil,
    Type        = "http_edge_incoming",
    Payload     = nil,
    EnvVersion  = nil,
    Hostname    = nil,
    Fields      = nil
}

function process_message()
    -- First, copy the current message as-is.
    landfill_msg.Timestamp  = read_message("Timestamp")
    landfill_msg.Type       = read_message("Type")
    landfill_msg.Hostname   = read_message("Hostname")
    landfill_msg.Pid        = read_message("Timestamp")
    -- UUID is auto-generated and meaningless anyways
    landfill_msg.Logger     = read_message("Logger")
    landfill_msg.Payload    = read_message("Payload")
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
    inject_message(landfill_msg)

    -- Reset Fields, since different namespaces may use different fields.
    main_msg.Fields = {}

    -- Carry forward payload some incoming fields.
    main_msg.Payload = read_message("Payload")
    main_msg.Timestamp = read_message("Timestamp")
    main_msg.EnvVersion = read_message("EnvVersion")

    -- Hostname is the host name of the server that received the message.
    main_msg.Hostname = read_message("Hostname")

    -- Host is the name of the HTTP endpoint the client used (such as
    -- "incoming.telemetry.mozilla.org").
    main_msg.Fields.Host = read_message("Fields[Host]")

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

    main_msg.Logger = namespace
    local dataLength = string.len(main_msg.Payload)
    -- Skip this message: Payload too large.
    if dataLength > cfg.max_data_length then
        return -1, string.format("Payload too large: %d > %d", dataLength, cfg.max_data_length)
    end

    local pathLength = string.len(path)
    -- Skip this message: Path too long.
    if pathLength > cfg.max_path_length then
        return -1, string.format("Path too long: %d > %d", pathLength, cfg.max_path_length)
    end
    -- Override Logger if specified.
    if cfg["logger"] then main_msg.Logger = cfg["logger"] end

    -- This DocumentID is what we should use to de-duplicate submissions.
    main_msg.Fields.DocumentID = table.remove(components, 1)

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

    -- Insert geo info.
    main_msg.Fields.geoCountry = get_geo_country()

    -- Send new message along.
    inject_message(main_msg)
    return 0
end
