-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- See https://wiki.mozilla.org/CloudServices/DataPipeline/HTTPEdgeServerSpecification

require "lpeg"
require "string"
require "table"
require 'geoip.city'

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

local msg = {
    Timestamp   = nil,
    Type        = "http_edge_incoming",
    Payload     = nil,
    Fields      = {}
}

-- TODO: Load the namespace configuration from an external source.
local ns_config = {
    telemetry = {
        max_data_length = 204800,
        max_path_length = 10240,
        dimensions = {"reason", "appName", "appVersion", "appUpdateChannel", "appBuildID"},
    },
}

function process_message()
    -- Carry forward payload some incoming fields.
    msg.Payload = read_message("Payload")
    msg.Timestamp = read_message("Timestamp")
    msg.EnvVersion = read_message("EnvVersion")

    -- Hostname is the host name of the server that received the message.
    msg.Hostname = read_message("Hostname")

    -- Host is the name of the HTTP endpoint the client used (such as
    -- "incoming.telemetry.mozilla.org").
    msg.Fields.Host = read_message("Fields[Host]")

    -- Path should be of the form:
    --     ^/submit/namespace/id[/extra/path/components]$
    local path = read_message("Fields[Path]")
    local components = split_path(path)

    -- Skip this message: Not enough path components.
    if #components < 3 then
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
    local dataLength = string.len(msg.Payload)
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
    if cfg["logger"] then msg.Logger = cfg["logger"] end

    -- This DocumentID is what we should use to de-duplicate submissions.
    msg.Fields.DocumentID = table.remove(components, 1)

    local num_components = #components
    if num_components > 0 then
        local dims = cfg["dimensions"]
        if dims ~= nil and table.getn(dims) >= num_components then
            for i=1,num_components do
                msg.Fields[dims[i]] = components[i]
            end
        else
            -- Didn't have dimension spec, or had too many components.
            msg.Fields.PathComponents = components
        end
    end

    -- Insert geo info.
    msg.Fields.geoCountry = get_geo_country()

    -- Send new message along.
    inject_message(msg)
    return 0
end
