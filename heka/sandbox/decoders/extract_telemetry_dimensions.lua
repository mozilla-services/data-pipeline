-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- See https://gist.github.com/mreid-moz/170e5680feb5fe6c28dc

require "cjson"
require "os"

local msg = {
Timestamp   = nil,
Type        = "telemetry_metadata",
Payload     = nil,
Fields      = { sourceName = "telemetry" }
}

local UNK_DIM = "UNKNOWN"
local UNK_GEO = "??"

function process_message()
    -- Carry forward payload
    msg.Payload = read_message("Payload")

    local ok, parsed = pcall(cjson.decode, msg.Payload)
    if not ok then return -1, parsed end

    msg.Fields.sourceVersion    = tostring(parsed.ver) or UNK_DIM

    local info = parsed.info
    if type(info) ~= "table" then return -1, "missing info object" end

    -- Get some more dimensions
    msg.Fields.reason           = info.reason or UNK_DIM
    msg.Fields.appName          = info.appName or UNK_DIM
    msg.Fields.appVersion       = info.appVersion or UNK_DIM
    msg.Fields.appUpdateChannel = info.appUpdateChannel or UNK_DIM

    -- Do not want default values for these.
    msg.Fields.appBuildID       = info.appBuildID
    msg.Fields.os               = info.OS
    msg.Fields.vendor           = info.vendor
    msg.Fields.clientID         = parsed.clientID

    -- Carry forward geolocation (country, at least)
    local ok, geo = pcall(cjson.decode, read_message("Fields[geoip]"))
    msg.Fields.geoCountry = UNK_GEO
    if ok then
        -- extract country
        msg.Fields.geoCountry = geo.countrycode or UNK_GEO
    end

    -- Carry forward timestamp.
    msg.Timestamp = read_message("Timestamp")

    msg.Fields.submissionDate = os.date("%Y%m%d", msg.Timestamp / 1e9)

    -- Send new message along
    inject_message(msg)

    return 0
end
