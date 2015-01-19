-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- see https://gist.github.com/mreid-moz/5203956e1d08b60339c0

require "cjson"
require "os"

local msg = {
Timestamp   = nil,
Type        = "fhr_metadata",
Payload     = nil,
Fields      = {}
}

local UNK_DIM = "UNKNOWN"
local UNK_GEO = "??"

function process_message()
    -- Carry forward payload
    msg.Payload = read_message("Payload")

    local ok, fhr = pcall(cjson.decode, msg.Payload)
    if not ok then return -1, fhr end

    msg.Fields.sourceVersion    = tostring(fhr.version) or UNK_DIM
    msg.Fields.sourceName       = "fhr"

    local info
    if msg.Fields.sourceVersion == "1" then
        if type(fhr.data) ~= "table" then
            return -1, "missing object: data"
        end
        if type(fhr.data.last) ~= "table" then
            return -1, "missing object: data.last"
        end
        if type(fhr.data.last["org.mozilla.appInfo.appinfo"]) == "table" then
            info = fhr.data.last["org.mozilla.appInfo.appinfo"]
        elseif type(fhr.data.last["org.mozilla.appInfo.appinfo.1"]) == "table" then
            info = fhr.data.last["org.mozilla.appInfo.appinfo.1"]
        else
            return -1, "missing object: data.last[org.mozilla.appInfo.appinfo]"
        end
    elseif msg.Fields.sourceVersion == "2" then
        if type(fhr.geckoAppInfo) ~= "table" then
            return -1, "missing object: geckoAppInfo"
        end
        info = fhr.geckoAppInfo
    elseif msg.Fields.sourceVersion == "3" then
        -- Use v3 structure.
        if type(fhr.environments) ~= "table" then
            return -1, "missing object: environments"
        end
        if type(fhr.environments.current) ~= "table" then
            return -1, "missing object: environments.current"
        end
        if type(fhr.environments.current.geckoAppInfo) ~= "table" then
            return -1, "missing object: environments.current.geckoAppInfo"
        end
        info = fhr.environments.current.geckoAppInfo
    else
        return -1, "unknown payload version"
    end

    -- Get some more dimensions
    msg.Fields.appName          = info.name or UNK_DIM
    msg.Fields.appVersion       = info.version or UNK_DIM
    msg.Fields.appUpdateChannel = info.updateChannel or UNK_DIM

    -- Do not want default values for these.
    msg.Fields.appBuildID       = info.appBuildID
    msg.Fields.os               = info.os
    msg.Fields.vendor           = info.vendor
    msg.Fields.clientID         = fhr.clientID

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
