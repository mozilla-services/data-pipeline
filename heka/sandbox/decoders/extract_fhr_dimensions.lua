-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
The decoder extracts the FHR partition dimensions from the large JSON payload
and adds them as message fields to avoid additional down stream parsing; it also
uses an IP address lookup to determine the submission's country of origin and
adds it a as a message field.

Config:

- geoip_city_db (string)
    The fully qualified path to the GeoIP city database (if not in the default
    location).

*Example Heka Configuration*

.. code-block:: ini

    [FHRDecoder]
    type = "SandboxDecoder"
    filename = "extract_fhr_dimensions.lua"
    memory_limit = 30000000
    output_limit = 2097152

        # Default
        # [FHRDecoder.config]
        # geoip_city_db = "/usr/local/share/GeoIP/GeoIPCity.dat"

*Example Heka Message*

:Timestamp: 2014-07-19 17:23:35.060999936 +0000 UTC
:Type: fhr_metadata
:Hostname: ip-10-227-137-43
:Pid: 0
:Uuid: 2dfcbeb8-18d4-41b8-af50-aa055fd94831
:Logger: fhr
:Payload: {...}
:EnvVersion:
:Severity: 7
:Fields:
    | name:"submissionDate" type:string value:"20140719"
    | name:"appVersion" type:string value:"30.0"
    | name:"appUpdateChannel" type:string value:"release"
    | name:"sourceVersion" type:string value:"2"
    | name:"clientID" type:string value:"a6d35999-2d8d-4c68-9c6b-fbe8c514e40e"
    | name:"os" type:string value:"Linux"
    | name:"geoCountry" type:string value:"GB"
    | name:"sourceName" type:string value:"fhr"
    | name:"vendor" type:string value:"Mozilla"
    | name:"appBuildID" type:string value:"20140608211622"
    | name:"appName" type:string value:"Firefox"
--]]

require "cjson"
require 'geoip.city'
require "os"

local city_db = assert(geoip.city.open(read_config("geoip_city_db")))

local msg = {
Timestamp   = nil,
Type        = "fhr_metadata",
Payload     = nil,
Fields      = { sourceName = "fhr" }
}

local UNK_DIM = "UNKNOWN"
local UNK_GEO = "??"

function process_message()
    -- Carry forward payload
    msg.Payload = read_message("Payload")

    local ok, fhr = pcall(cjson.decode, msg.Payload)
    if not ok then return -1, fhr end

    msg.Fields.sourceVersion    = tostring(fhr.version) or UNK_DIM

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

    -- IP address lookup
    msg.Fields.geoCountry = city_db:query_by_addr(read_message("Fields[remote_addr]"), "country_code") or UNK_GEO

    -- Carry forward timestamp.
    msg.Timestamp = read_message("Timestamp")

    msg.Fields.submissionDate = os.date("%Y%m%d", msg.Timestamp / 1e9)

    -- Send new message along
    inject_message(msg)

    return 0
end
