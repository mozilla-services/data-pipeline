-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Outputs a executive summary based on the main and crash pings as a derived stream
in the specified format one table/file per day.

Config:

filename = "executive_summary_full.lua"
message_matcher = "Type == 'telemetry' && (Fields[docType] == 'main' || Fields[docType] == 'crash')"

format      = "redshift.psv"
buffer_path = "/mnt/output"
buffer_size = 20 * 1024 * 1024
s3_path     = "s3://test"

--]]

local ds = require "derived_stream"
local fx = require "fx"
local ping = require "fx.ping"
require "string"

local doc_type
local search_counts

local function get_activity_timestamp()
    local ts
    if doc_type == "main" then
        ts = ping.get_timestamp(ping.info().subsessionStartDate)
    else
        ping.get_timestamp(ping.payload().payload.crashDate)
    end

    if not ts then
        ts = read_message("Fields[creationTimestamp]")
    end
    return ts
end

local function get_search_counts()
    local cnts = {0, 0, 0, 0}
    local sc = ping.khist().SEARCH_COUNTS
    if type(sc) ~= "table" then return cnts end

    for k, v in pairs(sc) do
        for i, e in ipairs({"[Gg]oogle", "[Bb]ing", "[Yy]ahoo", "."}) do
            if string.match(k, e) then
                if type(v.sum) == "number" then
                    cnts[i] = cnts[i] + v.sum
                end
                break
            end
        end
    end
    return cnts
end

local name = "executive_summary"
local schema = {
--  column name                     type                length  attributes  field /function
    {"Timestamp"                    ,"TIMESTAMP"        ,nil    ,"SORTKEY"  ,"Timestamp"},
    {"activityTimestamp"            ,"TIMESTAMP"        ,nil    ,nil        ,get_activity_timestamp},
    {"profileCreationTimestamp"     ,"TIMESTAMP"        ,nil    ,nil        ,ping.profile_creation_timestamp},
    {"buildId"                      ,"CHAR"             ,14     ,nil        ,"Fields[appBuildId]"},
    {"clientId"                     ,"CHAR"             ,36     ,"DISTKEY"  ,"Fields[clientId]"},
    {"documentId"                   ,"CHAR"             ,36     ,nil        ,"Fields[documentId]"},
    {"docType"                      ,"CHAR"             ,36     ,nil        ,function () return doc_type end},
    {"country"                      ,"VARCHAR"          ,5      ,nil        ,function () return fx.normalize_country(read_message("Fields[geoCountry]")) end},
    {"channel"                      ,"VARCHAR"          ,7      ,nil        ,function () return fx.normalize_channel(read_message("Fields[appUpdateChannel]")) end},
    {"os"                           ,"VARCHAR"          ,7      ,nil        ,function () return fx.normalize_os(read_message("Fields[os]")) end},
    {"osVersion"                    ,"VARCHAR"          ,32     ,nil        ,function () return ping.system().os.version end},
    {"app"                          ,"VARCHAR"          ,32     ,nil        ,"Fields[appName]"},
    {"version"                      ,"VARCHAR"          ,32     ,nil        ,"Fields[appVersion]"},
    {"vendor"                       ,"VARCHAR"          ,32     ,nil        ,"Fields[appVendor]"},
    {"reason"                       ,"VARCHAR"          ,32     ,nil        ,"Fields[reason]"},
    {'"default"'                    ,"BOOLEAN"          ,nil    ,nil        ,ping.is_default_browser},
    {"hours"                        ,"DOUBLE PRECISION" ,nil    ,nil        ,ping.hours},
    {"google"                       ,"INTEGER"          ,nil    ,nil        ,function () return search_counts[1] end},
    {"bing"                         ,"INTEGER"          ,nil    ,nil        ,function () return search_counts[2] end},
    {"yahoo"                        ,"INTEGER"          ,nil    ,nil        ,function () return search_counts[3] end},
    {"other"                        ,"INTEGER"          ,nil    ,nil        ,function () return search_counts[4] end},
}

local ds_pm
ds_pm, timer_event = ds.load_schema(name, schema)

function process_message()
    ping.clear_cache()
    doc_type = read_message("Fields[docType]")
    search_counts = get_search_counts()
    return ds_pm()
end

