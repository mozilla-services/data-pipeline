-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Outputs a executive summary based on the main and crash pings as a derived stream
in the specified format one table/file per day.

Config:

filename = "executive_summary.lua"
message_matcher = "Logger == 'fx' && Type == 'executive_summary'"

format      = "redshift.psv"
buffer_path = "/mnt/output"
buffer_size = 20 * 1024 * 1024
s3_path     = "s3://test"

--]]

local ds = require "derived_stream"
local name = "executive_summary"
local schema = {
--  column name                     type                length  attributes  field /function
    {"Timestamp"                    ,"TIMESTAMP"        ,nil    ,"SORTKEY"  ,"Timestamp"},
    {"activityTimestamp"            ,"TIMESTAMP"        ,nil    ,nil        ,"Fields[activityTimestamp]"},
    {"profileCreationTimestamp"     ,"TIMESTAMP"        ,nil    ,nil        ,"Fields[profileCreationTimestamp]"},
    {"buildId"                      ,"CHAR"             ,14     ,nil        ,"Fields[buildId]"},
    {"clientId"                     ,"CHAR"             ,36     ,"DISTKEY"  ,"Fields[clientId]"},
    {"documentId"                   ,"CHAR"             ,36     ,nil        ,"Fields[documentId]"},
    {"docType"                      ,"CHAR"             ,36     ,nil        ,"Fields[docType]"},
    {"country"                      ,"VARCHAR"          ,5      ,nil        ,"Fields[country]"},
    {"channel"                      ,"VARCHAR"          ,7      ,nil        ,"Fields[channel]"},
    {"os"                           ,"VARCHAR"          ,7      ,nil        ,"Fields[os]"},
    {"osVersion"                    ,"VARCHAR"          ,32     ,nil        ,"Fields[osVersion]"},
    {"app"                          ,"VARCHAR"          ,32     ,nil        ,"Fields[app]"},
    {"version"                      ,"VARCHAR"          ,32     ,nil        ,"Fields[version]"},
    {"vendor"                       ,"VARCHAR"          ,32     ,nil        ,"Fields[vendor]"},
    {"reason"                       ,"VARCHAR"          ,32     ,nil        ,"Fields[reason]"},
    {'"default"'                    ,"BOOLEAN"          ,nil    ,nil        ,"Fields[default]"},
    {"hours"                        ,"DOUBLE PRECISION" ,nil    ,nil        ,"Fields[hours]"},
    {"google"                       ,"INTEGER"          ,nil    ,nil        ,"Fields[google]"},
    {"bing"                         ,"INTEGER"          ,nil    ,nil        ,"Fields[bing]"},
    {"yahoo"                        ,"INTEGER"          ,nil    ,nil        ,"Fields[yahoo]"},
    {"other"                        ,"INTEGER"          ,nil    ,nil        ,"Fields[other]"},
}

process_message, timer_event = ds.load_schema(name, schema)
