-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Outputs a crash ping summary derived stream in the specified format one table/file per day.

Config:

filename = "crash_summary.lua"
message_matcher = "Type == 'telemetry' && Fields[docType] == 'crash'"

format      = "redshift.psv"
buffer_path = "/mnt/output"
buffer_size = 20 * 1024 * 1024
s3_path     = "s3://test"

--]]

local ds = require "derived_stream"
local fx = require "fx"
local ping = require "fx.ping"

local name = "crash_summary"
local schema = {
--  column name                 type            length  attributes  field /function
    {"Timestamp"                ,"TIMESTAMP"    ,nil    ,"SORTKEY"  ,"Timestamp"},
    {"crashDate"                ,"DATE"         ,nil    ,nil        ,function () return ping.get_date(ping.payload().payload.crashDate) end},
    {"clientId"                 ,"CHAR"         ,36     ,"DISTKEY"  ,"Fields[clientId]"},
    {"buildVersion"             ,"VARCHAR"      ,32     ,nil        ,function () return ping.build().version end},
    {"buildId"                  ,"CHAR"         ,14     ,nil        ,function () return ping.build().buildId end},
    {"buildArchitecture"        ,"VARCHAR"      ,32     ,nil        ,function () return ping.build().architecture end},
    {"channel"                  ,"VARCHAR"      ,7      ,nil        ,function () return fx.normalize_channel(read_message("Fields[appUpdateChannel]")) end},
    {"os"                       ,"VARCHAR"      ,7      ,nil        ,function () return ping.system().os.name end},
    {"osVersion"                ,"VARCHAR"      ,32     ,nil        ,function () return ping.system().os.version end},
    {"osServicepackMajor"       ,"VARCHAR"      ,32     ,nil        ,function () return ping.system().os.servicePackMajor end},
    {"osServicepackMinor"       ,"VARCHAR"      ,32     ,nil        ,function () return ping.system().os.servicePackMinor end},
    {"locale"                   ,"VARCHAR"      ,32     ,nil        ,function () return ping.settings().locale end},
    {"activeExperimentId"       ,"VARCHAR"      ,32     ,nil        ,function () return ping.addons().activeExperiment.id end},
    {"activeExperimentBranch"   ,"VARCHAR"      ,32     ,nil        ,function () return ping.addons().activeExperiment.branch end},
    {"country"                  ,"VARCHAR"      ,5      ,nil        ,function () return fx.normalize_country(read_message("Fields[geoCountry]")) end},
    {"hasCrashEnvironment"      ,"BOOLEAN"      ,nil    ,nil        ,function () return ping.payload().payload.hasCrashEnvironment end},
}

local ds_pm
ds_pm, timer_event = ds.load_schema(name, schema)

function process_message()
    ping.clear_cache()
    return ds_pm()
end

