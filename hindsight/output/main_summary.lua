-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Outputs a main ping summary derived stream in the specified format one table/file per day.

Config:

filename = "main_summary.lua"
message_matcher = "Type == 'telemetry' && Fields[docType] == 'main'"

format      = "redshift"
buffer_path = "/mnt/output" -- path where the temporary buffer files are stored
buffer_size = 10000 * 1024  -- size of the largest buffer before performing a multi-line insert
ts_field    = "Timestamp"   -- default

db_config = {
host = "example.com",
port = 5432,
name = "pipeline",
user = "user",
_password = "password",
}

-- OR

format = "protobuf"
output_path = "/mnt/output" -- path where the daily output files are written
ts_field    = "Timestamp"   -- default

-- OR

format = "tsv"
output_path = "/mnt/output" -- path where the daily output files are written
ts_field    = "Timestamp"   -- default
nil_value   = "NULL"        -- defaults to an empty string

--]]

local ds = require "derived_stream"
local fx = require "fx"
local ping = require "fx.ping"

local name = "main_summary"
local schema = {
--  column name                     type            length  attributes  field /function
    {"Timestamp"                    ,"TIMESTAMP"    ,nil    ,"SORTKEY"  ,"Timestamp"},
    {"subsessionDate"               ,"DATE"         ,nil    ,nil        ,function () return ping.get_date(ping.info().subsessionStartDate) end},
    {"clientId "                    ,"CHAR"         ,36     ,"DISTKEY"  ,"Fields[clientId]"},
    {"buildVersion"                 ,"VARCHAR"      ,32     ,nil        ,function () return ping.build().version end},
    {"buildId"                      ,"CHAR"         ,14     ,nil        ,function () return ping.build().buildId end},
    {"buildArchitecture"            ,"VARCHAR"      ,32     ,nil        ,function () return ping.build().architecture end},
    {"channel"                      ,"VARCHAR"      ,7      ,nil        ,function () return fx.normalize_channel(read_message("Fields[appUpdateChannel]")) end},
    {"os"                           ,"VARCHAR"      ,7      ,nil        ,function () return ping.system().os.name end},
    {"osVersion"                    ,"VARCHAR"      ,32     ,nil        ,function () return ping.system().os.version end},
    {"osServicepackMajor"           ,"VARCHAR"      ,32     ,nil        ,function () return ping.system().os.servicePackMajor end},
    {"osServicepackMinor"           ,"VARCHAR"      ,32     ,nil        ,function () return ping.system().os.servicePackMinor end},
    {"locale"                       ,"VARCHAR"      ,32     ,nil        ,function () return ping.settings().locale end},
    {"activeExperimentId"           ,"VARCHAR"      ,32     ,nil        ,function () return ping.addons().activeExperiment.id end},
    {"activeExperimentBranch"       ,"VARCHAR"      ,32     ,nil        ,function () return ping.addons().activeExperiment.branch end},
    {"country"                      ,"VARCHAR"      ,5      ,nil        ,function () return fx.normalize_country(read_message("Fields[geoCountry]")) end},
    {"reason"                       ,"VARCHAR"      ,32     ,nil        ,function () return ping.info().reason end},
    {"subsessionLength"             ,"INT"          ,nil    ,nil        ,function () return ping.info().subsessionLength end},
    {"timezoneOffset"               ,"INT"          ,nil    ,nil        ,function () return ping.info().timezoneOffset end},
    {"pluginHangs"                  ,"INT"          ,nil    ,nil        ,function () return ping.khist_sum("SUBPROCESS_CRASHES_WITH_DUMP", "pluginhang") end},
    {"abortsPlugin"                 ,"INT"          ,nil    ,nil        ,function () return ping.khist_sum("SUBPROCESS_ABNORMAL_ABORT", "plugin") end},
    {"abortsContent"                ,"INT"          ,nil    ,nil        ,function () return ping.khist_sum("SUBPROCESS_ABNORMAL_ABORT", "content") end},
    {"abortsGmplugin"               ,"INT"          ,nil    ,nil        ,function () return ping.khist_sum("SUBPROCESS_ABNORMAL_ABORT", "gmplugin") end},
    {"crashesdetectedPlugin"        ,"INT"          ,nil    ,nil        ,function () return ping.khist_sum("SUBPROCESS_CRASHES_WITH_DUMP", "plugin") end},
    {"crashesdetectedContent"       ,"INT"          ,nil    ,nil        ,function () return ping.khist_sum("SUBPROCESS_CRASHES_WITH_DUMP", "content") end},
    {"crashesdetectedGmplugin"      ,"INT"          ,nil    ,nil        ,function () return ping.khist_sum("SUBPROCESS_CRASHES_WITH_DUMP", "gmplugin") end},
    {"crashSubmitAttemptMain"       ,"INT"          ,nil    ,nil        ,function () return ping.khist_sum("PROCESS_CRASH_SUBMIT_ATTEMPT", "main-crash") end},
    {"crashSubmitAttemptContent"    ,"INT"          ,nil    ,nil        ,function () return ping.khist_sum("PROCESS_CRASH_SUBMIT_ATTEMPT", "content-crash") end},
    {"crashSubmitAttemptPlugin"     ,"INT"          ,nil    ,nil        ,function () return ping.khist_sum("PROCESS_CRASH_SUBMIT_ATTEMPT", "plugin-crash") end},
    {"crashSubmitSuccessMain"       ,"INT"          ,nil    ,nil        ,function () return ping.khist_sum("PROCESS_CRASH_SUBMIT_SUCCESS", "main-crash") end},
    {"crashSubmitSuccessContent"    ,"INT"          ,nil    ,nil        ,function () return ping.khist_sum("PROCESS_CRASH_SUBMIT_SUCCESS", "content-crash") end},
    {"crashSubmitSuccessPlugin"     ,"INT"          ,nil    ,nil        ,function () return ping.khist_sum("PROCESS_CRASH_SUBMIT_SUCCESS", "plugin-crash") end},
    {"activeAddons"                 ,"INT"          ,nil    ,nil        ,function () return ping.num_active_addons() end},
    {"flashVersion"                 ,"VARCHAR"      ,16     ,nil        ,function () return ping.flash_version() end},
}

local ds_pm
ds_pm, timer_event = ds.load_schema(name, schema)

function process_message()
    ping.clear_cache()
    return ds_pm()
end

