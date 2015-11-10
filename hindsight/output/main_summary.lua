-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Outputs a main ping summary derived stream in the specified format one table/file per day.

Config:

filename = "main_summary.lua"
message_matcher = "Type == 'telemetry' && Fields[docType] == 'main'"

format      = "redshift.psv"
buffer_path = "/mnt/output"
buffer_size = 100 * 1024 * 1024
s3_path     = "s3://test"

--]]

local ds = require "derived_stream"
local fx = require "fx"
local ping = require "fx.ping"

local name = "main_summary"
local schema = {
--  column name                     type            length  attributes  field /function
    {"Timestamp"                    ,"TIMESTAMP"    ,nil    ,"SORTKEY"  ,"Timestamp"},
    {"subsessionDate"               ,"DATE"         ,nil    ,nil        ,function () return ping.get_date(ping.info().subsessionStartDate) end},
    {"clientId"                     ,"CHAR"         ,36     ,"DISTKEY"  ,"Fields[clientId]"},
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
    {"subsessionLength"             ,"INTEGER"      ,nil    ,nil        ,function () return ping.info().subsessionLength end},
    {"timezoneOffset"               ,"INTEGER"      ,nil    ,nil        ,function () return ping.info().timezoneOffset end},
    {"pluginHangs"                  ,"INTEGER"      ,nil    ,nil        ,function () return ping.khist_sum("SUBPROCESS_CRASHES_WITH_DUMP", "pluginhang") end},
    {"abortsPlugin"                 ,"INTEGER"      ,nil    ,nil        ,function () return ping.khist_sum("SUBPROCESS_ABNORMAL_ABORT", "plugin") end},
    {"abortsContent"                ,"INTEGER"      ,nil    ,nil        ,function () return ping.khist_sum("SUBPROCESS_ABNORMAL_ABORT", "content") end},
    {"abortsGmplugin"               ,"INTEGER"      ,nil    ,nil        ,function () return ping.khist_sum("SUBPROCESS_ABNORMAL_ABORT", "gmplugin") end},
    {"crashesdetectedPlugin"        ,"INTEGER"      ,nil    ,nil        ,function () return ping.khist_sum("SUBPROCESS_CRASHES_WITH_DUMP", "plugin") end},
    {"crashesdetectedContent"       ,"INTEGER"      ,nil    ,nil        ,function () return ping.khist_sum("SUBPROCESS_CRASHES_WITH_DUMP", "content") end},
    {"crashesdetectedGmplugin"      ,"INTEGER"      ,nil    ,nil        ,function () return ping.khist_sum("SUBPROCESS_CRASHES_WITH_DUMP", "gmplugin") end},
    {"crashSubmitAttemptMain"       ,"INTEGER"      ,nil    ,nil        ,function () return ping.khist_sum("PROCESS_CRASH_SUBMIT_ATTEMPT", "main-crash") end},
    {"crashSubmitAttemptContent"    ,"INTEGER"      ,nil    ,nil        ,function () return ping.khist_sum("PROCESS_CRASH_SUBMIT_ATTEMPT", "content-crash") end},
    {"crashSubmitAttemptPlugin"     ,"INTEGER"      ,nil    ,nil        ,function () return ping.khist_sum("PROCESS_CRASH_SUBMIT_ATTEMPT", "plugin-crash") end},
    {"crashSubmitSuccessMain"       ,"INTEGER"      ,nil    ,nil        ,function () return ping.khist_sum("PROCESS_CRASH_SUBMIT_SUCCESS", "main-crash") end},
    {"crashSubmitSuccessContent"    ,"INTEGER"      ,nil    ,nil        ,function () return ping.khist_sum("PROCESS_CRASH_SUBMIT_SUCCESS", "content-crash") end},
    {"crashSubmitSuccessPlugin"     ,"INTEGER"      ,nil    ,nil        ,function () return ping.khist_sum("PROCESS_CRASH_SUBMIT_SUCCESS", "plugin-crash") end},
    {"activeAddons"                 ,"INTEGER"      ,nil    ,nil        ,function () return ping.num_active_addons() end},
    {"flashVersion"                 ,"VARCHAR"      ,16     ,nil        ,function () return ping.flash_version() end},
}

local ds_pm
ds_pm, timer_event = ds.load_schema(name, schema)

function process_message()
    ping.clear_cache()
    return ds_pm()
end

