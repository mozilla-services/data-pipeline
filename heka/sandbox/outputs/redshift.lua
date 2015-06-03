-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Example Redshift database message loader. The plugin includes table setup,
message transformation and file buffering to improve performance by bulk
loading data.  The bulk load will be triggered by a full buffer or by the
flush_interval.

The output will fail to start if the Redshift cluster is unavailable.  If the
Redshift cluster becomes unavailable while this plugin is running it will
backpressure Heka causing all data flowing through Heka to pause.  Ideally
all output plugins should be run in a separate instance of Heka and read from a
persistent store (file, Kafka, etc) to avoid impacting any core monitoring
operations.

Config:

- db_name (string)
- db_user (string)
- db_password (string)
- db_host (string)
- db_port (uint)
- buffer_file (string)
    Path to the buffer file
- buffer_size (uint, optional, default 10240000)
    File size, in bytes, of table inserts to buffer before bulk loading.
- flush_interval (uint, optional, default 60)
    The maximum amount of time in seconds between bulk load operations.  This
    should match the plugin ticker_interval configuration.

*Example Heka Configuration*

.. code-block:: ini

    [RedshiftOutput]
    type = "SandboxOutput"
    filename = "redshift.lua"
    message_matcher = "Type == 'logfile'"
    memory_limit = 60000000
    ticker_interval = 60

        [RedshiftOutput.config]
        db_name = "dev"
        db_user = "testuser"
        db_password = "testuserpw"
        db_host = "foobar.redshift.amazonaws.com"
        db_port = 5439
        buffer_file = "/var/tmp/redshift.insert"
        flush_interval = 60
--]]

require "io"
require "os"
require "string"
require "table"

local driver = require "luasql.postgres"

-- db setup
local db_name     = read_config("db_name") or error("db_name must be set")
local db_user     = read_config("db_user") or error("db_user must be set")
local db_password = read_config("db_password") or error("db_password must be set")
local db_host     = read_config("db_host") or error("db_host must be set")
local db_port     = read_config("db_port") or error("db_port must be set")
local buffer_file = read_config("buffer_file") or error("buffer_file must be set")

local buffer_size = read_config("buffer_size") or 10000 * 1024
assert(buffer_size > 0, "buffer_size must be greater than zero")

local flush_interval = read_config("flush_interval") or 60
assert(flush_interval >= 0, "flush_interval must be greater than or equal to zero")
flush_interval = flush_interval * 1e9

local env = assert (driver.postgres())
local con, err = env:connect(db_name, db_user, db_password, db_host, db_port)

local table_name = "telemetry_sample_42"
MAX_LENGTH = 65535
local columns = {
--   column name                   field name                            field type   field length
    {"msg_Timestamp",              "Timestamp",                          "TIMESTAMP", nil},
    {"sourceName",                 "Fields[sourceName]",                 "VARCHAR",   30},
    {"sourceVersion",              "Fields[sourceVersion]",              "VARCHAR",   12},
    {"submissionDate",             "Fields[submissionDate]",             "DATE",      nil},
    {"creationTimestamp",          "Fields[creationTimestamp]",          "TIMESTAMP", nil},
    {"geoCountry",                 "Fields[geoCountry]",                 "VARCHAR",   2},
    {"documentId",                 "Fields[documentId]",                 "VARCHAR",   36},
    {"reason",                     "Fields[reason]",                     "VARCHAR",   100},
    {"os",                         "Fields[os]",                         "VARCHAR",   100},
    {"docType",                    "Fields[docType]",                    "VARCHAR",   50},
    {"appName",                    "Fields[appName]",                    "VARCHAR",   100},
    {"appVersion",                 "Fields[appVersion]",                 "VARCHAR",   30},
    {"appUpdateChannel",           "Fields[appUpdateChannel]",           "VARCHAR",   30},
    {"appBuildId",                 "Fields[appBuildId]",                 "VARCHAR",   30},
    {"appVendor",                  "Fields[appVendor]",                  "VARCHAR",   30},
    {"clientId",                   "Fields[clientId]",                   "VARCHAR",   100},
    {"sampleId",                   "Fields[sampleId]",                   "SMALLINT",  nil},
    {"environment_addons",         "Fields[environment.addons]",         "VARCHAR",   MAX_LENGTH},
    {"environment_build",          "Fields[environment.build]",          "VARCHAR",   MAX_LENGTH},
    {"environment_partner",        "Fields[environment.partner]",        "VARCHAR",   MAX_LENGTH},
    {"environment_profile",        "Fields[environment.profile]",        "VARCHAR",   MAX_LENGTH},
    {"environment_settings",       "Fields[environment.settings]",       "VARCHAR",   MAX_LENGTH},
    {"environment_system",         "Fields[environment.system]",         "VARCHAR",   MAX_LENGTH},
    {"payload",                    "Payload",                            "VARCHAR",   MAX_LENGTH},
    {"payload_addonDetails",       "Fields[payload.addonDetails]",       "VARCHAR",   MAX_LENGTH},
    {"payload_addonHistograms",    "Fields[payload.addonHistograms]",    "VARCHAR",   MAX_LENGTH},
    {"payload_childPayloads",      "Fields[payload.childPayloads]",      "VARCHAR",   MAX_LENGTH},
    {"payload_chromeHangs",        "Fields[payload.chromeHangs]",        "VARCHAR",   MAX_LENGTH},
    {"payload_fileIOReports",      "Fields[payload.fileIOReports]",      "VARCHAR",   MAX_LENGTH},
    {"payload_histograms",         "Fields[payload.histograms]",         "VARCHAR",   MAX_LENGTH},
    {"payload_info",               "Fields[payload.info]",               "VARCHAR",   MAX_LENGTH},
    {"payload_keyedHistograms",    "Fields[payload.keyedHistograms]",    "VARCHAR",   MAX_LENGTH},
    {"payload_lateWrites",         "Fields[payload.lateWrites]",         "VARCHAR",   MAX_LENGTH},
    {"payload_log",                "Fields[payload.log]",                "VARCHAR",   MAX_LENGTH},
    {"payload_simpleMeasurements", "Fields[payload.simpleMeasurements]", "VARCHAR",   MAX_LENGTH},
    {"payload_slowSQL",            "Fields[payload.slowSQL]",            "VARCHAR",   MAX_LENGTH},
    {"payload_slowSQLstartup",     "Fields[payload.slowSQLstartup]",     "VARCHAR",   MAX_LENGTH},
    {"payload_threadHangStats",    "Fields[payload.threadHangStats]",    "VARCHAR",   MAX_LENGTH},
    {"payload_UIMeasurements",     "Fields[payload.UIMeasurements]",     "VARCHAR",   MAX_LENGTH},
    {"http_DNT",                   "Fields[DNT]",                        "BOOLEAN",   nil},
    {"http_Date",                  "Fields[Date]",                       "TIMESTAMP", nil}
}

function make_create_table()
    local pieces = {"CREATE TABLE IF NOT EXISTS ", table_name, " ("}
    for i, c in ipairs(columns) do
        table.insert(pieces, c[1])
        table.insert(pieces, " ")
        table.insert(pieces, c[3])
        if c[4] ~= nil then
            table.insert(pieces, "(")
            table.insert(pieces, c[4])
            table.insert(pieces, ")")
        end
        if c[4] == MAX_LENGTH then
            table.insert(pieces, " ENCODE LZO")
        end
        if i < #columns then
            table.insert(pieces, ", ")
        end
    end
    table.insert(pieces, ")")
    return table.concat(pieces)
end

assert (con, err)
assert (con:execute(make_create_table()))

-- file buffer setup
local sep = " "

local function open_file_buffer(mode)
    local f, e = io.open(buffer_file, mode)
    if f then
        f:setvbuf("no")
        if f:seek("end") == 0 then
            sep = " "
        else
            sep = ","
        end
    end
    return f, e
end

local last_flush = 0
local fh, err = open_file_buffer("a+") -- open it for append since we may have data remaining from the last shutdown
assert(fh, err)

local function bulk_load()
    if fh:seek("end") == 0 then
        return
    end

    fh:seek("set")
    local cnt, err = con:execute(fh:read("*a")) -- read the entire file and execute the query
    if cnt then
        last_flush = os.time() * 1e9
        fh:close()
        fh, err = open_file_buffer("w+") -- reset the file
        assert(fh, err)
    else
        error("bulk load failed: " .. err)
    end
end

function esc_str(v)
    if v == nil then
        return "NULL"
    end
    if type(v) ~= "string" then
        v = tostring(v)
    end
    if string.len(v) > MAX_LENGTH then
        v = "TRUNCATED:" .. string.sub(v, 1, MAX_LENGTH - 10)
    end

    -- Occasionally con:escape(v) returns nil here. Not sure why.
    local escd = con:escape(v)
    if escd == nil then
        return "NULL"
    end
    return table.concat({"'", escd, "'"})
end

function esc_num(v)
    if v == nil then
        return "NULL"
    end
    if type(v) ~= "number" then
        return esc_str(v)
    end
    return tostring(v)
end

function esc_ts(v)
    if v == nil then
        return "NULL"
    end
    if type(v) ~= "number" then
        return esc_str(v)
    end
    local seconds = v / 1e9
    return table.concat({"(TIMESTAMP 'epoch' + ", seconds, " * INTERVAL '1 seconds')"})
end

function make_insert()
    local pieces = {sep, "("}
    for i=1,#columns do
        if i > 1 then
            table.insert(pieces, ",")
        end
        local col = columns[i]
        if col[3] == "TIMESTAMP" then
            table.insert(pieces, esc_ts(read_message(col[2])))
        elseif col[3] == "SMALLINT" then
            table.insert(pieces, esc_num(read_message(col[2])))
        else
            table.insert(pieces, esc_str(read_message(col[2])))
        end
    end
    table.insert(pieces, ")")
    return table.concat(pieces)
end

-- plugin interfaces
function process_message()
    if sep == " " then
        fh:write(table.concat({"INSERT INTO ", table_name, " VALUES"}))
    end
    fh:write(make_insert())
    sep = ","

    if fh:seek("end") >= buffer_size then
        bulk_load()
    end

    return 0
end

function timer_event(ns)
    if ns - last_flush >= flush_interval then
        bulk_load()
    end
end
