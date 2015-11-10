-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[

-- Schema
See output/main_summary and output/crash_summary.lua for examples.

The schema is a lua table consisting of five columns:
1) column name - The name of the field in the output.
   For protobuf output if it exactly matches a message header name the header
   variable will is used otherwise it is added in the message Fields table.
2) type - http://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
3) length - Maximum length for string fields (nil for everything else)
4) attributes - http://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html?tag=duckduckgo-d-20
5) field /function - If this is a string the data is retrieved from read_message(field) otherwise the provided
   function is invoked and its return value is used for the column data.

-- Hindsight Configuration Examples

format      = "redshift.sql"
buffer_path = "/mnt/output" -- path where the temporary buffer files are stored
buffer_size = 10000 * 1000  -- size of the largest buffer before performing a multi-line insert max 16MB - 8KB
ts_field    = "Timestamp"   -- default

db_config = {
    host = "example.com",
    port = 5432,
    keepalives = 1,
    keepalives_idle = 30,
    dbname = "pipeline",
    user = "user",
    _password = "password",
}

-- OR

format      = "redshift.psv"
buffer_path = "/mnt/output"
buffer_size = 100 * 1024 * 1024 -- max 1GiB
s3_path     = "s3://test"

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

local M = {}
local assert    = assert
local error     = error
local pairs     = pairs
local pcall     = pcall
local require   = require
local setfenv   = setfenv
local tonumber  = tonumber
local tostring  = tostring
local type      = type

local read_config               = read_config
local read_message              = read_message

local io        = require "io"
local math      = require "math"
local os        = require "os"
local string    = require "string"
local concat    = require "table".concat

setfenv(1, M) -- Remove external access to contain everything in the module

local SEC_IN_DAY = 60 * 60 * 24

function load_schema(name, schema)
    local cfg_name          = read_config("cfg_name")
    local format            = read_config("format")
    local ts_field          = read_config("ts_field") or "Timestamp"
    local files             = {} -- manages the derived stream buffer/output files

    if format == "redshift.sql" then
        local driver    = require "luasql.postgres"
        local rsql      = require "derived_stream.redshift.sql"

        local function build_connection_string()
            local t = read_config("db_config")
            if type(t) ~= "table" then error("db_config must be a table") end

            local options = {}
            for k,v in pairs(t) do
                if type(k) ~= "string" then error("invalid connection string key") end
                if string.match(k, "^_") then
                    options[#options + 1] = string.format("%s=%s", string.sub(k, 2), tostring(v))
                else
                    options[#options + 1] = string.format("%s=%s", k, tostring(v))
                end
            end
            return concat(options, " ")
        end

        local uuid          = nil
        local constr        = build_connection_string()
        local buffer_path   = read_config("buffer_path") or error("buffer_path must be set")
        local buffer_max    = 16000 * 1000 -- 16MB
        local buffer_size   = read_config("buffer_size") or buffer_max - 8 * 1000
        assert(buffer_size > 0 and buffer_size <= buffer_max, "0 < buffer_size <= " .. tostring(buffer_max))

        local env = assert(driver.postgres())
        local con = assert(env:connect(constr))

        local function retry_db_error(err)
            if string.match(err, "EOF detected")
            or string.match(err, "connection not open") then
                local ncon, nerr = env:connect(constr)
                if nerr then
                    err = nerr
                else
                    con = ncon
                end
            else
                error(err, 2) -- exit on everything else
            end
            return err
        end

        local function get_output_file(ts)
            local day = math.floor(ts / (SEC_IN_DAY * 1e9))
            local f = files[day]
            if not f then
                local date = os.date("%Y%m%d", ts / 1e9)
                local table_name = string.format("%s_%s", name, date)
                local filename = string.format("%s/%s_%s.sql", buffer_path, cfg_name, date)
                local ok, cnt, err = pcall(con.execute, con, rsql.get_create_table_sql(table_name, schema))
                -- the duplicate key error is due to concurrent "create table" statements and the error is
                -- non fatal and expected when running multiple writers
                if ok and err and not string.match(err, "duplicate key violates unique constraint") then
                    return nil, retry_db_error(err)
                end
                if not ok then
                    error(err) -- exit on API errors
                end
                f = {fh = nil, table_name = table_name, filename = filename, offset = 0}
                files[day] = f
            end

            if not f.fh then
                f.fh = assert(io.open(f.filename, "a+"))
                f.offset = f.fh:seek("end")
            end
            return f
        end

        local function insert_file(f)
            if f.fh and f.offset ~= 0 then
                f.fh:seek("set")
                local ok, cnt, err = pcall(con.execute, con, f.fh:read("*a")) -- read the entire file and execute the query
                if ok and err then -- database error
                    return retry_db_error(err)
                end
                if not ok then
                    error(err) -- exit on API errors
                end
                f.fh:close()
                f.fh = nil
                f.offset = 0
                os.remove(f.filename);
            end
        end

        local function process_message()
            local file
            if not (uuid and uuid == read_message("Uuid")) then -- make sure we aren't in a retry loop
                local err
                file, err = get_output_file(read_message(ts_field))
                if not file then return -3, err end

                uuid = nil
                if file.offset == 0 then
                    file.fh:write("INSERT INTO ", file.table_name, " VALUES ")
                else
                    file.fh:write(",")
                end
                rsql.write_message(file.fh, schema, con)
                file.offset = file.fh:seek("end")
                if not file.offset then error("out of disk space") end
            end

            if not file or file.offset >= buffer_size then
                local err = insert_file(file)
                if err then
                    uuid = read_message("Uuid")
                    return -3, err
                end
            end
            return 0
        end

        local function timer_event(ns, shutdown)
            if shutdown then
                for k,v in pairs(files) do
                    insert_file(v)
                end
            end
        end

        return process_message, timer_event
    elseif format == "redshift.psv" then
        local rpsv = require "derived_stream.redshift.psv"

        local uuid          = nil
        local s3_path       = read_config("s3_path") or error("s3_path must be set")
        local buffer_path   = read_config("buffer_path") or error("buffer_path must be set")
        local buffer_max    = 1024 * 1024 * 1024 -- 1GiB
        local buffer_size   = read_config("buffer_size") or buffer_max
        assert(buffer_size > 0 and buffer_size <= buffer_max, "0 < buffer_size <= " .. tostring(buffer_max))
        local buffer_cnt    = 0
        local time_t        = 0

        local function get_output_file(ts)
            local day = math.floor(ts / (SEC_IN_DAY * 1e9))
            local f = files[day]
            if not f then
                local date = os.date("%Y%m%d", ts / 1e9)
                local table_name = string.format("%s_%s", name, date)
                local filename = string.format("%s/%s_%s.psv", buffer_path, cfg_name, date)
                f = {fh = nil, table_name = table_name, filename = filename, offset = 0}
                files[day] = f
            end

            if not f.fh then
                f.fh = assert(io.open(f.filename, "a"))
                f.offset = f.fh:seek("end")
            end
            return f
        end

        local function copy_file(f)
            if f.fh and f.offset ~= 0 then
                local t = os.time()
                local cmd
                if t == time_t then
                    buffer_cnt = buffer_cnt + 1
                    cmd = string.format("aws s3 cp %s %s/%s/%s/%d-%d.psv", f.filename, s3_path, f.table_name, cfg_name, time_t, buffer_cnt)
                else
                    time_t = t
                    buffer_cnt = 0
                    cmd = string.format("aws s3 cp %s %s/%s/%s/%d.psv", f.filename, s3_path, f.table_name, cfg_name, time_t)
                end
                local ret = os.execute(cmd)
                if ret ~= 0 then
                    return string.format("ret: %d, cmd: %s", ret, cmd)
                end
                f.fh:close()
                f.fh = nil
                f.offset = 0
                os.remove(f.filename);
            end
        end

        local function process_message()
            local file
            if not (uuid and uuid == read_message("Uuid")) then -- make sure we aren't in a retry loop
                local err
                file, err = get_output_file(read_message(ts_field))
                if not file then return -3, err end

                uuid = nil
                rpsv.write_message(file.fh, schema)
                file.offset = file.fh:seek("end")
                if not file.offset then error("out of disk space") end
            end

            if not file or file.offset >= buffer_size then
                local err = copy_file(file)
                if err then
                    uuid = read_message("Uuid")
                    return -3, err
                end
            end
            return 0
        end

        local function timer_event(ns, shutdown)
            if shutdown then
                for k,v in pairs(files) do
                    copy_file(v)
                end
            end
        end

        return process_message, timer_event
    elseif format == "protobuf" or format == "tsv" then
        local output_path = read_config("output_path") or error("output_path must be set")
        local files_cnt = 0

        local process_message
        local function timer_event(ns, shutdown)
            -- no op
        end

        local function prune_open_files()
            if files_cnt >= 10 then
                local min = math.huge
                local min_key
                for k,v in pairs(files) do
                    if v.time_t < min then
                        min = v.time_t
                        min_key = k
                    end
                end
                files[min_key].fh:close()
                files[min_key] = nil
                files_cnt = files_cnt - 1
            end
        end

        local function get_output_fh(ts, ext)
            local day = math.floor(ts / (SEC_IN_DAY * 1e9))
            local f = files[day]
            if not f then
                prune_open_files()
                local date = os.date("%Y%m%d", ts / 1e9)
                local filename = string.format("%s/%s_%s.%s", output_path, cfg_name, date, ext)
                f = {fh = nil, time_t = 0}
                local err
                f.fh = assert(io.open(filename, "a"))
                f.fh:setvbuf("no")
                files[day] = f
                files_cnt = files_cnt + 1
            end
            f.time_t = os.time()
            return f.fh
        end

        if format == "protobuf" then
            local hpb = require "derived_stream.heka_protobuf"
            process_message = function ()
                local fh = get_output_fh(read_message(ts_field), "log")
                local msg = {Type = name, Fields = {}}
                local ok, err = pcall(hpb.write_message, fh, msg, schema)
                if not ok then
                    return -1, err
                end
                return 0
            end
        else
            local tsv = require "derived_stream.tsv"
            local nil_value = read_config("nil_value") or ""
            process_message =  function ()
                local fh = get_output_fh(read_message(ts_field), "tsv")
                tsv.write_message(fh, schema, nil_value)
                return 0
            end
        end

        return process_message, timer_event
    else
        error("invalid derived stream format: " .. tostring(format))
    end
end

return M
