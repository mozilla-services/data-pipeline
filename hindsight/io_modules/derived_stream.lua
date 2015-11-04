-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

local M = {}
local assert    = assert
local error     = error
local pairs     = pairs
local pcall     = pcall
local require   = require
local tonumber  = tonumber

local batch_checkpoint_update   = batch_checkpoint_update
local read_config               = read_config
local read_message              = read_message

local io        = require "io"
local math      = require "math"
local os        = require "os"
local string    = require "string"

setfenv(1, M) -- Remove external access to contain everything in the module

local SEC_IN_DAY = 60 * 60 * 24

function load_schema(name, schema)
    local ticker_interval   = read_config("ticker_interval")
    local cfg_name          = read_config("cfg_name")
    local format            = read_config("format")
    local ts_field          = read_config("ts_field") or "Timestamp"
    local files             = {} -- manages the derive stream buffer/output files

    if format == "redshift" then
        local driver        = require "luasql.postgres"
        local rs            = require "derived_stream.redshift"

        local db_config     = read_config("db_config") or error("db_config must be set")
        local buffer_path   = read_config("buffer_path") or error("buffer_path must be set")
        local buffer_size   = read_config("buffer_size") or 10000 * 1024
        assert(buffer_size > 0, "buffer_size must be greater than zero")

        local last_insert   = 0
        local uuid          = nil

        local env = assert(driver.postgres())
        local con = assert(env:connect(db_config.name, db_config.user, db_config._password, db_config.host, db_config.port))

        local function get_output_file(ts)
            local day = math.floor(ts / (SEC_IN_DAY * 1e9))
            local f = files[day]
            if not f then
                local date = os.date("%Y%m%d", ts / 1e9)
                local table_name = string.format("%s_%s", name, date)
                local filename = string.format("%s/%s_%s.sql", buffer_path, cfg_name, date)
                local ok, cnt, err = pcall(con.execute, con, rs.get_create_table_sql(table_name, schema))
                if ok and err and not string.match(err, "duplicate key violates unique constraint") then
                    error(err) -- non recoverable database error
                end
                if not ok then return nil, err end -- API error

                f = {fh = nil, table_name = table_name, filename = filename, offset = 0}
                files[day] = f
            end

            if not f.fh then
                f.fh = assert(io.open(f.filename, "w+"))
                f.offset = 0
            end
            return f
        end

        local function insert_files() -- all files are flushed at once to ensure the checkpoint is accurate
            for k,v in pairs(files) do
                if v.fh and v.offset ~= 0 then
                    v.fh:seek("set")
                    local ok, cnt, err = pcall(con.execute, con, v.fh:read("*a")) -- read the entire file and execute the query
                    if ok and err then -- database error
                        error(err)
                    end
                    if not ok then return err end -- API error
                    v.fh:close()
                    v.fh = nil
                    os.remove(v.filename);
                end
            end
            last_insert = os.time()
        end

        local function process_message()
            local file
            if not (uuid and uuid == read_message("Uuid")) then -- make sure we aren't in a retry loop
                file, err = get_output_file(read_message(ts_field))
                if not file then return -3, err end

                uuid = nil
                if file.offset == 0 then
                    file.fh:write("INSERT INTO ", file.table_name, " VALUES ")
                else
                    file.fh:write(",")
                end
                rs.write_values_sql(file.fh, con, schema)
                file.offset = file.fh:seek("end")
            end

            if not file or file.offset >= buffer_size then
                local err = insert_files()
                if err then
                    uuid = read_message("Uuid")
                    return -3, err
                else
                    return 0
                end
            end
            return -4
        end

        local function timer_event(ns, shutdown)
            if shutdown or last_insert + ticker_interval <= ns / 1e9 then
                local err = insert_files()
                if not err then
                    batch_checkpoint_update()
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
