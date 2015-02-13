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
assert(con, err)
assert (con:execute[[
        CREATE TABLE IF NOT EXISTS logs(
        timestamp BIGINT,
        severity INTEGER,
        pid INTEGER,
        hostname VARCHAR(126),
        type VARCHAR(126),
        logger VARCHAR(126),
        envversion VARCHAR(126),
        payload VARCHAR(126)
        )
        ]])

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

local last_flush  = 0
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

-- plugin interfaces
function process_message()
    if sep == " " then
        fh:write("INSERT INTO logs VALUES")
    end
    fh:write(string.format("%s(%d,%d,%d,'%s','%s','%s','%s','%s')", sep,
    read_message("Timestamp"),
    read_message("Severity") or 7,
    read_message("Pid") or 0,
    con:escape(read_message("Hostname")),
    con:escape(read_message("Type")),
    con:escape(read_message("Logger")),
    con:escape(read_message("EnvVersion")),
    con:escape(read_message("Payload"))))
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
