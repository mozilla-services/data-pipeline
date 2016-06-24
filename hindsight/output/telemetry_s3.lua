-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
## Heka Protobuf Message S3 Output Partitioner

Batches message data into Heka protobuf stream files based on the specified path
dimensions and copies them to S3 when they reach the maximum size or maximum
age.

#### Sample Configuration

```lua
filename        = "telemetry_s3.lua"
message_matcher = "Type == 'telemetry'"
ticker_interval = 60

-- see the mozilla.telemetry.dimensions module
dimension_file  = "foobar.json"

-- directory location to store the intermediate output files
batch_path       = "/var/tmp/foobar"

-- Specifies how many data files to keep open at once. If there are more
-- "current" files than this, the least-recently used file will be closed
-- and then re-opened if more messages arrive before it is copied to S3. The
-- default is 1000. A value of 0 means no maximum.
max_file_handles    = 1000

-- Specifies how much data (in bytes) can be written to a single file before
-- it is copied to s3 (default 500MB)
max_file_size       = 1024 * 1024 * 500

-- Specifies how long (in seconds) to wait before it is copied to s3
-- (default 1 hour).  Idle files are only checked every ticker_interval seconds.
max_file_age        = 60 * 60

-- Specifies that all local files will be copied S3 before exiting (default false).
flush_on_shutdown = true
preserve_data       = not flush_on_shutdown -- should always be the inverse of flush_on_shutdown

s3_path             = "s3://foo"

compression         = "zst"
```
--]]

require "cjson"
require "io"
require "os"
require "string"
require "table"
local ts3 = require "telemetry.s3"

files               = {}
local fh_cnt        = 0
local time_t        = 0
local buffer_cnt    = 0

local hostname              = read_config("Hostname")
local batch_path            = read_config("batch_path") or error("batch_path must be specified")
local s3_path               = read_config("s3_path") or error("s3_path must be specified")
local max_file_handles      = read_config("max_file_handles") or 1000
local max_file_size         = read_config("max_file_size") or 1024 * 1024 * 500
local max_file_age          = read_config("max_file_age") or 60 * 60
local flush_on_shutdown     = read_config("flush_on_shutdown")
local compression           = read_config("compression")
if compression and compression ~= "zst" and compression ~= "gz" then
    error("compression must be nil, zst or gz")
end


local function get_fqfn(path)
    return string.format("%s/%s", batch_path, path)
end


local function close_fh(entry)
    if not entry[2] then return end
    entry[2]:close()
    entry[2] = nil
    fh_cnt = fh_cnt - 1
end


local function copy_file(path, entry)
    close_fh(entry)
    local t = os.time()
    local cmd
    if t == time_t then
        buffer_cnt = buffer_cnt + 1
    else
        time_t = t
        buffer_cnt = 0
    end

    local src  = get_fqfn(path)
    local dim_path = string.gsub(path, "+", "/")
    if compression == "zst" then
        cmd = string.format("zstd -c %s | aws s3 cp - %s/%s/%d_%d_%s.%s", src,
                            s3_path, dim_path, time_t, buffer_cnt, hostname, compression)
    elseif compression == "gz" then
        cmd = string.format("gzip -c %s | aws s3 cp - %s/%s/%d_%d_%s.%s", src,
                            s3_path, dim_path, time_t, buffer_cnt, hostname, compression)
    else
        cmd = string.format("aws s3 cp %s %s/%s/%d_%d_%s", src,
                            s3_path, dim_path, time_t, buffer_cnt, hostname)
    end

    print(cmd)
    local ret = os.execute(cmd)
    if ret ~= 0 then
        return string.format("ret: %d, cmd: %s", ret, cmd)
    end
    files[path] = nil

    local ok, err = os.remove(src);
    if not ok then
        return string.format("os.remove('%s') failed: %s", path, err)
    end
end


local function get_entry(path)
    local ct = os.time()
    local t = files[path]
    if not t then
        t = {ct, nil} -- last active, file handle
        files[path] = t
    else
        t[1] = ct
    end

    if not t[2] then
        if max_file_handles ~= 0 then
            if fh_cnt >= max_file_handles then
                local oldest = ct + 60
                local entry
                for k,v in pairs(files) do -- if we max out file handles a lot we will want to make this more efficient
                    local et = v[1]
                    if v[2] and et < oldest then
                        entry = v
                        oldest = et
                    end
                end
                if entry then close_fh(entry) end
            end
        end
        t[2] = assert(io.open(get_fqfn(path), "a"))
        fh_cnt = fh_cnt + 1
    end
    return t
end

local dimensions = ts3.validate_dimensions(read_config("dimension_file"))
-- create the batch directory if it does not exist
os.execute(string.format("mkdir -p %s", batch_path))

function process_message()
    local dims = {}
    for i,d in ipairs(dimensions) do
        local v = ts3.sanitize_dimension(read_message(d.field_name))
        if v then
            if d.matcher(v) then
                dims[i] = v
            else
                dims[i] = "OTHER"
            end
        else
            dims[i] = "UNKNOWN"
        end
    end
    local path = table.concat(dims, "+") -- the plus will be converted to a path separator '/' on copy
    local entry = get_entry(path)
    local fh = entry[2]
    fh:write(read_message("framed"))
    local size = fh:seek()
    if size >= max_file_size then
        local err = copy_file(path, entry)
        if err then print(err) end
    end
    return 0
end


function timer_event(ns, shutdown)
    local err
    local ct = os.time()
    for k,v in pairs(files) do
        if (shutdown and flush_on_shutdown) or (ct - v[1] >= max_file_age) then
            local e = copy_file(k, v)
            if e then err = e end
        elseif shutdown then
            close_fh(v)
        end
    end
    if shutdown and flush_on_shutdown and err then
        error(string.format("flush on shutdown failed, last error: %s", err))
    end
end
