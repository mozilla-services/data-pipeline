-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Reads the files list application server logs from S3 for reporting..

Config:

filename    = "server_logs.lua"
start_date  = "2015-11-01"
end_date    = "2016-05-11"
service     = "^loop%-app"
--]]

require "heka_stream_reader"
require "io"
require "os"
require "string"

local date_format   = "^(%d%d%d%d)%-(%d%d)%-(%d%d)$"
local service       = read_config("service") or "."
local start_date    = read_config("start_date")
local end_date      = read_config("end_date")

local syear, smonth, sday   = start_date:match(date_format)
start_date                  = os.time({year = syear, month = smonth, day = sday})

local eyear, emonth, eday   = end_date:match(date_format)
end_date                    = os.time({year = eyear, month = emonth, day = eday})

assert(end_date >= start_date, "end_date must be greater than or equal to the start_date")
local num_months = (eyear * 12 + emonth) - (syear * 12 + smonth)

local function get_file_list(year, month)
    local path = string.format("s3://heka-logs/shared/%04d-%02d/", year, month)
    local list = {}

    local fh = assert(io.popen(string.format("aws s3 ls %s", path)))
    for line in fh:lines() do
        local fn, ds = string.match(line, "^%d%d%d%d%-%d%d%-%d%d%s+%d%d:%d%d:%d%d%s+%d+%s+(.-%-(%d%d%d%d%d%d%d%d)_.+)")
        if ds then
            ds = os.time({year = ds:sub(1, 4), month = ds:sub(5, 6), day = ds:sub(7, 8)})
            if fn and string.match(fn, service) and ds >= start_date and ds <= end_date then
                list[#list + 1] = fn
            end
        end
    end
    fh:close()
    return path, list
end


local msg = {
    Timestamp = 0,
    Type = "",
    Logger = "",
    Fields = {
        action   = "",
        userType = "",
        uid      = ""
    }
}

function process_message()
    local year = tonumber(syear)
    local month = tonumber(smonth)
    for i=0, num_months do
        local path, list = get_file_list(year, month)
        for i,fn in ipairs(list) do
            local hsr = heka_stream_reader.new(path)
            print("processing", fn)
            local fh = assert(io.popen(string.format("aws s3 cp %s%s - | gzip -d -c", path, fn)))
            local found, consumed, read
            repeat
                repeat
                    found, consumed, read = hsr:find_message(fh)
                    if found then
                        -- inject_message(hsr) -- todo remove loop filtering

                        -- filtering/data reduction for loop testing
                        local action   = hsr:read_message("Fields[action]")
                        local userType = hsr:read_message("Fields[userType]")
                        local uid      = hsr:read_message("Fields[uid]")
                        if uid and action == "join" and (userType == "Unregistered" or userType == "Registered")  then
                            msg.Timestamp       = hsr:read_message("Timestamp")
                            msg.Type            = hsr:read_message("Type")
                            msg.Logger          = hsr:read_message("Logger")
                            msg.Fields.action   = action
                            msg.Fields.userType = userType
                            msg.Fields.uid      = uid
                            inject_message(msg)
                        end
                        -- end loop testing
                    end
                until not found
            until read == 0
            fh:close()
        end
        month = month + 1
        if month == 13 then
            month = 1
            year  = year + 1
        end
    end
    return 0
end
