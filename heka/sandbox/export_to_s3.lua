-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
 
require "io"
require "os"
require "string"
 
--[[
*Example Configuration*
    This stores the text Payload field of any matched message to disk in temp_dir
    and copies the file to S3 in s3_dir.
    The file will be named from the messages fields per the pattern: <Logger>.<payload_name>.<payload_type>
    ... with any non-alphanumeric characters substituted with '_'.

    [S3Output]
    type = "SandboxOutput"
    filename = "export_to_s3.lua"
    message_matcher = "Type == 'heka.sandbox.FirefoxADIRolling.json'"
    ticker_interval = 0

    [S3Output.config]
    temp_dir = "/temp/path"
    s3_dir = "s3://test"
--]]

local temp_dir = read_config("temp_dir") or error("temp_dir must be set")
local s3_dir = read_config("s3_dir") or error("s3_path must be set")
 
function process_message()
    local pt = read_message("Fields[payload_type]")
    if type(pt) ~= "string" then return -1, "invalid payload_type" end
 
    local pn = read_message("Fields[payload_name]") or ""
    if type(pn) ~= "string" then return -1, "invalid payload_name" end
 
    local logger = read_message("Logger") or ""
 
    pn = string.gsub(pn, "[^%w_]", "_")
    pt = string.gsub(pt, "[^%w_]", "_")
    logger = string.gsub(logger, "[^%w_]", "_")
 
    local filename = string.format("%s.%s.%s", logger, pn, pt)
    local temp_path = string.format("%s/%s", temp_dir, filename)
    local s3_cmd = string.format("aws s3 cp %s %s/%s", temp_path, s3_dir, filename)

    local fh, err = io.open(temp_path, "w")
    if err then return -1, err end
 
    local payload = read_message("Payload") or ""
    fh:write(payload)
    fh:close()

    local ret = os.execute(s3_cmd)
    if ret ~= 0 then
        return -1, string.format("ret: %d, cmd: %s", ret, s3_cmd)
    end

    os.remove(temp_path)
 
    return 0
end
 
function timer_event(ns)
    -- no op
end 
