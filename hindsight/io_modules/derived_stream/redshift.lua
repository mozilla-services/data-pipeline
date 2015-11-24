-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

local M = {}
local tostring  = tostring
local type      = type

local date      = require "os".date
local floor     = require "math".floor
local gsub      = require "string".gsub

setfenv(1, M) -- Remove external access to contain everything in the module

VARCHAR_MAX_LENGTH = 65535

function strip_nonprint(v)
    -- A CHAR column can only contain single-byte characters
    -- http://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html
    -- for our use restrict it to printable chars
    if v == nil then return end
    if type(v) ~= "string" then v = tostring(v) end
    return gsub(v, "[^\032-\126]", "?")
end

function esc_timestamp(v, default)
    if type(v) ~= "number" or v > 4294967296e9 or v < 0 then
        return default
    end
    return date("%Y-%m-%d %H:%M:%S.", floor(v / 1e9)) .. tostring(floor(v % 1e9 / 1e3))
end

function esc_smallint(v, default)
    if type(v) ~= "number" or v > 32767 or v < -32767 then
        return default
    end
    return tostring(floor(v))
end

function esc_integer(v, default)
    if type(v) ~= "number" or v > 2147483647 or v < -2147483647 then
        return default
    end
    return tostring(floor(v))
end

function esc_bigint(v, default)
    if type(v) ~= "number" then return default end
    return tostring(floor(v))
end

function esc_double(v, default)
    if type(v) ~= "number"then return default end
    if v ~= v then return "NaN" end
    if v == 1/0 then return "Infinity" end
    if v == -1/0 then return "-Infinity" end
    return tostring(v)
end

function esc_boolean(v, default)
    if type(v) ~= "boolean" then return default end
    if v then return "TRUE" end
    return "FALSE"
end

return M
