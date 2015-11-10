-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

local M = {}
local ipairs = ipairs
local read_message = read_message
local tostring = tostring
local type = type

local rs        = require "derived_stream.redshift"
local string    = require "string"

setfenv(1, M) -- Remove external access to contain everything in the module

local esc_chars = { ["|"] = "\\|", ["\r"] = "\\r", ["\n"] = "\\n" }
function esc_varchar(v, max)
    if v == nil then return "" end
    if max == nil then max = rs.VARCHAR_MAX_LENGTH end
    if type(v) ~= "string" then v = tostring(v) end
    if string.len(v) > max then v = string.sub(v, 1, max) end
    return string.gsub(v, "[|\r\n]", esc_chars)
end

function write_message(fh, schema)
    for i,v in ipairs(schema) do
        local value
        if type(v[5]) == "function" then
            value = v[5]()
        elseif type(v[5]) == "string" then
            value = read_message(v[5])
        end

        if v[2] == "TIMESTAMP" then
            value = rs.esc_ts(value, "")
        elseif v[2] == "SMALLINT" then
            value = rs.esc_smallint(value, "")
        elseif v[2] == "INTEGER" then
            value = rs.esc_integer(value, "")
        elseif v[2] == "BIGINT" then
            value = rs.esc_bigint(value, "")
        elseif v[2] == "DOUBLE PRECISION" or v[2] == "REAL" or v[2] == "DECIMAL" then
            value = rs.esc_double(value, "")
        elseif v[2] == "BOOLEAN" then
            value = rs.esc_booleon(value, "NULL")
        elseif v[2] == "CHAR" then
            value = esc_varchar(rs.strip_nonprint(value), v[3])
        elseif v[2] == "VARCHAR" or v[2] == "DATE" then
            value = esc_varchar(con, value, v[3])
        else
            error("Invaild Redshift data type (aliases are not allowed): " .. tostring(v[2]))
        end

        if i > 1 then
            fh:write("|", value)
        else
            fh:write(value)
        end
    end
    fh:write("\n")
end

return M
