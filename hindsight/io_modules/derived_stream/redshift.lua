-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

local M = {}
local ipairs = ipairs
local read_message = read_message
local tostring = tostring
local type = type

local string    = require "string"
local table     = require "table"

setfenv(1, M) -- Remove external access to contain everything in the module

VARCHAR_MAX_LENGTH = 65535

function esc_ts(v)
    if type(v) ~= "number" then return "NULL" end
    return string.format("(TIMESTAMP 'epoch' + %g * INTERVAL '1 seconds')", v / 1e9)
end

function esc_int(v)
    if type(v) ~= "number" or v > 2147483647 or v < -2147483647 then
        return "NULL"
    end
    return tostring(v)
end

function esc_smallint(v)
    if type(v) ~= "number"or v > 32767 or v < -32767 then
        return "NULL"
    end
    return tostring(v)
end

function esc_str(con, v, max)
    if v == nil then return "NULL" end
    if max == nil then max = VARCHAR_MAX_LENGTH end
    if type(v) ~= "string" then v = tostring(v) end
    if string.len(v) > max then v = string.sub(v, 1, max) end
    v = string.gsub(v, "[^\032-\126]", "?")

    local escd = con:escape(v)
    if not escd then return "NULL" end
    return string.format("'%s'", escd)
end

function write_values_sql(fh, con, schema)
    fh:write("(")
    for i,v in ipairs(schema) do
        local value = "NULL"
        if type(v[5]) == "function" then
            value = v[5]()
        elseif type(v[5]) == "string" then
            value = read_message(v[5])
        end

        if v[2] == "TIMESTAMP" then
            value = esc_ts(value)
        elseif v[2] == "INT" then
            value = esc_int(value)
        elseif v[2] == "SMALLINT" then
            value = esc_smallint(value)
        else
            value = esc_str(con, value, v[3])
        end

        if i > 1 then
            fh:write(",", value)
        else
            fh:write(value)
        end
    end
    fh:write(")")
end

function get_create_table_sql(name, schema)
    local pieces = {"CREATE TABLE IF NOT EXISTS ", name, " ("}
    for i, c in ipairs(schema) do
        if i > 1 then
            table.insert(pieces, ",")
        end
        table.insert(pieces, string.format("%s %s", c[1], c[2]))
        if c[3] ~= nil then
            table.insert(pieces, string.format("(%s)", c[3]))
        end
        if c[4]  then
            table.insert(pieces, " " .. c[4])
        end
    end
    table.insert(pieces, ")")
    return table.concat(pieces)
end

return M
