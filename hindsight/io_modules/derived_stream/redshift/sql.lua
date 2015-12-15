-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

local M = {}
local error = error
local ipairs = ipairs
local read_message = read_message
local tostring = tostring
local type = type

local rs        = require "derived_stream.redshift"
local string    = require "string"
local table     = require "table"

setfenv(1, M) -- Remove external access to contain everything in the module

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

function esc_timestamp(v)
    local ts = rs.esc_timestamp(v)
    if not ts then return "NULL" end
    return string.format("'%s'", ts)
end

function esc_varchar(con, v, max)
    if v == nil then return "NULL" end
    if max == nil then max = rs.VARCHAR_MAX_LENGTH end
    if type(v) ~= "string" then v = tostring(v) end
    if string.len(v) > max then v = string.sub(v, 1, max) end

    local escd = con:escape(v)
    if not escd then return "NULL" end
    return string.format("'%s'", escd)
end

function write_message(fh, schema, con)
    fh:write("(")
    for i,v in ipairs(schema) do
        local value = "NULL"
        if type(v[5]) == "function" then
            value = v[5]()
        elseif type(v[5]) == "string" then
            value = read_message(v[5])
        end

        if v[2] == "TIMESTAMP" then
            value = esc_timestamp(value)
        elseif v[2] == "SMALLINT" then
            value = rs.esc_smallint(value, "NULL")
        elseif v[2] == "INTEGER" then
            value = rs.esc_integer(value, "NULL")
        elseif v[2] == "BIGINT" then
            value = rs.esc_bigint(value, "NULL")
        elseif v[2] == "DOUBLE PRECISION" or v[2] == "REAL" or v[2] == "DECIMAL" then
            value = rs.esc_double(value, "NULL")
        elseif v[2] == "BOOLEAN" then
            value = rs.esc_boolean(value, "NULL")
        elseif v[2] == "CHAR" then
            value = esc_varchar(con, rs.strip_nonprint(value), v[3])
        elseif v[2] == "VARCHAR" or v[2] == "DATE" then
            value = esc_varchar(con, value, v[3])
        else
            error("Invaild Redshift data type (aliases are not allowed): " .. tostring(v[2]))
        end

        if i > 1 then
            fh:write(",", value)
        else
            fh:write(value)
        end
    end
    fh:write(")")
end

return M
