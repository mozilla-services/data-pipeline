-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

local M = {}
local ipairs    = ipairs
local tostring  = tostring
local type      = type

local read_message      = read_message
local encode_message    = encode_message

local gsub = require "string".gsub

setfenv(1, M) -- Remove external access to contain everything in the module

local esc_chars = { ["\t"] = "\\t", ["\r"] = "\\r", ["\n"] = "\\n" }

function esc_str(v)
    return gsub(v, "[\t\r\n]", esc_chars)
end

function write_message(fh, schema, nil_value)
    for i,v in ipairs(schema) do
        local value
        if type(v[5]) == "function" then
            value = v[5]()
        elseif type(v[5]) == "string" then
            value = read_message(v[5])
        end
        if not value then
            value = nil_value
        else
            value = tostring(value)
        end

        if v[2] == "CHAR" or v[2] == "VARCHAR" then
            value = esc_str(value)
        end

        if i > 1 then
            fh:write("\t", value)
        else
            fh:write(value)
        end
    end
    fh:write("\n")
end

return M
