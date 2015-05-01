-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "cjson"
local l = require "lpeg"

local grammar = (l.C"payload" + l.C"environment") * l.P"." * l.C(l.P(1)^1)

function process_message()
    local raw = read_message("raw")
    local ok, msg = pcall(decode_message, raw)
    if not ok then return -1, msg end

    if type(msg.Fields) ~= "table" then return -1, "missing Fields" end

    local meta = {
        Timestamp = msg.Timestamp / 1e9,
        Type = msg.Type,
        Hostname = msg.Hostname,
    }

    local ok, json = pcall(cjson.decode, read_message("Payload"))
    if not ok then return -1, json end

    for i=1, #msg.Fields do
        local section, name = grammar:match(msg.Fields[i].name)
        if section then
            local ok, object = pcall(cjson.decode, msg.Fields[i].value[1])
            if ok then
                json[section][name] = object
            end
        else
            meta[msg.Fields[i].name] = msg.Fields[i].value[1]
        end
    end

    local ok, jmeta = pcall(cjson.encode, meta)
    if not ok then return -1, jmeta end
    local ok, payload = pcall(cjson.encode, json)
    if not ok then return -1, payload end

    inject_payload("txt", "output", json.clientId, "\t[", jmeta, ",", payload, "]\n")
    return 0
end
