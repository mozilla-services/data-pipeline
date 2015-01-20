-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
_PRESERVATION_VERSION = 2

require "cjson"
require "circular_buffer"
require "math"
require "os"
require "string"
local dt = require "date_time"
local day_grammar = dt.build_strftime_grammar("%Y-%m-%d")

local rows = 365
local sec_per_row = 60 * 60 * 24
local origins = {"abouthome", "contextmenu", "searchbar", "urlbar", "total"}
local countries = {"US", "CN", "RU", "Total"}
local function make_cbuf()
    local cb = circular_buffer.new(rows, #origins, sec_per_row)
    for i, v in ipairs(origins) do
        cb:set_header(i, v)
    end
    return cb
end

engines = {
    {name = "Amazon", cbuf = make_cbuf(), match = "[Aa]mazon"},
    {name = "Bing"  , cbuf = make_cbuf(), match = "[Bb]ing"},
    {name = "Google", cbuf = make_cbuf(), match = "[Gg]oogle"},
    {name = "Yahoo" , cbuf = make_cbuf(), match = "[Yy]ahoo"},
    {name = "Other" , cbuf = make_cbuf(), match = "^[^_]"}
}

local countries_size = #countries
totals = circular_buffer.new(rows, #engines * countries_size, sec_per_row)
for i, v in ipairs(engines) do
    for j, c in ipairs(countries) do
        totals:set_header((i-1) * countries_size + j, string.format("%s_%s", v.name, c))
    end
end

local time = os.time
function process_message ()
    local payload =  read_message("Payload")

    local ok, fhr = pcall(cjson.decode, payload)
    if not ok then return -1 end

    local last_ping = 0
    if fhr["lastPingDate"] then
        last_ping = dt.time_to_ns(day_grammar:match(fhr["lastPingDate"]))
    end
    local t = time() * 1e9

    if type(fhr.data) ~= "table" then return -1, "invalid data" end
    if type(fhr.data.days) ~= "table" then return -1, "invalid data.days" end

    for k, v in pairs(fhr.data.days) do
        local ts = day_grammar:match(k)
        if ts then
            ts = dt.time_to_ns(ts)
            if ts >= last_ping then
                if ts > t then ts = t end -- don't allow dates in the future todo: adjust date for skew?
                if type(v["org.mozilla.searches.counts"]) == "table"  then
                    for n, c in pairs(v["org.mozilla.searches.counts"]) do
                        if string.sub(n, 1, 1) ~= "_" and type(c) == "number" then
                            for i, t in ipairs(engines) do
                                if string.match(n, t.match) then
                                    local cc = read_message("Fields[geoCountry]")
                                    for n=1, countries_size - 1 do
                                        if cc == countries[n] then
                                            totals:add(ts, (i-1) * countries_size + n, c)
                                            found = true
                                            break
                                        end
                                    end
                                    totals:add(ts, (i-1) * countries_size + countries_size, c)

                                    for x = 1, #origins-1 do
                                        if string.match(n, origins[x]) then
                                            t.cbuf:add(ts, x, c)
                                            break
                                        end
                                    end
                                    t.cbuf:add(ts, #origins, c)
                                    break
                                end
                            end
                        end
                    end
                end
            end
        end
    end
    return 0
end


local floor = math.floor
local date  = os.date
local json  = {}
for i=1, rows do
    json[i] = {date = "", time_t = 0}
    for m, e in ipairs(engines) do
        local t = {}
        json[i][e.name] = t
        for j, c in ipairs(countries) do
            t[c] = 0
        end
    end
end

function timer_event(ns)
    for i, v in ipairs(engines) do
        inject_payload("cbuf", v.name, v.cbuf)
    end
    inject_payload("cbuf", "Totals", totals)

    local ts = totals:current_time() - (rows - 1) * sec_per_row * 1e9
    for i, v in ipairs(json) do
        v.time_t = floor(ts/1e9)
        v.date   = date("%F", v.time_t)
        for m, e in ipairs(engines) do
            for j, c in ipairs(countries) do
                local val = totals:get(ts, (m-1) * countries_size + j)
                if val ~= val then val = 0 end
                v[e.name][c] = val
            end
        end
        ts = ts + sec_per_row * 1e9
    end
    inject_payload("json", "totals", cjson.encode(json))
end
