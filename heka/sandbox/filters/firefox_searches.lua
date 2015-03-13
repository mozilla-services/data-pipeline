-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Calculates search totals by engine, origin, and country.

Config:

*Example Heka Configuration*

.. code-block:: ini

    [FirefoxSearches]
    type = "SandboxFilter"
    filename = "lua_filters/firefox_searches.lua"
    message_matcher = "Type == 'telemetry' && Fields[docType] == 'main'"
    ticker_interval = 60
    output_limit = 512000
    preserve_data = true
--]]

require "cjson"
require "circular_buffer"
require "math"
require "os"
require "string"

local ROWS           = 365
local SEC_PER_ROW    = 60 * 60 * 24

local origins        = {"abouthome", "contextmenu", "searchbar", "urlbar", "total"}
local ORIGINS_SIZE   = #origins

local countries      = {"US", "CN", "RU", "Total"}
local COUNTRIES_SIZE = #countries

local function make_cbuf()
    local cb = circular_buffer.new(ROWS, ORIGINS_SIZE, SEC_PER_ROW)
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
    {name = "Other" , cbuf = make_cbuf(), match = "."}
}

totals = circular_buffer.new(ROWS, #engines * COUNTRIES_SIZE, SEC_PER_ROW)
for i, v in ipairs(engines) do
    for j, c in ipairs(countries) do
        totals:set_header((i-1) * COUNTRIES_SIZE + j, string.format("%s_%s", v.name, c))
    end
end

local time = os.time
function process_message ()
    local json = read_message("Fields[payload.keyedHistograms]")
    if not json then return -1, "no keyedHistograms" end

    local ok, khist = pcall(cjson.decode, json)
    if not ok then return -1, khist end
    if type(khist.SEARCH_COUNTS) ~= "table" then return -1, "no SEARCH_COUNTS" end

    local ts = read_message("Timestamp")
    for k, v in pairs(khist.SEARCH_COUNTS) do
        for i, e in ipairs(engines) do
            if string.match(k, e.match) then
                if type(v.sum) ~= "number" then return -1, string.format("missing %s.sum", k) end
                local c = v.sum
                local cc = read_message("Fields[geoCountry]")
                for n = 1, COUNTRIES_SIZE - 1 do
                    if cc == countries[n] then
                        totals:add(ts, (i-1) * COUNTRIES_SIZE + n, c)
                        break
                    end
                end
                totals:add(ts, (i-1) * COUNTRIES_SIZE + COUNTRIES_SIZE, c)

                for n = 1, ORIGINS_SIZE - 1 do
                    if string.match(k, origins[n]) then
                        e.cbuf:add(ts, n, c)
                        break
                    end
                end
                e.cbuf:add(ts, ORIGINS_SIZE, c)
                break
            end
        end
    end
    return 0
end

local floor = math.floor
local date  = os.date
local json  = {}
for i=1, ROWS do
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

    local ts = totals:current_time() - (ROWS - 1) * SEC_PER_ROW * 1e9
    for i, v in ipairs(json) do
        v.time_t = floor(ts/1e9)
        v.date   = date("%F", v.time_t)
        for m, e in ipairs(engines) do
            for j, c in ipairs(countries) do
                local val = totals:get(ts, (m-1) * COUNTRIES_SIZE + j)
                if val ~= val then val = 0 end
                v[e.name][c] = val
            end
        end
        ts = ts + SEC_PER_ROW * 1e9
    end
    inject_payload("json", "totals", cjson.encode(json))
end
