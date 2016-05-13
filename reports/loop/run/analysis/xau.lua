-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
WARNING THIS PLUGIN EXPECTS THE DATA ORDERED BY DAY WITH NO GAPS.

If it is going to be run for more than this one-off we should make it more robust
--]]


require "cjson"
require "hyperloglog"
require "math"
require "os"
require "table"

local SEC_IN_DAY  = 60 * 60 * 24

local days = {}
local cday = -1
local hll

local function compute_range(i, len)
    if i == 1 then return nil end
    local s = i - (len - 1)
    if s < 1 then
        s = 1
    end
    return hyperloglog.count(unpack(days, s, i))
end

function process_message()
    local day = math.floor(read_message("Timestamp") / 1e9 / SEC_IN_DAY)
    if cday ~= -1 and (day < cday or day > cday + 1) then
        print("day", os.date("%Y%m%d", day * SEC_IN_DAY) , "cday",  os.date("%Y%m%d", cday * SEC_IN_DAY))
        error("data is out of order or has gaps")
    end

    if day ~= cday then
        hll = hyperloglog.new()
        days[#days + 1] = hll
        cday = day
    end
    hll:add(read_message("Fields[uid]"))
    return 0
end

function timer_event(ns, shutdown)
    local fday = cday - #days
    local json = {}
    for i, v in ipairs(days) do
        local dau = v:count()
        local wau = compute_range(i, 7) or dau
        local mau = compute_range(i, 28) or dau
        json[#json + 1] = {date = os.date("%Y%m%d", (fday + i) * SEC_IN_DAY), dau = dau, wau = wau, mau = mau}
    end
    table.sort(json, function(t1, t2) return t1.date < t2.date end)
    inject_payload("json", "xau", cjson.encode(json))
end
