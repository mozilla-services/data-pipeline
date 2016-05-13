-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Sanity check to make sure the low loop traffic volumes don't through off the
loop hyperloglog results too much.
--]]

require "hyperloglog"
require "math"

local days = {}

function process_message()
    local day = math.floor(read_message("Timestamp") / 1e9 / 86400)
    local d = days[day]
    if not d then
        d = {hyperloglog.new(), {}}
        days[day] = d
    end
    local p = read_message("Fields[uid]")
    d[1]:add(p)
    d[2][p] = true
    return 0
end

function timer_event(ns, shutdown)
    for k,v in pairs(days) do
        local cnt = v[1]:count()
        local acnt = 0
        for m,n in pairs (v[2]) do
            acnt = acnt + 1
        end
        print(k, "hll", cnt, "actual", acnt, "percentage", cnt/acnt)
    end
end
