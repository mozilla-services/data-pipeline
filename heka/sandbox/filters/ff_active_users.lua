-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "circular_buffer"
require "cjson"
require "math"
require "os"
require "hyperloglog"

local DAYS = 30
local SEC_IN_DAY = 60 * 60 * 24
local floor = math.floor
local date = os.date

day_cb  = circular_buffer.new(DAYS, 1, SEC_IN_DAY)
day_cb:set_header(1, "Active Users")
day_hll = {}
for i=1,DAYS do
    day_hll[i] = hyperloglog.new()
end
current_day = -1

local month_names = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
local MONTHS = #month_names
month_hll = {}
for i=1,MONTHS do
    month_hll[i] = hyperloglog.new()
end
current_month = -1

local function clear_days(s, e)
    for i = s + 1, e do
        local idx = i % DAYS + 1
        day_hll[idx]:clear()
        day_cb:set(i * SEC_IN_DAY * 1e9, 1, 0)
    end
end

local function update_day(ts, cid, day)
    if current_day == -1 then current_day = day end

    local delta = day - current_day
    if delta > 0 and delta < DAYS then
        clear_days(current_day, day)
        current_day = day
    elseif delta >= DAYS then
        clear_days(current_day, current_day + DAYS)
        current_day = current_day + delta
    elseif delta <= -DAYS then
        return 0 -- ignore data in the past
    end
    local idx = day % DAYS + 1
    if day_hll[idx]:add(cid) then
        day_cb:set(ts, 1, day_hll[idx]:count())
    end
end

local function clear_months(s, n)
    for i = 1, n do
        s = s + 1
        if s > MONTHS then s = 1 end
        month_hll[s]:clear()
    end
end

local function update_month(ts, cid, day_changed)
    local month = current_month
    if current_month == -1 or day_changed then
        local t = date("*t", ts / 1e9)
        month = tonumber(t.month)
        if current_month == -1 then current_month = month end
    end

    local delta = month - current_month
    if delta > 0 then
        clear_months(current_month, delta)
        current_month = month
    elseif delta < -1 then -- if older than a month roll over the year
        clear_months(current_month, MONTHS + delta)
        current_month = month
    end
    month_hll[month]:add(cid)
end

----

function process_message()
    local ts  = read_message("Timestamp")
    local cid = read_message("Fields[clientId]")
    if type(cid) == "string" then
        local day = floor(ts / (SEC_IN_DAY * 1e9))
        local day_changed = day ~= current_day
        update_day(ts, cid, day)
        update_month(ts, cid, day_changed)
    end
    return 0
end

function timer_event(ns)
    inject_payload("cbuf", "Firefox Active Daily Users", day_cb)

    local json = {}
    local idx = current_month
    if idx == -1 then idx = 0 end

    for i=1,MONTHS do
        idx = idx + 1
        if idx > MONTHS then idx = 1 end
        json[i] = {[month_names[idx]] = month_hll[idx]:count()}
    end
    inject_payload("json", "Firefox Active Monthly Users", cjson.encode(json))
end
