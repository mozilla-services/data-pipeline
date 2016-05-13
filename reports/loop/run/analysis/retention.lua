-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
WARNING THIS PLUGIN EXPECTS THE DATA ORDERED BY DAY WITH NO GAPS.
--]]

require "cjson"
require "math"
require "os"
require "table"

local DAY_OFFSET  = 4 -- start the week on Sunday and correct for the Unix epoch landing on a Thursday
local SEC_IN_DAY  = 60 * 60 * 24
local SEC_IN_WEEK = SEC_IN_DAY * 7

local COHORT    = 1
local DAY       = 2
local uids      = {} -- each key has an array columns: cohort, day, interval flag

local interval_days = read_config("interval_days") or error("an interval_days must be configured")

function process_message()
    local day       = math.floor(read_message("Timestamp") / 1e9 / SEC_IN_DAY)
    local week      = math.floor((day + DAY_OFFSET) / 7)
    local cohort    = week * SEC_IN_WEEK - (SEC_IN_DAY * DAY_OFFSET)
    local uid       = read_message("Fields[uid]")

    local u = uids[uid]
    if not u then
        u = {cohort, day}
        uids[uid] = u
        return 0
    end
    local delta = day - u[DAY]
    if delta <= 0 then return 0 end

    local interval = math.floor((delta - 1) / interval_days)
    local cinterval = #u - 2
    if interval == cinterval then
        u[cinterval + 3] = true
    end
    return 0
end


function timer_event(ns, shutdown)
    local cohorts = {}
    for k, u in pairs(uids) do
        local cohort = u[COHORT]
        local c = cohorts[cohort]
        if not c then
            c = {user_count = 1, intervals = {}}
            cohorts[cohort] = c
        else
            c.user_count = c.user_count + 1
        end
        for i, j in ipairs(u) do
            if i > 2  and j then -- skip the cohort and day entries
                local value = c.intervals[i - 2]
                if not value then
                    c.intervals[i - 2] = 1
                else
                   c.intervals[i - 2] = value + 1
                end
            end
        end
    end

    local json = {interval_days = interval_days, cohorts = {}}
    for k, c in pairs(cohorts) do
        json.cohorts[#json.cohorts + 1] = {cohort = os.date("%Y%m%d", k), cohort_user_count = c.user_count, interval_counts = c.intervals}
    end
    table.sort(json.cohorts, function(t1, t2) return t1.cohort < t2.cohort end)
    inject_payload("json", "retention", cjson.encode(json))
end
