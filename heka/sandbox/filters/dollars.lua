-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- Run tests using `lua -e "TestMode = true;" dollars.lua`

--[[
Monitor submission sizes and counts by channel and document type. This tracks
factors relevant to budget planning. See Bug 1179751.

*Example Heka Configuration*

.. code-block:: ini

    [PipelineBudget]
    type = "SandboxFilter"
    filename = "lua_filters/dollars.lua"
    message_matcher = "Type == 'payload_size'"
    ticker_interval = 60
    preserve_data = false

--]]
_PRESERVATION_VERSION = 1

require "table"

local bdate_grammar

if not TestMode then
    require "lpeg"
    require "cjson"

    fx = require "fx"
    max_per_channel = read_config("max_per_channel") or 60
    -- YYYYMMDD<whatever>
    bdate_grammar = lpeg.R("09") ^ 8
end

-- Size and count by day
sizes = {
    submission = {},
    build = {},
}

-- Key count by channel (for eviction purposes)
counters = {
    submission = {},
    build = {},
}

local function get_build_date(buildid)
    if type(buildid) ~= "string" then
        return nil
    end

    if string.len(buildid) < 8 then
        return nil
    end

    if not lpeg.match(bdate_grammar, buildid) then
        return nil
    end

    return string.sub(buildid, 1, 8)
end

-- If possible, evict a key older than "newdate" to make room for the new key.
local function evict(counter, newdate)
    local evictable_keys = {}
    local count = 0
    for k, v in pairs(counter) do
        count = count + 1
        if k < newdate then
            table.insert(evictable_keys, k)
        end
    end
    if #evictable_keys == 0 then
        return 0
    end
    table.sort(evictable_keys)
    local to_delete = 0
    while count >= max_per_channel do
        -- wipe out the min key.
        to_delete = to_delete + 1
        counter[evictable_keys[to_delete]] = nil
        count = count - 1
    end
    return to_delete
end

local function record_size(sizes, counter, channel, date, docType, size)
    if not sizes[channel] then
        sizes[channel] = {}
        counter[channel] = 0
    end
    local sc = sizes[channel]

    local scd = sc[date]
    if not scd then
        if counter[channel] >= max_per_channel then
            removed = evict(sc, date)
            if removed == 0 then
                -- This date is too old and our table is full.
                return
            end
            counter[channel] = counter[channel] - removed
        end
        counter[channel] = counter[channel] + 1
        scd = {}
        sc[date] = scd
    end

    local scdt = scd[docType]
    if not scdt then
        scdt = {count = 0, size = 0}
        scd[docType] = scdt
    end

    scdt.count = scdt.count + 1
    scdt.size  = scdt.size + size
    -- TODO: hdrhistogram to get a distribution of sizes
end

function process_message()
    local bdate = get_build_date(read_message("Fields[build]"))
    if not bdate then
        -- Skip it.
        return 0
    end

    local channel = fx.normalize_channel(read_message("Fields[channel]"))
    local sdate = read_message("Fields[submissionDate]")
    local msgType = read_message("Fields[docType]")
    if msgType ~= "main" and msgType ~= "saved-session" then
        msgType = "other"
    end
    local size = read_message("Fields[size]")

    record_size(sizes.submission, counters.submission, channel, sdate, msgType, size)
    record_size(sizes.build, counters.build, channel, bdate, msgType, size)

    return 0
end

function timer_event(ns)
    local ok, serialized = pcall(cjson.encode, sizes)
    if ok then
        inject_payload("json", "Submission Sizes by channel and date", serialized)
    end
end

---------------------------------------------------------
local function test()
    max_per_channel = 5

    local c = {}
    c["20150801"] = 1
    c["20150802"] = 2
    c["20150805"] = 3
    c["20150807"] = 4
    c["20150811"] = 5
    c["20150821"] = 6

    local count = 0
    for k, v in pairs(c) do
        count = count + 1
    end
    assert(count == 6)

    -- Nothing should be evicted here.
    evict(c, "20150701")
    count = 0
    for k, v in pairs(c) do
        count = count + 1
    end
    assert(count == 6)

    -- Things should actually be evicted here.
    evict(c, "20150822")
    count = 0
    for k, v in pairs(c) do
        count = count + 1
    end
    assert(count == (max_per_channel - 1))

    s = {}
    c = {}
    record_size(s, c, "nightly", "20150821", "main", 500)
    assert(s.nightly["20150821"].main.count == 1)
    assert(s.nightly["20150821"].main.size == 500)
    record_size(s, c, "nightly", "20150821", "main", 500)
    assert(s.nightly["20150821"].main.count == 2)
    assert(s.nightly["20150821"].main.size == 1000)
    record_size(s, c, "nightly", "20150801", "main", 500)
    record_size(s, c, "nightly", "20150802", "main", 500)
    record_size(s, c, "nightly", "20150621", "main", 500)
    record_size(s, c, "nightly", "20150803", "main", 500)
    assert(s.nightly["20150621"])
    assert(s.nightly["20150801"])
    assert(s.nightly["20150802"])
    assert(s.nightly["20150803"])

    -- Should evict the oldest
    record_size(s, c, "nightly", "20150805", "main", 500)
    assert(s.nightly["20150621"] == nil)
    assert(s.nightly["20150801"])
    record_size(s, c, "nightly", "20150806", "main", 500)
    assert(s.nightly["20150801"] == nil)
    record_size(s, c, "nightly", "20150807", "main", 500)
    record_size(s, c, "nightly", "20150808", "main", 500)
    record_size(s, c, "nightly", "20150821", "main", 500)

    -- should have no effect (no room for old)
    assert(s.nightly["20150721"] == nil)
    record_size(s, c, "nightly", "20150721", "main", 500)
    assert(s.nightly["20150721"] == nil)

    record_size(s, c, "nightly", "20150807", "main", 500)
    record_size(s, c, "nightly", "20150809", "main", 500)
    record_size(s, c, "nightly", "20150809", "main", 500)
    record_size(s, c, "nightly", "20150621", "main", 500)
    -- Adding an entry for another type still shouldn't add old stuff
    record_size(s, c, "nightly", "20150721", "saved-session", 500)
    record_size(s, c, "nightly", "20150621", "saved-session", 500)
    assert(s.nightly["20150721"] == nil)
    assert(s.nightly["20150621"] == nil)

    -- But adding new stuff is ok
    record_size(s, c, "nightly", "20150807", "saved-session", 500)
    record_size(s, c, "nightly", "20150809", "saved-session", 500)
    assert(s.nightly["20150807"])
    assert(s.nightly["20150809"])

    -- Other channels should track separately.
    record_size(s, c, "aurora", "20150821", "main", 500)
    record_size(s, c, "aurora", "20150821", "main", 500)
    record_size(s, c, "aurora", "20150809", "main", 440)
    record_size(s, c, "aurora", "20150809", "main", 440)
    record_size(s, c, "aurora", "20150809", "main", 450)
    record_size(s, c, "aurora", "20150809", "main", 451)
    record_size(s, c, "aurora", "20150721", "saved-session", 500)
    record_size(s, c, "aurora", "20150621", "saved-session", 500)
    record_size(s, c, "aurora", "20150621", "saved-session", 505)
    record_size(s, c, "aurora", "20150809", "saved-session", 500)

    for channel, days in pairs(s) do
        for day, types in pairs(days) do
            for type, v in pairs(types) do
                assert(v.count > 0)
                assert(v.size > 0)
            end
        end
    end
    assert(c.nightly == max_per_channel)
    assert(c.aurora == 4)
    assert(s.nightly["20150821"])
    assert(s.nightly["20150821"].main.count == 3)
    assert(s.nightly["20150821"].main.size == 1500)

    assert(s.aurora["20150809"])
    assert(s.aurora["20150809"].main.count == 4)
    assert(s.aurora["20150809"].main.size == 1781)
end

if TestMode then
    test()
end
