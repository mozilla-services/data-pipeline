-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Firefox Active Instances

The memory requirements for this are roughly:
12kb hyperloglog * framecount * 2 channels * ~50 versions

Note that the version count is just a rough guess for upper limits, we will
have to finetune this from real-world data.

*Example Heka Configuration*

.. code-block:: ini

    [FirefoxADIRolling]
    type = "SandboxFilter"
    filename = "lua_filters/firefox_adi_rolling.lua"
    message_matcher = "Logger == 'fx' && Type == 'executive_summary' && Fields[vendor] == 'Mozilla' && Fields[app] == 'Firefox'"
    memory_limit = 300000000
    ticker_interval = 60
    preserve_data = true
--]]
require "cjson"
require "math"
require "os"
require "hyperloglog"

local SEC_IN_DAY = 60 * 60 * 24
local floor = math.floor

-- We track active Firefox instances for a window of ACTIVE_INTERVAL_IN_SEC,
-- in frames that span FRAME_SIZE_IN_SEC.
-- This is a near-real-time rolling window of active instances, so the current
-- frame is determined by the current server time.
-- We add one frame to actually look back over the whole ACTIVE_INTERVAL_IN_SEC.
local FRAME_SIZE_SEC = 15 * 60
local ACTIVITY_WINDOW_SEC = SEC_IN_DAY
local FRAME_COUNT = floor(ACTIVITY_WINDOW_SEC / FRAME_SIZE_SEC) + 1

-- This holds an array of frames.
-- Each frame contains a table mapping versions to hyperloglogs.
local active_instances = {}
for i=1,FRAME_COUNT do
    active_instances[i] = {
        beta = {},
        release = {},
    }
end

-- The last time we updated the ADI frames, in seconds.
local last_update = -1

local function clear_frame(frame)
    for channel,data in pairs(active_instances[frame]) do
        active_instances[frame][channel] = {}
    end
end

local function get_frame_hll(frame, channel, version)
    local data = active_instances[frame][channel]
    if data[version] == nil then
        data[version] = hyperloglog.new()
    end
    return data[version]
end

local function get_oldest_frame(now)
    local oldest_frame = (floor(now / FRAME_SIZE_SEC) + 1) % FRAME_COUNT
    return active_instances[oldest_frame + 1]
end

local function update_activity(cid, channel, version, timestamp_ns)
    -- os.time() returns seconds, rounding down the message timestamp avoids
    -- issues comparing to this.
    local ts = floor(timestamp_ns / 1e9)
    local now = os.time()

    -- Ignore messages that are too old or from the future.
    -- This makes things work properly with backfill and odd messages.
    if (ts > now) or ((now - ts) > ACTIVITY_WINDOW_SEC) then
        return
    end

    local now_frame = floor(now / FRAME_SIZE_SEC) % FRAME_COUNT
    local oldest_frame = (now_frame + 1) % FRAME_COUNT
    local last_frame = floor(last_update / FRAME_SIZE_SEC) % FRAME_COUNT
    local msg_frame = floor(ts / FRAME_SIZE_SEC) % FRAME_COUNT

    -- If all frames went outside the activity window since the last message,
    -- clear out everything.
    if (now - last_update) > ACTIVITY_WINDOW_SEC then
        for i=1,FRAME_COUNT do
            clear_frame(i)
        end
    -- If the current frame changed since the last message, clear out all frames
    -- since then and the current one.
    elseif last_frame ~= now_frame then
        local from = (last_frame + 1) % FRAME_COUNT
        local steps = (now_frame - from) % FRAME_COUNT

        for i=from,from+steps do
            clear_frame((i % FRAME_COUNT) + 1)
        end
    end

    -- Loop through the circular buffer, marking this client as seen from
    -- the oldest time frame up to the messages frame.
    local steps = (msg_frame - oldest_frame) % FRAME_COUNT
    for i=oldest_frame,oldest_frame+steps do
        get_frame_hll((i % FRAME_COUNT) + 1, channel, version):add(cid)
    end

    last_update = now
end

function process_message()
    local ts  = read_message("Timestamp")
    local cid = read_message("Fields[clientId]")
    local version = read_message("Fields[version]")
    local channel = read_message("Fields[channel]")

    local function is_nonempty_string(s)
        return type(s) == "string" and s ~= ""
    end

    if is_nonempty_string(cid) and is_nonempty_string(version) and
       is_nonempty_string(channel) and
       (channel == "release" or channel == "beta") then
        update_activity(cid, channel, version, ts)
    end

    return 0
end

function timer_event(ns)
    local now = os.time()
    local json = {
        updateTimestamp = now * 1e3,
        adi = {},
    }

    if (now - last_update) <= ACTIVITY_WINDOW_SEC then
        local frame = get_oldest_frame(now)
        for channel,data in pairs(frame) do
            json.adi[channel] = {}
            for version,hll in pairs(data) do
                json.adi[channel][version] = hll:count()
            end
        end
    end

    -- TODO: Currently we only display this in the Heka dashboard.
    -- This data needs to be published to S3 for consumption by other teams.
    inject_payload("json", "Firefox ADI Rolling", cjson.encode(json))
end
