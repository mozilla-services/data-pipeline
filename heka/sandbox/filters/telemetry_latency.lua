-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
1) How many days do we need to look back for k% of active profiles to be up-to-date?
   Each active profile is associated to the date in which we last received a submission
   for it on our servers. Periodically, we compute for all profiles the difference
   between the current date and the date of reception and finally plot a histogram
   of the differences expressed in number of days.

2) How may days do we need to look back to observe k% of active profile activity?
   Each active profile is associated with its last activity date. Periodically, we compute
   for all active profiles the difference from the last activity date to the current date
   and finally plot a histogram of the differences expressed in number of days.

3) What’s the delay in hours between the start of a session activity for an active profile 
   and the time we receive the submission? When we receive a new submission for an active
   profile we compute the delay from the start of the submission activity to the time we
   received the submission on our servers. Periodically, we plot a histogram of the latencies
   expressed in hours.

Note that:
   - As timeseries of histograms or heatmaps are not supported by the Heka plotting
     facilities, only the median and some other percentiles are being output.

   - An active profile is one who has used the browser in the last six weeks (42 days)

*Example Heka Configuration*

.. code-block:: ini

    [TelemetryLatency]
    type = "SandboxFilter"
    filename = "lua_filters/telemetry_latency.lua"
    message_matcher = "Type == 'telemetry' && Fields[docType] == 'main'"
    ticker_interval = 60
    preserve_data = false
--]]

require "circular_buffer"
require "table"
require "string"
require "math"
require "os"

local LOST_PROFILE_THRESHOLD = 42 -- https://people.mozilla.org/~bsmedberg/fhr-reporting/#usage
local PING_VERSION = "4"
local NSPERHOUR = 60*60*1e9
local NSPERDAY = 24*NSPERHOUR
local MEDIAN = 1
local PERCENTILE_75 = 2
local PERCENTILE_99 = 3

local seen_by_channel = {}
local seen_history_by_channel = {}

local active_by_channel = {}
local active_history_by_channel = {}

local delay_by_channel = {}
local delay_history_by_channel = {}

local rows = read_config("rows") or 1440
local sec_per_row = read_config("sec_per_row") or 60

local function log(message)
    local dbg = {message}
    inject_payload("txt", "debug", table.concat(dbg, "\n"))
end

local function get_channel_entry(t, channel)
   local entry = t[channel]
   if not entry then
      entry = {}
      t[channel] = entry
   end
   return entry
end

local function get_history(unit, metric_history_by_channel, channel)
   local history = metric_history_by_channel[channel]
   if not history then
      history = circular_buffer.new(rows, 3, sec_per_row)
      history:set_header(MEDIAN, "Median", unit, "none")
      history:set_header(PERCENTILE_75, "75th percentile", unit, "none")
      history:set_header(PERCENTILE_99, "99th percentile", unit, "none")
      metric_history_by_channel[channel] = history
   end
   return history
end

local function process_client_metric(metric_by_channel, channel, client_id, value)
   local metric = get_channel_entry(metric_by_channel, channel)
   metric[client_id] = value
end

function process_message ()
   local sample_id = read_message("Fields[sampleId]")
   local version = read_message("Fields[sourceVersion]")

   if version == PING_VERSION and sample_id == 0 then
      local ts = read_message("Timestamp")
      local channel = read_message("Fields[appUpdateChannel]") or "UNKNOWN"
      local client_id = read_message("Fields[clientId]")
      local activity_ts = read_message("Fields[creationTimestamp]") -- exists only in new "unified" pings

      process_client_metric(seen_by_channel, channel, client_id, ts)
      process_client_metric(active_by_channel, channel, client_id, activity_ts)
      process_client_metric(delay_by_channel, channel, client_id, ts - activity_ts)
   end

   return 0
end

local function percentile(sorted_array, p)
   return sorted_array[math.ceil(#sorted_array*p/100)]
end

local function timer_event_metric(descr, unit, metric_by_channel, metric_history_by_channel, ns, calc)
   for channel, metric in pairs(metric_by_channel) do
      local sorted_metric = {}

      for k, v in pairs(metric) do
         sorted_metric[#sorted_metric + 1] = calc(ns, v)
      end

      table.sort(sorted_metric)
      local median = percentile(sorted_metric, 50)
      local perc75 = percentile(sorted_metric, 75)
      local perc99 = percentile(sorted_metric, 99)

      local history = get_history(unit, metric_history_by_channel, channel)
      if median then history:set(ns, MEDIAN, median) end
      if perc75 then history:set(ns, PERCENTILE_75, perc75) end
      if perc99 then history:set(ns, PERCENTILE_99, perc99) end

      inject_payload("cbuf", channel .. " " .. descr, history)
   end
end

local function remove_inactive_client(channel, client_id)
   seen_by_channel[channel][client_id] = nil
   active_by_channel[channel][client_id] = nil
   delay_by_channel[channel][client_id] = nil
end

local function remove_inactive_clients(current_ts)
   for channel, active in pairs(active_by_channel) do
      for client_id, last_active_ts in pairs(active) do
         if (current_ts - last_active_ts)/NSPERDAY > LOST_PROFILE_THRESHOLD then
            remove_inactive_client(channel, client_id)
         end
      end
   end
end

function timer_event(ns)
   remove_inactive_clients(ns)
   timer_event_metric("up-to-date", "days", seen_by_channel, seen_history_by_channel, ns,
                      function(ns, v) return math.floor((ns - v)/NSPERDAY) end)
   timer_event_metric("active", "days", active_by_channel, active_history_by_channel, ns,
                      function(ns, v) return math.floor((ns - v)/NSPERDAY) end)
   timer_event_metric("delay", "hours", delay_by_channel, delay_history_by_channel, ns,
                      function(ns, v) return math.floor(v/NSPERHOUR) end)
end
