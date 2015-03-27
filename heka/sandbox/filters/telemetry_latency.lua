-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
1) What's the distribution of the latency between the subsession ping creation
   time and the server reception time in the past 24h?

2) What's the distribution of the latency between the subsession ping creation time
   and the server reception time in the past 7 days?

3) What’s the delay in hours for active profiles between the date a ping was created 
   and the time it was received on our servers? When a new submission for an
   active profile is received, the latency between the ping creation date and the 
   reception time is computed. Periodically, a histogram of the latencies is plot.

4) How many days do we need to look back to see at least one submission for k% of 
   active profiles? Each active profile is associated to the date for which a submission 
   was last received on our servers. Periodically, we compute for all profiles the 
   difference between the current date and the date of reception and finally plot 
   a histogram of the differences expressed in number of days.

Note that:
   - As timeseries of histograms or heatmaps are not supported by the Heka plotting
     facilities, only the median and some other percentiles are being output.

   - An active profile is one who has used the browser in the last six weeks (42 days)

*Example Heka Configuration*

.. code-block:: ini

    [TelemetryLatency]
    type = "SandboxFilter"
    filename = "lua_filters/telemetry_latency.lua"
    message_matcher = "Type == 'telemetry' && Fields[docType] == 'main' && Fields[sampleId] == 0 && Fields[sourceVersion] == 4 && Fields[appUpdateChannel] == 'nightly'"
    ticker_interval = 60
    preserve_data = false
    memory_limit = 209715200
--]]

require "circular_buffer"
require "table"
require "string"
require "math"
require "os"

local LOST_PROFILE_THRESHOLD = 42 -- https://people.mozilla.org/~bsmedberg/fhr-reporting/#usage
local NSPERHOUR = 60*60*1e9
local NSPERDAY = 24*NSPERHOUR
local SECPERHOUR = NSPERHOUR/1e9
local MEDIAN = 1
local PERCENTILE_75 = 2
local PERCENTILE_99 = 3

local seen_by_channel = {}
local seen_history_by_channel = {}

local creation_delay_by_channel = {}
local creation_delay_history_by_channel = {}

local creation_delay24h_by_channel = {}
local creation_delay24h_history_by_channel = {}

local creation_delay7d_by_channel = {}
local creation_delay7d_history_by_channel = {}

local rows = read_config("rows") or 1440
local sec_per_row = read_config("sec_per_row") or 60

local beginning_of_time = nil
local last_subsessions_cleanup = nil

local function log(message)
    local dbg = {message}
    inject_payload("txt", "debug", table.concat(dbg, "\n"))
end

local function get_channel_entry(t, channel, build)
   local entry = t[channel]
   if not entry then
      entry = build()
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
   local metric = get_channel_entry(metric_by_channel, channel,
                                    function() return {} end)
   metric[client_id] = value
end

local function new_subsession_struct(rows)
   return {list = {}, counter= circular_buffer.new(rows, 1, SECPERHOUR)}
end

local function process_subsession_metric(ts, metric_by_channel, hours, channel, value)
   local metric = get_channel_entry(metric_by_channel, channel,
                                    function() return new_subsession_struct(hours) end)
   table.insert(metric.list, value)
   metric.counter:add(ts, 1, 1)
end

function process_message ()
   local ts = read_message("Timestamp")
   local client_id = read_message("Fields[clientId]")
   local creation_ts = read_message("Fields[creationTimestamp]") -- exists only in new "unified" pings
   local channel = read_message("Fields[appUpdateChannel]") or "UNKNOWN"
   local reason = read_message("Fields[reason]")
   local ch_reason = channel .. ":" .. reason

   process_client_metric(seen_by_channel, ch_reason, client_id, ts)
   process_client_metric(creation_delay_by_channel, ch_reason, client_id, ts - creation_ts)
   process_subsession_metric(ts, creation_delay24h_by_channel, 24, ch_reason, ts - creation_ts)
   process_subsession_metric(ts, creation_delay7d_by_channel, 7*24, ch_reason, ts - creation_ts)

   return 0
end

local function percentile(sorted_array, p)
   return sorted_array[math.ceil(#sorted_array*p/100)]
end

local function update_plot(descr, unit, channel, history, metric, ns, calc)
   table.sort(metric)

   local median = calc(ns, percentile(metric, 50))
   local perc75 = calc(ns, percentile(metric, 75))
   local perc99 = calc(ns, percentile(metric, 99))

   if median then history:set(ns, MEDIAN, median) end
   if perc75 then history:set(ns, PERCENTILE_75, perc75) end
   if perc99 then history:set(ns, PERCENTILE_99, perc99) end

   inject_payload("cbuf", channel .. " " .. descr, history)
end

local function timer_event_client_metric(descr, unit, metric_by_channel,
                                  metric_history_by_channel, ns, calc)
   for channel, metric in pairs(metric_by_channel) do
      local flat_metric = {}
      local history = get_history(unit, metric_history_by_channel, channel)

      for k, v in pairs(metric) do
         flat_metric[#flat_metric + 1] = v
      end

      update_plot(descr, unit, channel, history, flat_metric, ns, calc)
   end
end

local function timer_event_subsession_metric(descr, unit, metric_by_channel,
                                             metric_history_by_channel, ns, calc)
   for channel, flat_metric in pairs(metric_by_channel) do
      local history = get_history(unit, metric_history_by_channel, channel)
      update_plot(descr, unit, channel, history, flat_metric.list, ns, calc)
   end
end

local function remove_inactive_client(channel, client_id)
   seen_by_channel[channel][client_id] = nil
   creation_delay_by_channel[channel][client_id] = nil
end

local function remove_inactive_clients(ts)
   for channel, seen in pairs(seen_by_channel) do
      for client_id, last_seen_ts in pairs(seen) do
         if (ts - last_seen_ts)/NSPERDAY > LOST_PROFILE_THRESHOLD then
            remove_inactive_client(channel, client_id)
         end
      end
   end
end

local function remove_old_subsessions(ts, metric_by_channel)
   for channel, ss_entry in pairs(metric_by_channel) do
      num_recent_entries = ss_entry.counter:compute("sum", 1)
      num_total_entries = #ss_entry.list

      for i = 1, num_total_entries - num_recent_entries, 1 do
         ss_entry.list.remove(1) -- slow, but does it matter if we run this every hour?
      end
   end
end

function timer_event(ns)
   remove_inactive_clients(ns)
   remove_old_subsessions(ns, creation_delay24h_by_channel)
   remove_old_subsessions(ns, creation_delay7d_by_channel)

   timer_event_client_metric("active profiles last seen", "days", seen_by_channel,
                             seen_history_by_channel, ns,
                             function(ns, v) return math.floor((ns - v)/NSPERDAY) end)
   timer_event_client_metric("creation delay for active profiles ", "hours",
                             creation_delay_by_channel, creation_delay_history_by_channel,
                             ns, function(ns, v) return math.floor(v/NSPERHOUR) end)

   timer_event_subsession_metric("creation delay past 24h", "hours",
                                 creation_delay24h_by_channel,
                                 creation_delay24h_history_by_channel, ns,
                                 function(ns, v) return math.floor(v/NSPERHOUR) end)
   timer_event_subsession_metric("creation delay past 7d", "hours",
                                 creation_delay7d_by_channel,
                                 creation_delay7d_history_by_channel, ns,
                                 function(ns, v) return math.floor(v/NSPERHOUR) end)
end
