-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Input to read entries from the systemd journal

Config:
    matches = "[]"
    embedded JSON array of matches to apply. See
    http://www.freedesktop.org/software/systemd/man/journalctl.html for a
    description of matches. By default, all journal entries that the user
    running Heka has access to are matched.

    use_fields = true
    whether to map systemd fields to top-level Heka fields
    e.g. _HOSTNAME -> Fields[Hostname] etc. This is superseded by
    process_module_entry_point.

    process_module = nil
    module to load (if any) to further transform messages before injecting
    (similar to a decoder)

    process_module_entry_point = nil
    method to call handing off the current table for further decoding. It is
    expected that this method call inject_message().

    offset_method = "manual_oldest"
    The method used to determine at which offset to begin consuming messages.
    The valid values are:

    - *manual_oldest*
       Heka will track the offset and resume from where it last left off, or
       else from the beginning of the journal if no checkpoint file exists
       (default).
    - *manual_newest*
       Heka will track the offset and resume from where it last left off, or
       else from the end of the journal if no checkpoint file exists.
    - *newest*
       Heka will start reading from the most recent available offset.
    - *oldest*
       Heka will start reading from the oldest available offset.

    offset_file = nil (required if offset_method is "manual_oldest" or "manual_newest")
    File to store the checkpoint in. Must be unique. Currently the sandbox API
    does not provide access to Logger information so this file must be
    specified explicitly.

*Example Heka Configuration*

.. code-block:: ini

    [SystemdInput]
    type = "SandboxInput"
    filename = "lua_inputs/systemd.lua"

        [SystemdInput.config]
        matches = '["_SYSTEMD_UNIT=fxa-auth.service"]'
        offset_method = "newest"
--]]

require "io"
require "os"
require "cjson"
local s = require "string"
local format = s.format
local dbg = require "debug"
local function debug (...)
    dbg.debug("SystemdInput: " .. format(...))
end

local sj = require "systemd.journal"

-- TODO support disjunction
local matches = cjson.decode(read_config("matches") or "[]")

local offset_method = read_config("offset_method") or "manual_oldest"
local cursor, checkpoint_file
if offset_method == "manual_newest" or offset_method == "manual_oldest" then
    checkpoint_file = read_config("offset_file") or error("must specify offset_file")
elseif offset_method and not (
    offset_method == "newest" or offset_method == "oldest") then
    error(format("offset_method must be one of '%s', '%s', or '%s'",
                 "manual_newest", "manual_oldest", "newest", "oldest"))
end

-- Try to format systemd's default MESSAGE_ID field into a format heka
-- supports. *Note* this value is potentially user-supplied and might be
-- garbage.
local function format_uuid(str)
    local s = ""
    local clean = str:gsub("-", "")
    if not clean:match("^%x+$") then return str end
    for i in string.gmatch(clean, "(..)") do
        s = s .. string.char(tonumber(i, 16))
    end
    return s
end

-- This is a cursory attempt at making a more "heka-ish"
-- message. process_module_entry_point can be used to for arbitrary mappings
-- and transformations.
local fields_map = {
    MESSAGE = "Payload"
    , MESSAGE_ID = {
        "Uuid",
        function(x) format_uuid(x) end
    }
    , _HOSTNAME = "Hostname"
    , _SOURCE_REALTIME_TIMESTAMP = {
        "Timestamp",
        function (i) return i * 1e3 end
    }
    , __REALTIME_TIMESTAMP = {
        "Timestamp",
        function (i) return i * 1e3 end
    }
    , _PID = "PID"
    , PRIORITY = "Severity"
    , _SYSTEMD_UNIT = "Logger"
    , _SYSTEMD_USER_UNIT = "Logger"
    , SYSLOG_IDENTIFIER = "Logger"
}

-- see
-- http://www.freedesktop.org/software/systemd/man/systemd.journal-fields.html
-- for a list of fields. Currently "any-wins" semantics when multiple journal
-- fields map to a single heka one since tables are traversed in arbitrary
-- order.
local function map_fields(tbl)
    for key, value in pairs(fields_map) do
        local newkey, method
        if type(value) == "table" then
            newkey, method = unpack(value)
        else
            newkey = value
        end

        local v = tbl.Fields[key]
        if v then
            if method then
                tbl[newkey] = method(v)
            else
                tbl[newkey] = v
            end
            tbl.Fields[key] = nil
        end
    end
    inject_message(tbl)
    return 0
end

local mod = read_config("process_module")
local decoder_name = read_config("process_module_entry_point")
if mod and not decoder_name then
    error(format(
              "must provide process_module_entry_point for module %s", mod))
elseif mod then
    mod = require(mod)
    decoder = mod[decoder_name] or error(
        format("can't find function %s in module %s",
                      decoder_name, mod))
elseif read_config("use_fields") then
    decoder = map_fields
    decoder_name = "map_fields"
end

function process_message()
    local j = assert(sj.open())
    j:set_data_threshold(0)
    local checkpoint = offset_method:match("manual")

    local fh, cursor
    if checkpoint then
        fh = io.open(checkpoint_file, "r")
        if fh then
            -- the systemd cursor is variable length, so we write out the cursor
            -- with a newline to avoid having to truncate the checkpoint file
            cursor = fh:read("*line")
            if cursor then
                debug("cursor: %s", cursor)
            else
                debug("empty checkpoint file")
            end
            fh:close()
            fh = assert(io.open(checkpoint_file, "r+"))
        else
            debug("checkpoint file %s doesn't exist, creating",
                  checkpoint_file)
            fh = assert(io.open(checkpoint_file, "w+"))
        end
        fh:setvbuf("no")
    end

    for i, match in ipairs(matches) do
        debug("adding match %s", match)
        assert(j:add_match(match))
    end

    debug("using offset method '%s'", offset_method)
    if cursor then
        if not j:seek_cursor(cursor) then
            fh:close()
            os.remove(checkpoint_file)
            error(format("failed to seek to cursor %s, removing checkpoint and stopping", cursor))
        end
        -- Note that [seek_cursor] does not actually make any entry the new
        -- current entry, this needs to be done in a separate step with a
        -- subsequent sd_journal_next(3) invocation (or a similar call)
        j:next()

        -- maybe issue a warning instead if this fails
        assert(j:test_cursor(cursor))
    elseif offset_method:match("newest") then
        assert(j:seek_tail())
        j:previous()
    elseif offset_method:match("oldest") then
        assert(j:seek_head())
        j:next()
    end

    local ready = j:next()

    while true do
        while not ready do
            if j:wait(1) ~= sj.WAKEUP.NOP then
                ready = j:next()
            end
        end

        local cursor = j:get_cursor()

        local msg = {
            Type = "heka.systemd",
            Timestamp = os.time() * 1e9,
            Fields = j:to_table()
        }
        if decoder then
            local r, err = decoder(msg)
            if r ~= 0 then
                debug("%s failed: %s", decoder_name, err or "")
            end
        else
            inject_message(msg)
        end

        if checkpoint then
            local offset, err = fh:seek("set")
            if err then error(err) end
            assert(offset == 0)
            fh:write(cursor .. "\n")
            fh:flush()
        end

        ready = j:next()
    end
    fh:close()
    return 0
end
