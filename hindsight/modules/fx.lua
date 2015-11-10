-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- Imports
local ipairs = ipairs
local type = type
local l = require "lpeg"
require "string"
local upper = string.upper
local tonumber = tonumber

local M = {}
setfenv(1, M) -- Remove external access to contain everything in the module

local normalize_channel_grammar =
l.C"release" * -1 +
l.C"beta" +
(l.P("nightly") * -1 + "nightly-cck-") / "nightly" +
l.C"aurora" * -1 +
l.Cc"Other"

function normalize_channel(name)
    if type(name) ~= "string" then name = "" end
    return normalize_channel_grammar:match(name)
end

local function anywhere (p)
  return l.P{ p + 1 * l.V(1) }
end

local normalize_os_grammar =
(l.P"Windows" + "WINNT") / "Windows" +
l.P"Darwin" / "Mac" +
(anywhere"Linux" + anywhere"BSD" + anywhere"SunOS") / "Linux" +
l.Cc"Other"

function normalize_os(name)
    if type(name) ~= "string" then name = "" end
    return normalize_os_grammar:match(name)
end

-- https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
local country_names = {
    "Other","AD","AE","AF","AG","AI","AL","AM","AO","AQ","AR","AS","AT","AU",
    "AW","AX","AZ","BA","BB","BD","BE","BF","BG","BH","BI","BJ","BL","BM","BN",
    "BO","BQ","BR","BS","BT","BV","BW","BY","BZ","CA","CC","CD","CF","CG","CH",
    "CI","CK","CL","CM","CN","CO","CR","CU","CV","CW","CX","CY","CZ","DE","DJ",
    "DK","DM","DO","DZ","EC","EE","EG","EH","ER","ES","ET","FI","FJ","FK","FM",
    "FO","FR","GA","GB","GD","GE","GF","GG","GH","GI","GL","GM","GN","GP","GQ",
    "GR","GS","GT","GU","GW","GY","HK","HM","HN","HR","HT","HU","ID","IE","IL",
    "IM","IN","IO","IQ","IR","IS","IT","JE","JM","JO","JP","KE","KG","KH","KI",
    "KM","KN","KP","KR","KW","KY","KZ","LA","LB","LC","LI","LK","LR","LS","LT",
    "LU","LV","LY","MA","MC","MD","ME","MF","MG","MH","MK","ML","MM","MN","MO",
    "MP","MQ","MR","MS","MT","MU","MV","MW","MX","MY","MZ","NA","NC","NE","NF",
    "NG","NI","NL","NO","NP","NR","NU","NZ","OM","PA","PE","PF","PG","PH","PK",
    "PL","PM","PN","PR","PS","PT","PW","PY","QA","RE","RO","RS","RU","RW","SA",
    "SB","SC","SD","SE","SG","SH","SI","SJ","SK","SL","SM","SN","SO","SR","SS",
    "ST","SV","SX","SY","SZ","TC","TD","TF","TG","TH","TJ","TK","TL","TM","TN",
    "TO","TR","TT","TV","TW","TZ","UA","UG","UM","US","UY","UZ","VA","VC","VE",
    "VG","VI","VN","VU","WF","WS","YE","YT","ZA","ZM","ZW"}
local country_ids = {}
for i, v in ipairs(country_names) do
    country_ids[v] = i - 1
end

function normalize_country(name)
    if type(name) == "string" then
        name = upper(name)
    else
        name = ""
    end

    if not country_ids[name] then
        return country_names[1]
    end
    return name
end

local channel_names = {"Other", "release", "beta", "nightly", "aurora"}
local channel_ids = {}
for i, v in ipairs(channel_names) do
    channel_ids[v] = i - 1
end

local os_names = {"Other", "Windows", "Mac", "Linux"}
local os_ids = {}
for i, v in ipairs(os_names) do
    os_ids[v] = i - 1
end

local os_win_names = {"Other", "Windows 10", "Windows 8", "Windows 7"}
local os_win_ids = {}
for i, v in ipairs(os_win_names) do
    os_win_ids[v] = i - 1
end


function get_country_count()
    return #country_names
end

function get_country_name(id)
    if id then
        id = id + 1
    else
        id = 1
    end

    local name = country_names[id]
    if name then return name end

    return country_names[1]
end

function get_channel_count()
    return #channel_names
end

function get_channel_name(id)
    if id then
        id = id + 1
    else
        id = 1
    end

    local name = channel_names[id]
    if name then return name end

    return channel_names[1]
end

function get_os_count()
    return #os_names
end

function get_os_name(id)
    if id then
        id = id + 1
    else
        id = 1
    end

    local name = os_names[id]
    if name then return name end

    return os_names[1]
end

function get_os_win_count()
    return #os_win_names
end

function get_os_win_name(id)
    if id then
        id = id + 1
    else
        id = 1
    end

    local name = os_win_names[id]
    if name then return name end

    return os_win_names[1]
end


function get_country_id(name)
    if not name then return 0 end

    local id = country_ids[name]
    if id then return id end

    return 0
end

function get_channel_id(name)
    if not name then return 0 end

    local id = channel_ids[name]
    if id then return id end

    return 0
end

function get_os_id(name)
    if not name then return 0 end

    local id = os_ids[name]
    if id then return id end

    return 0
end

function get_os_win_id(version)
    local ver = tonumber(version) or 0

    local id = os_win_ids["Other"]
    if ver >= 10 and ver < 11 then
        id = os_win_ids["Windows 10"]
    elseif ver == 6.2 or ver == 6.3 then
        id = os_win_ids["Windows 8"]
    elseif ver == 6.1 then
        id = os_win_ids["Windows 7"]
    end

    return id
end


function get_boolean_value(v)
    if type(v) == "boolean" then
        return v
    end
    return false
end


return M
