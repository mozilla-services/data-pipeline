-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- Imports
local ipairs = ipairs
local type = type

local M = {}
setfenv(1, M) -- Remove external access to contain everything in the module

local country_names = {
"Other","AD","AE","AF","AG","AI","AL","AM","AO","AQ","AR","AS","AT","AU","AW","AX","AZ","BA","BB","BD","BE","BF","BG","BH","BI","BJ","BL","BM","BN","BO","BQ","BR","BS","BT","BV","BW","BY","BZ","CA","CC","CD","CF","CG","CH","CI","CK","CL","CM","CN","CO","CR","CU","CV","CW","CX","CY","CZ","DE","DJ","DK","DM","DO","DZ","EC","EE","EG","EH","ER","ES","ET","FI","FJ","FK","FM","FO","FR","GA","GB","GD","GE","GF","GG","GH","GI","GL","GM","GN","GP","GQ","GR","GS","GT","GU","GW","GY","HK","HM","HN","HR","HT","HU","ID","IE","IL","IM","IN","IO","IQ","IR","IS","IT","JE","JM","JO","JP","KE","KG","KH","KI","KM","KN","KP","KR","KW","KY","KZ","LA","LB","LC","LI","LK","LR","LS","LT","LU","LV","LY","MA","MC","MD","ME","MF","MG","MH","MK","ML","MM","MN","MO","MP","MQ","MR","MS","MT","MU","MV","MW","MX","MY","MZ","NA","NC","NE","NF","NG","NI","NL","NO","NP","NR","NU","NZ","OM","PA","PE","PF","PG","PH","PK","PL","PM","PN","PR","PS","PT","PW","PY","QA","RE","RO","RS","RU","RW","SA","SB","SC","SD","SE","SG","SH","SI","SJ","SK","SL","SM","SN","SO","SR","SS","ST","SV","SX","SY","SZ","TC","TD","TF","TG","TH","TJ","TK","TL","TM","TN","TO","TR","TT","TV","TW","TZ","UA","UG","UM","US","UY","UZ","VA","VC","VE","VG","VI","VN","VU","WF","WS","YE","YT","ZA","ZM","ZW"}
local country_ids = {}
for i, v in ipairs(country_names) do
    country_ids[v] = i - 1
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


function get_default(dflt)
    if type(dflt) == "boolean" then
        return dflt
    end
    return false
end

return M
