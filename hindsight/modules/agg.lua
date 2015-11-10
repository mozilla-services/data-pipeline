-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

local M = {}
local type = type
local pairs = pairs
setfenv(1, M) -- Remove external access to contain everything in the module

-- Merge two objects. Add all data from "src" to "dest". Numeric values are
-- added, boolean and string values are overwritten, and arrays and objects are
-- recursively merged.
-- Any data with different types in dest and src will be skipped.
-- Example:
--local a = {
--    foo = 1,
--    bar = {1, 1, 3},
--    quux = 3
--}
--local b = {
--    foo = 5,
--    bar = {0, 0, 5, 1},
--    baz = {
--        hello = 100
--    }
--}
--
--local c = merge_objects(a, b)
---------
-- c contains {
--    foo = 5,
--    bar = {1, 1, 8, 1},
--    baz = {
--        hello = 100
--    },
--    quux = 3
--}
function merge_objects(dest, src)
    if dest == nil then
        return src
    end
    if src == nil then
        return dest
    end

    local tdest = type(dest)
    local tsrc = type(src)

    -- Types are different. Ignore the src value, because src is wrong.
    if tdest ~= tsrc then
        return dest
    end

    -- types are the same, neither is nil.
    if tdest == "number" then
        return dest + src
    end

    -- most recent wins:
    if tdest == "boolean" or tdest == "string" then
        return src
    end

    if tdest == "table" then
        -- array or object, iterate by key
        for k,v in pairs(src) do
            dest[k] = merge_objects(dest[k], v)
        end
        return dest
    end

    -- How did we get here?
    --print("weird type: ", tdest, "\n")
    return dest
end

return M
