-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "string"
local gzip = require "gzip"

function process_message()
    local payload = read_message("Payload")
    local b1, b2 = string.byte(payload, 1, 2)

    if b1 == 0x1f and b2 == 0x8b then  -- test for gzip magic header bytes
        local ok, result = pcall(gzip.decompress, payload)
        if not ok then
            return -1, result
        end
        write_message("Payload", result)
    end

    return 0
end
