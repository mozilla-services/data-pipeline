require "io"
require "string"

local PARTITIONS = 16 
local fhs = {}
for i=1, PARTITIONS do
    fhs[i] = assert(io.open(string.format("xa%c", 96 + i ), "w+"))
end
local cnt = 0

for line in io.lines("list.txt") do
    local idx = cnt % PARTITIONS + 1
    fhs[idx]:write(line, "\n")
    cnt = cnt + 1
end

for i=1, PARTITIONS do
    fhs[i]:close()
end
