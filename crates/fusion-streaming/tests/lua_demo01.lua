#!/usr/local/bin/lua
function echo(param)
    for i, v in ipairs(param) do
        print(i .. "\t" .. v)
    end
    print("this is echo function")
end

echo({ "test", "test2" })

my_table = {}
my_table["id"] = "id01"
my_table["name"] = "name01"
my_table["age"] = 30

print(my_table.id .. "\t" .. my_table.age .. "\t" .. my_table.name)

function hset(key, field, value)
    print("hset " .. key .. " " .. field .. " " .. value)
end

hset("testkey0001", "field001", "value");

local str = "这是一个包含日期的字符串，比如 2025-01-16 和 2023-12-31。"

function _fusion_map_func(row_table)
    return "s" .. val.field1 .. val.field2
end

-- 使用 string.match 提取日期
local date = string.match(str, "%d%d%d%d%-%d%d%-%d%d")

if date then
    print("提取到的日期是:", date)
else
    print("没有找到符合格式的日期")
end

-- 定义一个方法，接受一个字符串参数
function format_string(input)
    -- 获取当前日期，格式化为 yyyy-MM-dd
    local current_date = os.date("%Y-%m-%d")

    -- 返回格式化后的字符串
    return "${" .. input .. "}_${" .. current_date .. "}"
end

-- 测试
local input = "xxx"
local output = format_string(input)
print(output)  -- 输出: ${xxx}_${2023-10-05}（假设当前日期是 2023-10-05）

local tbl1 = {}
tbl1['ss'] = 'saf'
print(tbl1['ss'])