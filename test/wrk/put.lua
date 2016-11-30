-- Part of code stolen from https://github.com/gcr/lua-s3

local hmac = require('hmac')


config = {
    ACCESS_KEY='K37ThoXzqN8CDqWhyM2x',
    SECRET_KEY='dkCgPPDL9ZVAA5xaLFBazVRhqmRtF2XY3rYWjCef',
    BUCKET='smallfiles',
}

local function generateAuthHeaders(awsVerb, awsId, awsKey, awsToken, md5, acl, type, destination)
    -- Thanks to https://github.com/jamesmarlowe/lua-resty-s3/, BSD license.
    -- Used to sign S3 requests.
    -- awsToken, md5, acl, type are all optional.
    local id, key = awsId, awsKey
    local date = os.date("!%a, %d %b %Y %H:%M:%S +0000")
    local hm, err = hmac:new(key)
    local StringToSign = (awsVerb..string.char(10)..
            (md5 or "")..string.char(10)..
            (type or "")..string.char(10)..
            string.char(10).. -- passing date as an x-amz header
            (acl and "x-amz-acl:"..acl..string.char(10) or "")..
            "x-amz-date:"..date..string.char(10)..
            (awsToken and "x-amz-security-token:"..awsToken..string.char(10) or "")..
            destination)
    --print(StringToSign)
    headers, err = hm:generate_headers("AWS", id, "sha1", StringToSign)
    headers.date = date
    return headers, err
end

function put(key, data)
    local acl = "public-read"
    local bucketname = config.BUCKET

    local path = "/" .. bucketname .. "/" .. key

    -- 'enc' is a global function from hmac.lua that encodes the result
    -- in base64.
    -- local md5 = enc(crypto.digest("md5", data, true))
    local md5 = nil -- md5 could be empty
    local authHeaders = generateAuthHeaders("PUT", config.ACCESS_KEY, config.SECRET_KEY, nil,
        md5,
        acl,
        nil,
        path)
    local headers = {
        ["x-amz-date"]=authHeaders.date,
        ["x-amz-acl"]=acl, -- optional
        authorization=authHeaders.auth,
        -- ["content-md5"]=md5,
        -- ["content-length"]=#data
    }
    return wrk.format("PUT", path, headers, data)
end

local file = io.open('4k', "rb")
if file == nil then
    print("error")
    return nil
end
local content = file:read "*a"
file:close()


local threadId = 1

function setup(thread)
    thread:set("id", threadId)
    threadId = threadId + 1
end

function init(args)
    count = 0
end

function request()
    count = count + 1
    local key = tostring(id) .. "_" .. tostring(count)
    return put(key, content)
end