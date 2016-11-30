-- Stolen from https://github.com/jamesmarlowe/lua-resty-hmac
-- Copyright (C) James Marlowe (jamesmarlowe), Lumate LLC.


local crypto = require("sha1")


-- obtain a copy of enc() here: http://lua-users.org/wiki/BaseSixtyFour
function enc(data)
    -- character table string
    local b='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
    return ((data:gsub('.', function(x)
        local r,b='',x:byte()
        for i=8,1,-1 do
            r=r..(b%2^i-b%2^(i-1)>0 and '1' or '0')
        end
        return r;
    end)..'0000'):gsub('%d%d%d?%d?%d?%d?', function(x)
        if (#x < 6) then return '' end
        local c=0
        for i=1,6 do
            c=c+(x:sub(i,i)=='1' and 2^(6-i) or 0)
        end
        return b:sub(c+1,c+1)
    end)..({ '', '==', '=' })[#data%3+1])
end


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = new_tab(0, 155)
_M._VERSION = '0.01'


local hash_algorithms = {
    md5 = 1, md4 = 1, md2 = 1, sha1 = 1, sha = 1, sha256 = 1, mdc2 = 1, ripemd160 = 1
}


local months = {Jan = 1,Feb = 2,Mar = 3,Apr = 4, May = 5, Jun = 6,
    Jul = 7,Aug = 8,Sep = 9,Oct = 10,Nov = 11,Dec = 12}


local mt = { __index = _M }


function _M.new(self, secret)
    local secret = secret
    if not secret then
        return nil, "must provide secret"
    end
    return setmetatable({ secret = secret }, mt)
end


function _M.generate_signature(self, dtype, message, delimiter)
    local secret = self.secret
    if not secret then
        return nil, "not initialized"
    end

    if type(dtype) ~= "string" or not hash_algorithms[dtype] then
        dtype = "sha1"
    end

    if type(delimiter) ~= "string" then
        delimiter = string.char(10)
    end

    local StringToSign = ""

    if type(message) == "table" then
        table.sort(message)
        for k, v in pairs(message) do
            StringToSign = StringToSign..v..(k ~= #message and delimiter or "")
        end
    elseif type(message) == "string" then
        StringToSign = message
    else
        return nil, "invalid message"
    end

    -- local sig_hmac  =  crypto.hmac.digest(dtype, StringToSign, secret, true)
    local sig_hmac = hmac_sha1_binary(secret, StringToSign)
    local signature  =  enc(sig_hmac)

    self.StringToSign = StringToSign
    self.sig_hmac = sig_hmac
    self.signature = signature

    return signature
end


function _M.check_signature(self, dtype, message, delimiter, signature)
    local secret = self.secret
    if not secret then
        return nil, "not initialized"
    end

    local new_signature, err = self:generate_signature(dtype, message, delimiter)
    if not new_signature then
        return new_signature, err
    end

    local correct = (new_signature == signature)

    return correct, (correct and "signature matches" or "not a match")
end


function _M.generate_headers(self, service, id, dtype, message, delimiter)
    local secret = self.secret
    if not secret then
        return nil, "not initialized"
    end

    local signature = self.signature
    if not signature then
        signature, err = self:generate_signature(dtype, message, delimiter)
        if signature then
            self.signature = signature
        else
            return nil, err
        end
    end

    if type(service) ~= "string" then
        service = "AWS"
    end

    if type(id) ~= "string" then
        return nil, "no id provided"
    end

    local date = os.date("!%a, %d %b %Y %H:%M:%S +0000")
    local auth = service.." "..id..":"..signature

    self.date = date
    self.auth = auth

    return {date = date, auth = auth}
end


function _M.check_headers(self, service, id, dtype, message, delimiter, max_time_diff)
    local request_headers = ngx.req.get_headers()
    local authorization = request_headers.authorization
    local date2 = request_headers.date

    if not authorization or not date2 then
        return nil, "missing headers"
    end

    local req_service,req_id,req_signature = authorization:match("(%S+) (%S+):(%S+)")

    if req_service ~= service or req_id ~= id then
        return nil, "incorrect authorization"
    end

    local day, month, year, hour, minute, second =
    date2:match("%a+, (%d+) (%a+) (%d+) (%d+):(%d+):(%d+)")
    month = months[month]

    local date1 = os.date("!*t")
    date1.isdst = nil
    local timestamp1 = os.time(date1)
    local timestamp2 = os.time({month = month,   year = year,
        day   = day,     hour = hour,
        min   = minute,  sec  = second})

    if type(max_time_diff) ~= "number" then
        max_time_diff  =  300 -- 5 minutes
    end

    if (timestamp2 - timestamp1) > max_time_diff then
        return nil, "time difference too great"
    end

    signature, err = self:check_signature(dtype, message, delimiter, req_signature)
    if not signature then
        return signature, err
    end

    return true
end


return _M