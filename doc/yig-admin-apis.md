#YIG Admin Api Quick Guide

This is a demonstration for users how to send admin requests to yig
through Restful apis other than admin tools .


## Dependency

ALL admin api requests must be signed by jwt.

Jwt Headers always are 

```
{
  "alg": "HS256",
  "typ": "JWT"
}
```

Jwt Payload could be subset of these below 

```
{
  "bucket": "bucket_sample",
  "object": "object_sample",
  "uid": "user_id_sample"
}
```

And the most important thing is that signed key must be the same as "AdminKey" specified in yig.json usually located at /etc/yig/yig.json.

Available JWT libs below:

Go: [github.com/dgrijalva/jwt-go]()


More information Please follow JWT [https://jwt.io/introduction/]()

##API

###Get Bucket Usage

Get total usage of a bucket.

####Request Syntax
```
GET /admin/usage HTTP/1.1
Host: s3.test.com
Date: date
Authorization: Bearer {token}
```

#### Jwt payload
```
{
  "bucket": "test"
}
```

####Response
```
{"Usage":1027}

```

###Get Bucket Info

Get infomation of a bucket.

####Request Syntax
```
GET /admin/bucket HTTP/1.1
Host: s3.test.com
Date: date
Authorization: Bearer {token}
```

#### Jwt payload
```
{
  "bucket": "test"
}
```

####Response
```
{
    "Bucket": {
        "Name": "test", 
        "CreateTime": "2018-11-19T03:26:43Z", 
        "OwnerId": "hehehehe", 
        "CORS": {
            "CorsRules": null
        }, 
        "ACL": {
            "CannedAcl": "private"
        }, 
        "LC": {
            "XMLName": {
                "Space": "", 
                "Local": ""
            }, 
            "Rule": null
        }, 
        "Versioning": "Disabled", 
        "Usage": 1027
    }
}

```

###Get User Info

Get infomation of a user.

####Request Syntax
```
GET /admin/user HTTP/1.1
Host: s3.test.com
Date: date
Authorization: Bearer {token}
```

#### Jwt payload
```
{
  "uid": "hehehehe"
}
```

####Response
```
{"Buckets":["test"],"Keys":null}

```

###Get Object Info

Get infomation of a object.

####Request Syntax
```
GET /admin/object HTTP/1.1
Host: s3.test.com
Date: date
Authorization: Bearer {token}
```

#### Jwt payload
```
{
  "bucket": "test",
  "object": "README"
}
```

####Response
```
{
    "Object": {
        "Rowkey": "Cgrql5Wir6Y2nw==", 
        "Name": "README", 
        "BucketName": "test", 
        "Location": "77ca01c4-ce08-4a98-af48-325474b0ecfc", 
        "Pool": "rabbit", 
        "OwnerId": "hehehehe", 
        "Size": 1027, 
        "ObjectId": "4122:7", 
        "LastModifiedTime": "2018-11-19T03:57:01.3869592Z", 
        "Etag": "4fd134c42915ea6734a5d9b56441c447", 
        "ContentType": "binary/octet-stream", 
        "CustomAttributes": { }, 
        "Parts": { }, 
        "PartsIndex": null, 
        "ACL": {
            "CannedAcl": "private"
        }, 
        "NullVersion": true, 
        "DeleteMarker": false, 
        "VersionId": "75af1323755e2cce9e28852b67a2b3e57c740bdf46b3f0bd", 
        "SseType": "", 
        "EncryptionKey": "", 
        "InitializationVector": ""
    }
}

```






