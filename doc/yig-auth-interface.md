#YIG Auth Interface

##DescribeAccessKeys

Get SecretKeys by AccessKeys from Iam

###Request Syntax
```
POST / HTTP/1.1
X-Le-Key: key
X-Le-Secret: secret
content-type: application/json

body
{
"action": "DescribeAccessKeys",
"accessKeys":["hehehehe"]
}
```

X-Le-Key && X-Le-Secret are used to identify if requests is legal.

####Response
```

{
    "total":1,
    "accessKeySet":[
        {
            "projectId":"p-abcdef",
            "name":"user1",
            "accessKey":"hehehehe",
            "accessSecret":"hehehehe",
            "status":"active",
            "updated":"2006-01-02T15:04:05Z07:00"
        }
    ]
}

```