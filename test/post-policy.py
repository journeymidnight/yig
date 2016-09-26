import base
import config
import sanity

cors_config = {
    'CORSRules': [
        {
            'AllowedHeaders': [
                '*',
            ],
            'AllowedMethods': [
                'POST',
            ],
            'AllowedOrigins': [
                '*',
            ],
        },
    ]
}

HTML = {
    # v2, fields to replace:
    # URL, ACCESS_KEY, POLICY, SIGNATURE
    'path-s3': '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Post Policy Upload - v2</title>
</head>
<body>
<form action="{URL}" method="POST" enctype="multipart/form-data">
    Key to upload: <input type="input" name="key" value="post-policy-${filename}" /><br />
    <input type="hidden" name="acl" value="public-read" />
    <input type="hidden" name="success_action_redirect" value="http://git.letv.cn" />
    Content-Type: <input type="input" name="Content-Type" value="text/plain" /><br />
    <input type="hidden" name="AWSAccessKeyId" value="{ACCESS_KEY}" />
    <input type="hidden" name="Policy" value="{POLICY}" />
    <input type="hidden" name="Signature" value="{SIGNATURE}" />
    File: <input type="file" name="file" /> <br />
    <!-- The elements after this will be ignored -->
    <input type="submit" name="submit" value="Upload to Amazon S3" />
</form>
</body>
</html>
''',
    # v4, fields to replace:
    # URL, CREDENTIAL, DATE, POLICY, SIGNATURE
    'path-s3v4': '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Post Policy Upload - v4</title>
</head>
<body>
<form action="{URL}" method="post" enctype="multipart/form-data">
    Key to upload: <input type="input"  name="key" value="post-policy-${filename}" /><br />
    <input type="hidden" name="acl" value="public-read" />
    <input type="hidden" name="success_action_redirect" value="http://git.letv.cn" />
    Content-Type: <input type="input"  name="Content-Type" value="text/plain" /><br />
    <input type="hidden"   name="X-Amz-Credential" value="{CREDENTIAL}" />
    <input type="hidden"   name="X-Amz-Algorithm" value="AWS4-HMAC-SHA256" />
    <input type="hidden"   name="X-Amz-Date" value="{DATE}" />

    <input type="hidden" name="Policy" value='{POLICY}' />
    <input type="hidden" name="X-Amz-Signature" value="{SIGNATURE}" />
    File: <input type="file" name="file" /> <br />
    <!-- The elements after this will be ignored -->
    <input type="submit" name="submit" value="Upload to Amazon S3" />
</form>
</body>
</html>
'''
}

# =====================================================


def put_bucket_cors(name, client):
    client.put_bucket_cors(
        Bucket=name+'hehe',
        CORSConfiguration=cors_config,
    )


def generate_post_policy(name, client):
    ans = client.generate_presigned_post(
        Bucket=name+'hehe',
        Key='post-policy-${filename}',
        Fields={
            'acl': 'public-read',
            'Content-Type': 'text/plain',
            'success_action_redirect': 'http://git.letv.cn',
        },
        Conditions=[
            {'acl': 'public-read'},
            ['starts-with', '$key', 'post-policy-'],
            ['eq', '$content-type', 'text/plain'],
            ['eq', '$success_action_redirect', 'http://git.letv.cn']
        ]
    )
    print ans
    fields = ans['fields']
    html = HTML[name]
    html = html.replace('{URL}', ans['url']).replace('{POLICY}', fields['policy'])
    if 'v4' in name:
        html = html.replace('{CREDENTIAL}', fields['x-amz-credential'])\
               .replace('{DATE}', fields['x-amz-date'])\
               .replace('{SIGNATURE}', fields['x-amz-signature'])
    else:
        html = html.replace('{ACCESS_KEY}', fields['AWSAccessKeyId'])\
               .replace('{SIGNATURE}', fields['signature'])
    f = open('post-policy-' + name + '.html', 'w')
    f.write(html)
    f.close()
    print '>>> Now open', 'post-policy-' + name + '.html', 'and test in browser.'

# =====================================================

TESTS = [
    sanity.create_bucket,
    put_bucket_cors,
    generate_post_policy,
]

if __name__ == '__main__':
    base.run(TESTS)
