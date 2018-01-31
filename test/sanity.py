import base
from urllib import unquote

SMALL_TEST_FILE = bytes('a' * 1024 * 1024)  # 1M
RANGE_1 = bytes('abcdefghijklmnop' * 64 * 1024)  # 1M
RANGE_2 = bytes('qrstuvwxyz012345' * 64 * 1024)  # 1M

# =====================================================


def create_bucket(name, client):
    client.create_bucket(Bucket=name+'hehe')


def head_bucket(name, client):
    client.head_bucket(Bucket=name+'hehe')


def head_bucket_nonexist(name, client):
    client.head_bucket(Bucket=name+'haha')


def list_buckets(name, client):
    ans = client.list_buckets()
    print 'List buckets: ', ans


def put_object(name, client):
    """Single part upload"""
    client.put_object(
        Body=SMALL_TEST_FILE,
        Bucket=name+'hehe',
        Key=name+'hehe'
    )


def get_object(name, client):
    ans = client.get_object(
        Bucket=name+'hehe',
        Key=name+'hehe',
    )
    body = ans['Body'].read()
    assert body == SMALL_TEST_FILE
    print 'Get object:', ans


def put_object_copy(name, client):
    ans = client.copy_object(
        Bucket=name+'hehe',
        Key=name+'hehe-copied',
        CopySource={
            'Bucket': name+'hehe',
            'Key': name+'hehe',
        }
    )
    print 'Copy object:', ans

    # clean-up
    client.delete_object(
        Bucket=name+'hehe',
        Key=name+'hehe-copied',
    )


def get_object_nonexist(name, client):
    client.get_object(
        Bucket=name+'haha',
        Key=name+'haha'
    )


def list_objects_v1(name, client):
    ans = client.list_objects(
        Bucket=name+'hehe'
    )
    print 'List objects:', ans


def list_objects_v2(name, client):
    ans = client.list_objects_v2(
        Bucket=name+'hehe'
    )
    print 'List objects v2:', ans

def _put_obj_with_str(client, bucket, key):
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=bytes(key),
        )

def _get_keys_prefixes(li):
    if li.has_key('Contents'):
           keys = [unquote(x['Key']) for x in li['Contents']]
    else:
        keys = []
    if li.has_key('CommonPrefixes'):
           prefixes = [unquote(x['Prefix']) for x in li['CommonPrefixes']]
    else:
        prefixes = []
    return (keys, prefixes)

def _validate_bucket_list(client, bucket, prefix, delimiter, marker, max_keys,
                          is_truncated, check_objs, check_prefixes, next_marker):
   
    li = client.list_objects(Bucket=bucket, Delimiter=delimiter, Prefix=prefix, MaxKeys=max_keys, Marker=marker)

    assert li['IsTruncated'] == is_truncated
    marker = None
    if li.has_key('NextMarker'):
           marker = unquote(li['NextMarker'])
    assert marker == next_marker

    (keys, prefixes) = _get_keys_prefixes(li)

    assert len(keys) == len(check_objs)
    assert len(prefixes) == len(check_prefixes)

    objs = [e for e in keys]
    assert objs == check_objs

    prefix_names = [e for e in prefixes]
    assert prefix_names == check_prefixes

    return marker

def list_objects_with_delimiter_prefix(name, client):
    bucket = name+'hehe' + "-listobj"
    client.create_bucket(Bucket=bucket)
    _put_obj_with_str(client, bucket, "asdf")
    _put_obj_with_str(client, bucket, "boo/bar")
    _put_obj_with_str(client, bucket, "boo/baz/xyzzy")
    _put_obj_with_str(client, bucket, "cquux/thud")
    _put_obj_with_str(client, bucket, "cquux/bla")

    delim = '/'
    marker = ''
    prefix = ''

    marker = _validate_bucket_list(client, bucket, prefix, delim, '', 1, True, ['asdf'], [], 'asdf')
    marker = _validate_bucket_list(client, bucket, prefix, delim, marker, 1, True, [], ['boo/'], 'boo/')
    marker = _validate_bucket_list(client, bucket, prefix, delim, marker, 1, False, [], ['cquux/'], None)

    marker = _validate_bucket_list(client, bucket, prefix, delim, '', 2, True, ['asdf'], ['boo/'], 'boo/')
    marker = _validate_bucket_list(client, bucket, prefix, delim, marker, 2, False, [], ['cquux/'], None)

    prefix = 'boo/'

    marker = _validate_bucket_list(client, bucket, prefix, delim, '', 1, True, ['boo/bar'], [], 'boo/bar')
    marker = _validate_bucket_list(client, bucket, prefix, delim, marker, 1, False, [], ['boo/baz/'], None)

    marker = _validate_bucket_list(client, bucket, prefix, delim, '', 2, False, ['boo/bar'], ['boo/baz/'], None)

    client.delete_object(Bucket=bucket, Key='asdf')
    client.delete_object(Bucket=bucket, Key='boo/bar')
    client.delete_object(Bucket=bucket, Key='boo/baz/xyzzy')
    client.delete_object(Bucket=bucket, Key='cquux/thud')
    client.delete_object(Bucket=bucket, Key='cquux/bla')
    client.delete_bucket(Bucket=bucket)


def list_object_versions(name, client):
    ans = client.list_object_versions(
        Bucket=name+'hehe'
    )
    print 'List object versions:', ans


def delete_nonempty_bucket_should_fail(name, client):
    ans = client.delete_bucket(
        Bucket=name+'hehe'
    )
    print 'Delete non-empty bucket', ans


def delete_object(name, client):
    client.delete_object(
        Bucket=name+'hehe',
        Key=name+'hehe',
    )


upload_id = {}
upload_id_to_abort = {}
def create_multipart_upload(name, client):
    ans = client.create_multipart_upload(
        Bucket=name+'hehe',
        Key=name+'multipart',
    )
    print 'Create multipart upload:', ans
    global upload_id
    upload_id[name] = ans['UploadId']
    ans = client.create_multipart_upload(
        Bucket=name+'hehe',
        Key=name+'toAbort',
    )
    global upload_id_to_abort
    upload_id_to_abort[name] = ans['UploadId']
    print 'Create multipart upload to abort:', ans


etag_1 = {}
etag_2 = {}
def upload_part(name, client):
    global etag_1
    global etag_2
    ans = client.upload_part(
        Body=RANGE_1,
        Bucket=name+'hehe',
        Key=name+'multipart',
        PartNumber=1,
        UploadId=upload_id[name],
    )
    print 'Upload part 1:', ans
    etag_1[name] = ans['ETag']
    ans = client.upload_part(
        Body=RANGE_2,
        Bucket=name+'hehe',
        Key=name+'multipart',
        PartNumber=2,
        UploadId=upload_id[name],
    )
    print 'Upload part 2:', ans
    etag_2[name] = ans['ETag']


def list_multipart_uploads(name, client):
    ans = client.list_multipart_uploads(
        Bucket=name+'hehe'
    )
    print 'List multipart uploads:', ans


def list_parts(name, client):
    ans = client.list_parts(
        Bucket=name+'hehe',
        Key=name+'multipart',
        UploadId=upload_id[name],
    )
    print 'List multipart upload parts:', ans


def abort_multipart_upload(name, client):
    ans = client.abort_multipart_upload(
        Bucket=name+'hehe',
        Key=name+'toAbort',
        UploadId=upload_id_to_abort[name],
    )
    print 'Abort multipart upload:', ans


def complete_multipart_upload(name, client):
    ans = client.complete_multipart_upload(
        Bucket=name+'hehe',
        Key=name+'multipart',
        MultipartUpload={
            'Parts': [
                {'ETag': etag_1[name], 'PartNumber': 1},
                {'ETag': etag_2[name], 'PartNumber': 2},
            ]
        },
        UploadId=upload_id[name],
    )
    print 'Complete multipart upload:', ans

def put_object_copy_multipart(name, client):
    ans = client.copy_object(
        Bucket=name+'hehe',
        Key=name+'hehe-multipart-copied',
        CopySource={
            'Bucket': name+'hehe',
            'Key': name+'multipart',
        }
    )
    print 'Copy multipart object:', ans

    ans = client.get_object(
        Bucket=name+'hehe',
        Key=name+'hehe-multipart-copied',
    )
    body = ans['Body'].read()
    assert body == RANGE_1 + RANGE_2
    print 'Get object:', ans

    # clean-up
    client.delete_object(
        Bucket=name+'hehe',
        Key=name+'hehe-multipart-copied',
    )


def get_multipart_uploaded_object(name, client):
    ans = client.get_object(
        Bucket=name+'hehe',
        Key=name+'multipart',
    )
    body = ans['Body'].read()
    assert body == RANGE_1 + RANGE_2
    print 'Get object:', ans


def delete_multipart_object(name, client):
    client.delete_object(
        Bucket=name+'hehe',
        Key=name+'multipart',
    )


def get_object_ranged(name, client):
    ans = client.get_object(
        Bucket=name+'hehe',
        Key=name+'multipart',
        Range='bytes=1048570-1048580'
    )
    body = ans['Body'].read()
    assert body == 'klmnopqrstu'
    print 'Get object:', ans

cors_config = {
    'CORSRules': [
        {
            'AllowedHeaders': [
                'hehe',
            ],
            'AllowedMethods': [
                'GET',
            ],
            'AllowedOrigins': [
                '*',
            ],
            'ExposeHeaders': [
                'hehe',
            ],
            'MaxAgeSeconds': 123
        },
    ]
}
def put_bucket_cors(name, client):
    client.put_bucket_cors(
        Bucket=name+'hehe',
        CORSConfiguration=cors_config,
    )


def get_bucket_cors(name, client):
    ans = client.get_bucket_cors(
        Bucket=name+'hehe',
    )
    assert ans['CORSRules'] == cors_config['CORSRules']
    print 'Get bucket CORS:', ans


def delete_bucket_cors(name, client):
    client.delete_bucket_cors(
        Bucket=name+'hehe'
    )


def put_bucket_versioning(name, client):
    client.put_bucket_versioning(
        Bucket=name+'hehe',
        VersioningConfiguration={
            'Status': 'Enabled'
        }
    )


def get_bucket_versioning(name, client):
    ans = client.get_bucket_versioning(
        Bucket=name+'hehe'
    )
    assert ans['Status'] == 'Enabled'
    print 'Get bucket versioning:', ans


def delete_bucket(name, client):
    client.delete_bucket(Bucket=name+'hehe')


def delete_bucket_nonexist(name, client):
    client.delete_bucket(Bucket=name+'haha')


# =====================================================


TESTS = [create_bucket,
         head_bucket, head_bucket_nonexist,
         list_buckets,
         put_object,
         put_object_copy,
         get_object, get_object_nonexist,
         list_objects_v1, list_objects_v2, list_object_versions,list_objects_with_delimiter_prefix,
         delete_nonempty_bucket_should_fail,
         delete_object,
         create_multipart_upload, upload_part, list_multipart_uploads, list_parts, abort_multipart_upload, complete_multipart_upload,
	 put_object_copy_multipart,
         get_multipart_uploaded_object,
         get_object_ranged,
         delete_multipart_object,
         put_bucket_cors, get_bucket_cors, delete_bucket_cors,
         put_bucket_versioning, get_bucket_versioning,
         delete_bucket, delete_bucket_nonexist]

if __name__ == '__main__':
    base.run(TESTS)
