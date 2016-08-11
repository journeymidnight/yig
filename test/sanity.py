import botocore.session
import time
from botocore.client import Config


CONFIG = {
    'access_key': 'hehehehe',
    'secret_key': 'hehehehe',
    'region': 'cn-bj-1',
    'endpoint': 'http://10.75.144.116:3000'
}

clients = {}
session = botocore.session.get_session()
for addressStyle in ['path']: #['path', 'virtual']:
    for signatureVersion in ['s3', 's3v4']:
        client = session.create_client('s3', region_name=CONFIG['region'],
                                       use_ssl=False, verify=False,
                                       endpoint_url=CONFIG['endpoint'],
                                       aws_access_key_id=CONFIG['access_key'],
                                       aws_secret_access_key=CONFIG['secret_key'],
                                       config=Config(
                                           signature_version = signatureVersion,
                                           s3={
                                               'addressing_style': addressStyle,
                                           }
                                       ))
        clients[addressStyle+'-'+signatureVersion] = client

SMALL_TEST_FILE = bytes('a' * 1024 * 1024)  # 1M

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
        Key=name+'hehe',
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
        Body=SMALL_TEST_FILE,
        Bucket=name+'hehe',
        Key=name+'hehe',
        PartNumber=1,
        UploadId=upload_id[name],
    )
    print 'Upload part 1:', ans
    etag_1[name] = ans['ETag']
    ans = client.upload_part(
        Body=SMALL_TEST_FILE,
        Bucket=name+'hehe',
        Key=name+'hehe',
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
        Key=name+'hehe',
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
        Key=name+'hehe',
        MultipartUpload={
            'Parts': [
                {'ETag': etag_1[name], 'PartNumber': 1},
                {'ETag': etag_2[name], 'PartNumber': 2},
            ]
        },
        UploadId=upload_id[name],
    )
    print 'Complete multipart upload:', ans


def delete_bucket(name, client):
        client.delete_bucket(Bucket=name+'hehe')


def delete_bucket_nonexist(name, client):
        client.delete_bucket(Bucket=name+'haha')


# =====================================================

def is_a_fail_test(testFunction):
    fail_name_patterns = ['nonexist']
    for pattern in fail_name_patterns:
        if pattern in testFunction.__name__:
            return True
    return False

TESTS = [create_bucket,
         head_bucket, head_bucket_nonexist,
         list_buckets,
         put_object,
         get_object, get_object_nonexist,
         list_objects_v1, list_objects_v2,
         delete_object,
         create_multipart_upload, upload_part, list_multipart_uploads, list_parts, abort_multipart_upload, complete_multipart_upload,
         delete_bucket, delete_bucket_nonexist]

if __name__ == '__main__':
    good_count = 0
    fail_count = 0
    failed_tests = []
    for t in TESTS:
        for name, client in clients.iteritems():
            print '-' * 60
            e = None
            before = time.time()
            try:
                t(name, client)
            except Exception as e:
                print 'Exception: ', e
            after = time.time()
            print 'Time elapsed: ', after - before, 'sec'
            if (e is None and not is_a_fail_test(t)) or (e and is_a_fail_test(t)):
                print t.__name__ + '()', 'done for', name
                good_count += 1
            else:
                print t.__name__ + '()', 'failed for', name
                fail_count += 1
                failed_tests.append(t.__name__)

    print '=' * 60
    print fail_count, 'out of', good_count + fail_count, 'tests failed'
    if fail_count != 0:
        print 'Failed test(s): '
        print failed_tests

