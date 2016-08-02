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
        Body=bytes('a' * 1024 * 1024),  # 1M
        Bucket=name+'hehe',
        Key=name+'hehe'
    )


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

