import botocore.session
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


def create_bucket():
    for name, c in clients.iteritems():
        try:
            c.create_bucket(Bucket=name+'hehe')
        except Exception as e:
            print 'create_bucket() failed for ', name
            print "Exception: ", e
        else:
            print 'create_bucket() done for ', name


def head_bucket():
    for name, c in clients.iteritems():
        try:
            c.head_bucket(Bucket=name+'hehe')
        except Exception as e:
            print 'head_bucket() failed for ', name
            print 'Exception: ', e
        else:
            print 'head_bucket() done for ', name


def head_bucket_nonexist():
    for name, c in clients.iteritems():
        try:
            c.head_bucket(Bucket=name+'haha')
        except Exception as e:
            print 'head_bucket_nonexist() done for ', name
            print 'Exception: ', e
        else:
            print 'head_bucket_nonexist() failed for ', name


def list_buckets():
    for name, c in clients.iteritems():
        try:
            ans = c.list_buckets()
        except Exception as e:
            print 'list_buckets() failed for', name
            print 'Exception: ', e
        else:
            print 'list_buckets() done: ', ans


def delete_bucket():
    for name, c in clients.iteritems():
        try:
            c.delete_bucket(Bucket=name+'hehe')
        except Exception as e:
            print 'delete_bucket() failed for ', name
            print 'Exception: ', e
        else:
            print 'delete_bucket() done for ', name


def delete_bucket_nonexist():
    for name, c in clients.iteritems():
        try:
            c.delete_bucket(Bucket=name+'haha')
        except Exception as e:
            print 'delete_bucket_nonexist() done for ', name
            print 'Exception: ', e
        else:
            print 'delete_bucket_nonexist() failed for ', name

TESTS = [create_bucket,
         head_bucket, head_bucket_nonexist,
         list_buckets,
         delete_bucket, delete_bucket_nonexist]
if __name__ == '__main__':
    for t in TESTS:
        t()
