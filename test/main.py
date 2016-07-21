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


TESTS = [create_bucket, ]
if __name__ == '__main__':
    for t in TESTS:
        t()
