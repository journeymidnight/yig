import botocore.session
from botocore.client import Config
from config import CONFIG
import time

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
                                           signature_version=signatureVersion,
                                           s3={
                                               'addressing_style': addressStyle,
                                           }
                                       ))
        clients[addressStyle+'-'+signatureVersion] = client


def is_a_fail_test(testFunction):
    fail_name_patterns = ['nonexist', 'should_fail']
    for pattern in fail_name_patterns:
        if pattern in testFunction.__name__:
            return True
    return False


def run(tests):
    good_count = 0
    fail_count = 0
    failed_tests = []
    for t in tests:
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