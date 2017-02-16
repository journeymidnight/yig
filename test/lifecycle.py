import base
import sanity
import os
import time
import botocore



def put_bucket_lifecycle(name, client):
    print "bucketname:" + name
    client.put_bucket_lifecycle(Bucket=name+'hehe', LifecycleConfiguration={
                                                                'Rules': [
                                                                    {
                                                                        'Expiration': {
                                                                            'Days': 1,
                                                                        },
                                                                        'ID': 'test',
                                                                        'Prefix': '',
                                                                        'Status': 'Enabled',
                                                                    },
                                                                ]
                                                            })

def del_bucket_lifecycle(name, client):
    client.delete_bucket_lifecycle(Bucket=name+'hehe')

def do_lc(name, client):
    os.system('../lc')
    time.sleep(3)

def get_bucket_lifecycle(name, client):
    response = client.get_bucket_lifecycle(Bucket=name+'hehe')
    print 'lifecycle config: ', response

def get_object(name, client):
    try:
        response = client.get_object(
            Bucket=name+'hehe',
            Key=name+'hehe',
        )
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print "object not existed"
        else:
            print "Unexpected error: %s" % e

TESTS = [
    sanity.create_bucket,
    sanity.put_object,
    put_bucket_lifecycle,
    get_bucket_lifecycle,
    sanity.get_object,
    do_lc,
    get_object,
    del_bucket_lifecycle,
    sanity.delete_bucket
]

if __name__ == '__main__':
    base.run(TESTS)
