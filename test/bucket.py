import base
import sanity
import requests


# =====================================================


def create_bucket_presigned(name, client):
    url = client.generate_presigned_url(
        ClientMethod='create_bucket',
        Params={
            'Bucket': name+'presigned'
        },
        HttpMethod='PUT',
    )
    print url
    response = requests.put(url)
    print 'Presigned create bucket: ', response


def list_buckets_presigned(name, client):
    url = client.generate_presigned_url(
        ClientMethod='list_buckets',
        HttpMethod='GET'
    )
    print url
    response = requests.get(url)
    print 'Presigned list buckets: ', response

# =====================================================

TESTS = [
#    sanity.create_bucket,
#    sanity.head_bucket, sanity.head_bucket_nonexist,
    create_bucket_presigned,
    sanity.list_buckets,
]

if __name__ == '__main__':
    base.run(TESTS)