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
    print 'Presigned create bucket: ', response.text
    assert response.status_code == 200


def list_buckets_presigned(name, client):
    url = client.generate_presigned_url(
        ClientMethod='list_buckets',
        HttpMethod='GET'
    )
    print url
    response = requests.get(url)
    print 'Presigned list buckets: ', response.text
    assert response.status_code == 200


def list_buckets_presigned_expired(name, client):
    url = client.generate_presigned_url(
        ClientMethod='list_buckets',
        HttpMethod='GET',
        ExpiresIn=-3600,
    )
    print url
    response = requests.get(url)
    print 'Presigned list buckets: ', response.text
    assert response.status_code != 200


def put_bucket_acl(name, client):
    client.put_bucket_acl(
        Bucket=name+'hehe',
        ACL='public-read-write',
    )


def get_bucket_acl(name, client):
    ans = client.get_bucket_acl(
        Bucket=name+'hehe',
    )
    print 'Get bucket ACL:', ans


# boto cannot even send presigned acl requests correctly,
# so comment out

# def put_bucket_acl_presigned(name, client):
#     url = client.generate_presigned_url(
#         ClientMethod='put_bucket_acl',
#         Params={
#             'Bucket': name+'presigned',
#             'ACL': 'public-read-write'
#         },
#         HttpMethod="PUT",
#     )
#     print url
#     response = requests.put(url)
#     print 'Presigned put bucket ACL:', response.headers
#     assert response.status_code == 200


# def get_bucket_acl_presigned(name, client):
#     url = client.generate_presigned_url(
#         ClientMethod='get_bucket_acl',
#         Params={
#             'Bucket': name+'hehe',
#         },
#         HttpMethod='GET',
#     )
#     print url
#     response = requests.get(url)
#     print 'Presigned get bucket ACL:', response.text
#     assert response.status_code == 200



# =====================================================

TESTS = [
    sanity.create_bucket,
    sanity.head_bucket, sanity.head_bucket_nonexist,
    create_bucket_presigned,
    list_buckets_presigned,
    list_buckets_presigned_expired,
    put_bucket_acl, get_bucket_acl,
]

if __name__ == '__main__':
    base.run(TESTS)