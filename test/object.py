import base
import sanity
import requests
import config

# =====================================================


def get_object_presigned(name, client):
    url = client.generate_presigned_url(
        ClientMethod='get_object',
        HttpMethod='GET',
        Params={
            'Bucket': name+'hehe',
            'Key': name+'hehe',
        }
    )
    print url
    response = requests.get(url)
    print 'Get object presigned:', response.status_code
    assert response.status_code == 200
    assert response.text == sanity.SMALL_TEST_FILE


def put_object_acl(name, client):
    client.put_object_acl(
        Bucket=name+'hehe',
        Key=name+'hehe',
        ACL='public-read',
    )


def get_object_acl(name, client):
    ans = client.get_object_acl(
        Bucket=name+'hehe',
        Key=name+'hehe',
    )
    print 'Get object ACL:', ans


def get_public_object(name, client):
    url = config.CONFIG['endpoint'] + '/' + name+'hehe' + '/' + name+'hehe'
    print url
    response = requests.get(url)
    print 'Get public object:', response.status_code
    assert response.status_code == 200
    assert response.text == sanity.SMALL_TEST_FILE


# =====================================================

TESTS = [
    sanity.create_bucket,
    sanity.put_object,
    get_object_presigned,
    put_object_acl, get_object_acl,
    get_public_object,
]

if __name__ == '__main__':
    base.run(TESTS)
