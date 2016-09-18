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


def object_encryption_s3(name, client):
    client.put_object(
        Body=sanity.SMALL_TEST_FILE,
        Bucket=name+'hehe',
        Key=name+'encrypted',
        ServerSideEncryption='AES256',
    )
    ans = client.get_object(
        Bucket=name+'hehe',
        Key=name+'encrypted',
    )
    body = ans['Body'].read()
    assert body == sanity.SMALL_TEST_FILE
    print 'SSE-S3:', ans


def object_encryption_customer_key(name, client):
    client.put_object(
        Body=sanity.SMALL_TEST_FILE,
        Bucket=name+'hehe',
        Key=name+'encrypted_custom',
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='0123456789abcdef' * 2
    )
    ans = client.get_object(
        Bucket=name+'hehe',
        Key=name+'encrypted',
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='0123456789abcdef' * 2
    )
    body = ans['Body'].read()
    assert body == sanity.SMALL_TEST_FILE
    print 'SSE-C:', ans


# =====================================================

TESTS = [
    sanity.create_bucket,
    sanity.put_object,
    get_object_presigned,
    put_object_acl, get_object_acl,
    get_public_object,
    object_encryption_s3,
    object_encryption_customer_key,
]

if __name__ == '__main__':
    base.run(TESTS)
