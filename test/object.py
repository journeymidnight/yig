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
        Key=name+'encrypted_custom',
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='0123456789abcdef' * 2
    )
    body = ans['Body'].read()
    assert body == sanity.SMALL_TEST_FILE
    print 'SSE-C:', ans


def object_encryption_wrong_customer_key_should_fail(name, client):
    client.put_object(
        Body=sanity.SMALL_TEST_FILE,
        Bucket=name+'hehe',
        Key=name+'encrypted_wrong_key',
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='0123456789abcdef' * 2
    )
    ans = client.get_object(
        Bucket=name+'hehe',
        Key=name+'encrypted_wrong_key',
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='abcdef0123456789' * 2
    )
    body = ans['Body'].read()
    assert body == sanity.SMALL_TEST_FILE


def sse_copy_plain_to_s3(name, client):
    client.put_object(
        Body=sanity.SMALL_TEST_FILE,
        Bucket=name+'hehe',
        Key=name+'plain'
    )
    client.copy_object(
        Bucket=name+'hehe',
        Key=name+'to_s3',
        CopySource={
            'Bucket': name+'hehe',
            'Key': name+'plain'
        },
        ServerSideEncryption='AES256'
    )
    ans = client.get_object(
        Bucket=name+'hehe',
        Key=name+'to_s3',
    )
    body = ans['Body'].read()
    assert body == sanity.SMALL_TEST_FILE
    print 'SSE copy: plain to s3:', ans


def sse_copy_plain_to_custom(name, client):
    client.put_object(
        Body=sanity.SMALL_TEST_FILE,
        Bucket=name+'hehe',
        Key=name+'plain'
    )
    client.copy_object(
        Bucket=name+'hehe',
        Key=name+'to_custom',
        CopySource={
            'Bucket': name+'hehe',
            'Key': name+'plain'
        },
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='0123456789abcdef' * 2
    )
    ans = client.get_object(
        Bucket=name+'hehe',
        Key=name+'to_custom',
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='0123456789abcdef' * 2
    )
    body = ans['Body'].read()
    assert body == sanity.SMALL_TEST_FILE
    print 'SSE copy: plain to s3:', ans


def sse_copy_s3_to_custom(name, client):
    client.put_object(
        Body=sanity.SMALL_TEST_FILE,
        Bucket=name+'hehe',
        Key=name+'_s3',
        ServerSideEncryption='AES256',
    )
    client.copy_object(
        Bucket=name+'hehe',
        Key=name+'to_custom',
        CopySource={
            'Bucket': name+'hehe',
            'Key': name+'_s3'
        },
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='0123456789abcdef' * 2
    )
    ans = client.get_object(
        Bucket=name+'hehe',
        Key=name+'to_custom',
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='0123456789abcdef' * 2
    )
    body = ans['Body'].read()
    assert body == sanity.SMALL_TEST_FILE
    print 'SSE copy: plain to s3:', ans


def sse_copy_custom_to_custom(name, client):
    client.put_object(
        Body=sanity.SMALL_TEST_FILE,
        Bucket=name+'hehe',
        Key=name+'_custom',
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='0123456789abcdef' * 2
    )
    client.copy_object(
        Bucket=name+'hehe',
        Key=name+'to_custom',
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='abcdef0123456789' * 2,
        CopySource={
            'Bucket': name+'hehe',
            'Key': name+'_custom'
        },
        CopySourceSSECustomerAlgorithm='AES256',
        CopySourceSSECustomerKey='0123456789abcdef' * 2
    )
    ans = client.get_object(
        Bucket=name+'hehe',
        Key=name+'to_custom',
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='abcdef0123456789' * 2
    )
    body = ans['Body'].read()
    assert body == sanity.SMALL_TEST_FILE
    print 'SSE copy: plain to s3:', ans

# =====================================================

TESTS = [
    sanity.create_bucket,
    sanity.put_object,
    get_object_presigned,
    put_object_acl, get_object_acl,
    get_public_object,
    object_encryption_s3,
    object_encryption_customer_key,
    object_encryption_wrong_customer_key_should_fail,
    sse_copy_plain_to_s3,
    sse_copy_plain_to_custom,
    sse_copy_s3_to_custom,
    sse_copy_custom_to_custom,
]

if __name__ == '__main__':
    base.run(TESTS)
