import base
import sanity

# =====================================================


def sse_s3_multipart(name, client):
    ans = client.create_multipart_upload(
        Bucket=name+'hehe',
        Key=name+'s3',
        ServerSideEncryption='AES256'
    )
    print 'Create SSE-S3 multipart upload:', ans
    upload_id = ans['UploadId']

    ans = client.upload_part(
        Body=sanity.RANGE_1,
        Bucket=name+'hehe',
        Key=name+'s3',
        PartNumber=1,
        UploadId=upload_id,
    )
    print 'Upload SSE-S3 part 1:', ans
    etag_1 = ans['ETag']

    ans = client.upload_part(
        Body=sanity.RANGE_2,
        Bucket=name+'hehe',
        Key=name+'s3',
        PartNumber=2,
        UploadId=upload_id,
    )
    print 'Upload SSE-S3 part 2:', ans
    etag_2 = ans['ETag']

    ans = client.complete_multipart_upload(
        Bucket=name+'hehe',
        Key=name+'s3',
        MultipartUpload={
            'Parts': [
                {'ETag': etag_1, 'PartNumber': 1},
                {'ETag': etag_2, 'PartNumber': 2},
            ]
        },
        UploadId=upload_id
    )
    print 'Complete SSE-S3 multipart upload:', ans

    ans = client.get_object(
        Bucket=name+'hehe',
        Key=name+'s3'
    )
    body = ans['Body'].read()
    assert body == sanity.RANGE_1 + sanity.RANGE_2
    print 'Get SSE-S3 multipart upload object:', ans


def sse_custom_multipart(name, client):
    ans = client.create_multipart_upload(
        Bucket=name+'hehe',
        Key=name+'custom',
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='0123456789abcdef' * 2
    )
    print 'Create SSE-S3 multipart upload:', ans
    upload_id = ans['UploadId']

    ans = client.upload_part(
        Body=sanity.RANGE_1,
        Bucket=name+'hehe',
        Key=name+'custom',
        PartNumber=1,
        UploadId=upload_id,
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='0123456789abcdef' * 2
    )
    print 'Upload SSE-C part 1:', ans
    etag_1 = ans['ETag']

    ans = client.upload_part_copy(
        Bucket=name+'hehe',
        Key=name+'custom',
        PartNumber=2,
        UploadId=upload_id,
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='0123456789abcdef' * 2,
        CopySource={
            'Bucket': name+'hehe',
            'Key': name+'s3'
        },
        CopySourceRange='bytes=1048576-2097151'
    )
    print 'Copy SSE-C part 2:', ans
    etag_2 = ans['CopyPartResult']['ETag']

    ans = client.complete_multipart_upload(
        Bucket=name+'hehe',
        Key=name+'custom',
        MultipartUpload={
            'Parts': [
                {'ETag': etag_1, 'PartNumber': 1},
                {'ETag': etag_2, 'PartNumber': 2},
            ]
        },
        UploadId=upload_id
    )
    print 'Complete SSE-C multipart upload:', ans

    ans = client.get_object(
        Bucket=name+'hehe',
        Key=name+'custom',
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='0123456789abcdef' * 2,
    )
    body = ans['Body'].read()
    assert body == sanity.RANGE_1 + sanity.RANGE_2
    print 'Get SSE-C multipart upload object:', ans


def delete_multipart_uploaded_objects(name, client):
    ans = client.delete_objects(
        Bucket=name+'hehe',
        Delete={
            'Objects': [
                {
                    'Key': name+'s3'
                },
                {
                    'Key': name+'custom'
                }
            ]
        }
    )
    print 'Delete multiple objects:', ans

# =====================================================

TESTS = [
    sanity.create_bucket,
    sse_s3_multipart,
    sse_custom_multipart,
    delete_multipart_uploaded_objects,
    sanity.delete_bucket,
]

if __name__ == '__main__':
    base.run(TESTS)
