import base
import sanity


def compare_files(files_1, files_2):
    print 'FILES', files_1
    print 'FILES', files_2
    list_1 = sorted(files_1.keys())
    list_2 = sorted(files_2.keys())
    if len(list_1) != len(list_2):
        return False
    for i in range(len(list_1)):
        if list_1[i] != list_2[i]:
            return False
    return True


def upload_test_unit(name, client, current_files, current_versions):
    ans = client.put_object(
        Body=sanity.SMALL_TEST_FILE,
        Bucket=name+'hehe',
        Key=name+'_versioning'
    )
    print 'Put object:', name + '_versioning', ans
    current_files[name+'_versioning'] = True
    current_versions[(name+'_versioning', ans.get('Key') or "")] = True

    ans = client.create_multipart_upload(
        Bucket=name+'hehe',
        Key=name+'_versioning',
    )
    upload_id = ans['UploadId']
    ans = client.upload_part(
        Body=sanity.RANGE_1,
        Bucket=name+'hehe',
        Key=name+'_versioning',
        PartNumber=1,
        UploadId=upload_id,
    )
    etag = ans['ETag']
    ans = client.complete_multipart_upload(
        Bucket=name+'hehe',
        Key=name+'_versioning',
        MultipartUpload={
            'Parts': [
                {'ETag': etag, 'PartNumber': 1},
            ]
        },
        UploadId=upload_id,
    ),
    print 'Put object multipart:', name + '_versioning', ans
    current_files[name+'_versioning'] = True
    current_versions[(name+'_versioning', ans[0].get('VersionId') or "")] = True


def list_test_unit(name, client, current_files, current_versions):
    list_v1 = client.list_objects(
        Bucket=name+'hehe'
    )
    print 'List v1:', list_v1
    files_v1 = {}
    for f in list_v1.get('Contents'):
        files_v1[(f.get('Key'), f.get('VersionId') or "")] = True
    assert compare_files(current_files, files_v1)

    list_v2 = client.list_objects_v2(
        Bucket=name+'hehe',
    )
    print 'List v2:', list_v2
    files_v2 = {}
    for f in list_v2.get('Contents'):
        files_v2[(f.get('Key'), f.get('VersionId') or "")] = True
    assert compare_files(current_files, files_v2)

    list_versions = client.list_object_versions(
        Bucket=name+'hehe'
    )
    print 'List versions:', list_versions
    files_versions = {}
    for f in list_versions.get('Versions'):
        files_versions[(f.get('Key'), f.get('VersionId') or "")] = True
    assert compare_files(current_versions, files_versions)

# =====================================================

# bucket_name -> file_name -> dummy bool
CURRENT_FILES = {}
# bucket_name -> (file_name, version) -> dummy bool
CURRENT_VERSIONS = {}


def upload_objects_versioning_disabled(name, client):
    current_files = {}
    if CURRENT_FILES.get(name + 'hehe') is None:
        CURRENT_FILES[name+'hehe'] = {}
    else:
        current_files = CURRENT_FILES[name+'hehe']

    for i in range(3):
        upload_test_unit(name, client, current_files)

    list_test_unit(name, client, current_files)

    CURRENT_FILES[name+'hehe'] = current_files


def upload_objects_versioning_enabled(name, client):
    client.put_bucket_versioning(
        Bucket=name+'hehe',
        VersioningConfiguration={
            'Status': 'Enabled'
        }
    )
    ans = client.get_bucket_versioning(
        Bucket=name+'hehe'
    )
    assert ans['Status'] == 'Enabled'

    current_files = {}
    if CURRENT_FILES.get(name + 'hehe') is None:
        CURRENT_FILES[name+'hehe'] = {}
    else:
        current_files = CURRENT_FILES[name+'hehe']

    for i in range(3):
        upload_test_unit(name, client, current_files)

    list_test_unit(name, client, current_files)

    CURRENT_FILES[name+'hehe'] = current_files


# =====================================================

TESTS = [
    sanity.create_bucket,
    upload_objects_versioning_disabled,
    upload_objects_versioning_enabled,
    sanity.delete_bucket,
]

if __name__ == '__main__':
    base.run(TESTS)
