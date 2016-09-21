import base
import sanity


def compare_files(files_1, files_2):
    print 'FILES', files_1.keys()
    print 'FILES', files_2.keys()
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
    current_versions[(name+'_versioning', ans.get('VersionId') or "")] = True

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
    if list_v1.get('Contents') is not None:
        for f in list_v1.get('Contents'):
            files_v1[f.get('Key')] = True
    assert compare_files(current_files, files_v1)

    list_v2 = client.list_objects_v2(
        Bucket=name+'hehe',
    )
    print 'List v2:', list_v2
    files_v2 = {}
    if list_v2.get('Contents') is not None:
        for f in list_v2.get('Contents'):
            files_v2[f.get('Key')] = True
    assert compare_files(current_files, files_v2)

    list_versions = client.list_object_versions(
        Bucket=name+'hehe'
    )
    print 'List versions:', list_versions
    files_versions = {}
    if list_versions.get('Versions') is not None:
        for f in list_versions.get('Versions'):
            files_versions[(f.get('Key'), f.get('VersionId') or "")] = True
    assert compare_files(current_versions, files_versions)


def simple_delete(name, client, current_files, current_versions):
    files = current_files.keys()
    print 'Simple delete:', files
    for f in files:
        ans = client.delete_object(
            Bucket=name+'hehe',
            Key=f
        )
        del current_files[f]
        if ans['DeleteMarker']:
            current_versions[(f, ans.get('VersionId'))] = True


def versioned_delete(name, client, current_files, current_versions):
    versions = current_versions.keys()
    print 'Versioned delete:', versions
    for version in versions:
        f, v = version
        ans = client.delete_object(
            Bucket=name+'hehe',
            Key=f,
            VersionId=v if v else 'null'
        )
        returned_version = "" if ans.get('VersionId') == "null" else ans.get('VersionId')
        del current_versions[(f, returned_version)]


# =====================================================

# bucket_name -> file_name -> dummy bool
CURRENT_FILES = {}
# bucket_name -> (file_name, version) -> dummy bool
CURRENT_VERSIONS = {}


def upload_objects_versioning_disabled(name, client):
    current_files = {}
    current_versions = {}
    if CURRENT_FILES.get(name + 'hehe') is None:
        CURRENT_FILES[name+'hehe'] = {}
        CURRENT_VERSIONS[name+'hehe'] = {}
    else:
        current_files = CURRENT_FILES[name+'hehe']
        current_versions = CURRENT_VERSIONS[name+'hehe']

    for i in range(3):
        upload_test_unit(name, client, current_files, current_versions)

    list_test_unit(name, client, current_files, current_versions)

    CURRENT_FILES[name+'hehe'] = current_files
    CURRENT_VERSIONS[name+'hehe'] = current_versions


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
    current_versions = {}
    if CURRENT_FILES.get(name + 'hehe') is None:
        CURRENT_FILES[name+'hehe'] = {}
        CURRENT_VERSIONS[name+'hehe'] = {}
    else:
        current_files = CURRENT_FILES[name+'hehe']
        current_versions = CURRENT_VERSIONS[name+'hehe']

    for i in range(3):
        upload_test_unit(name, client, current_files, current_versions)

    list_test_unit(name, client, current_files, current_versions)

    CURRENT_FILES[name+'hehe'] = current_files
    CURRENT_VERSIONS[name+'hehe'] = current_versions


def delete_object_versioning_enabled(name, client):
    current_files = CURRENT_FILES[name+'hehe']
    current_versions = CURRENT_VERSIONS[name+'hehe']

    simple_delete(name, client, current_files, current_versions)
    try:
        ans = client.get_object(
            Bucket=name+'hehe',
            Key=name+'_versioning'
        )
    except Exception as e:
        assert 'NoSuchKey' in str(e)
    list_test_unit(name, client, current_files, current_versions)

    versioned_delete(name, client, current_files, current_versions)
    list_test_unit(name, client, current_files, current_versions)

# =====================================================

TESTS = [
    sanity.create_bucket,
    upload_objects_versioning_disabled,
    upload_objects_versioning_enabled,
    delete_object_versioning_enabled,
    sanity.delete_bucket,
]

if __name__ == '__main__':
    base.run(TESTS)
