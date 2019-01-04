import base
import requests
import botocore
import botocore.session


def my_delete_buckets(name,client):
    buckets_dict = client.list_buckets()
    buckets_list = buckets_dict['Buckets']
    for bucket_info in buckets_list:
        my_delete_objects(bucket_info['Name'],client)
        client.delete_bucket(
            Bucket = bucket_info['Name']
    )


def my_delete_objects(bucket_name,client):
    objects_dict = client.list_objects(
        Bucket = bucket_name
    )
    print objects_dict['Name']
    objects_list = objects_dict['Contents']
    for object_info in objects_list:
        client.delete_object(
             Bucket = bucket_name,
             Key=object_info['Key']
        )

TESTS = [
      my_delete_buckets,
      ]

if __name__ == '__main__':
        base.run(TESTS)
