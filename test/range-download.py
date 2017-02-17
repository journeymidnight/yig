import sanity
import base

RANGE = bytes('abcdefghijklmnop' * 64 * 1024 * 4)  # 4M

def s3_multipart(name, client):

	ans = client.create_multipart_upload(Bucket=name+'hehe', Key=name+'multipartrange')
	print('Create MulitpartUpload 1 + 1 + 2, upload total 4M data')
	upload_id = ans['UploadId']

	#prepare DATA
	
	#Data Slice |--1M--|--1M--|----2M----|
	prepared_data = {}
	prepared_data[1] = {"offset":0,       "len":(1<<20), "etag":""}
	prepared_data[2] = {"offset":(1<<20), "len":(1<<20), "etag":""}
	prepared_data[3] = {"offset":(2<<20), "len":(2<<20), "etag":""}

        for part_number, value  in prepared_data.iteritems():
            this_range = RANGE[value["offset"]:value["offset"] + value["len"]]
            ans = client.upload_part(
                    Body=this_range,
                    Bucket=name+"hehe",
                    Key=name+"multipartrange",
                    PartNumber=part_number,
                    UploadId=upload_id)
            print 'Upload part %d: len: %d, %s' % (part_number, len(this_range), ans["ETag"])
            prepared_data[part_number]["etag"] = ans["ETag"]


        ans = client.complete_multipart_upload(
            Bucket=name+"hehe",
            Key=name+"multipartrange",
            MultipartUpload={
                "Parts":[
                    {"ETag":v["etag"], "PartNumber": k} for k, v in prepared_data.iteritems()
                 ]
                },
            UploadId=upload_id
            )
        print 'Complete multipart upload:', ans["ETag"]


def format_range(r):
    return "bytes=%d-%d" % (r[0],r[1])
	
def range_download_delete(name, client):
    #both is offset
    ranges = [(500, 999),
	      (1048575, 1048577),#Range: 1M-1, 1M+1
	      (1048576, 1048577),#Range: 1M, 1M+1
	      (1048575, 1048576),#Range: 1M-1, 1M
	      (0, 4194304),
	      (524288,  2097156),
              (2097152, 4194300)
	]
    for r in ranges:
	ans = client.get_object(
		Bucket=name+'hehe',
		Key=name+'multipartrange',
		Range=format_range(r)
	)
	#compare that
	body = ans['Body'].read()
	assert body == RANGE[r[0]:(r[1] + 1)]

    client.delete_object(
        Bucket=name+'hehe',
        Key=name+'multipartrange',
    )


   


TESTS = [
	sanity.create_bucket,
	s3_multipart,
	range_download_delete,
	sanity.delete_bucket,
	]


if __name__ == '__main__':
    base.run(TESTS)
