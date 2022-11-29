import boto3

s3 = boto3.client('s3')


def create_s3_presigned_url(bucket_name, key_name, expires):
    url = s3.generate_presigned_url(ClientMethod='get_object', Params={'Bucket': bucket_name, 'Key': key_name}, ExpiresIn=expires)
    return url


print(create_s3_presigned_url('share-raw-dataset', 'rawdata_2022.10.zip', 60*60*24*7))
