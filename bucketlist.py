import boto3

s3_resource = boto3.resource('s3')
print("List of buckets:")
for bucket in s3_resource.buckets.all():
    print(f"\t{bucket.name}")