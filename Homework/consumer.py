"""
Your module description here
"""
import argparse
import boto3

ap = argparse.ArgumentParser()
ap.add_argument("-rb", "--request-bucket", help="source bucket of widget requests")
ap.add_argument("-wb", "--widget-bucket", help="destination bucket to send widgets")
ap.add_argument("-wt", "--widget-table", help="destination database table to send widgets")

args = ap.parse_args()
rb = args.request_bucket
wb = args.widget_bucket
wt = args.widget_table

s3_resource = boto3.resource('s3')

def getBucketList(s3_resource):
    bucket_list = []
    for bucket in s3_resource.buckets.all():
        bucket_list.append(bucket.name)
    
    return bucket_list

print(rb)
bucket_list = getBucketList(s3_resource)
if rb in bucket_list:
    print(True)
else:
    print(False)



    