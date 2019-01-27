import boto3

BUCKET_NAME = "nestlapse"

s3 = boto3.resource("s3")


bucket = s3.Bucket(BUCKET_NAME)
# for camera in ["backpatio", "backdoor"]:
results = bucket.objects.filter(Delimiter="/", Prefix="backpatio/", MaxKeys=10)


for res in results:
    print(res)
