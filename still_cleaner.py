import boto3

BUCKET_NAME = "nestlapse"

s3 = boto3.resource("s3")


def bucket_factory() -> boto3.resource:
    return s3.Bucket(BUCKET_NAME)


def bulk_scan_for_empties(directory: str):
    bucket = bucket_factory()
    results = list(bucket.objects.filter(Delimiter="/", Prefix=directory))
    print(f"Got back {len(results)} items")
    return (obj for obj in results if obj.size == 0)


def scan_for_empties(directory: str, prefix: str):
    bucket = bucket_factory()
    results = bucket.objects.filter(Delimiter="/", Prefix=f"{directory}/{prefix}")
    return (obj for obj in results if obj.size == 0)


for empty in bulk_scan_for_empties("continuous/backpatio/"):
    print(empty)
