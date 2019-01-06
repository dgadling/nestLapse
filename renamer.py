import boto3
from datetime import datetime
from pytz import timezone
import logging

BUCKET_NAME = "nestlapse"

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s: %(message)s", level=logging.INFO
)

s3 = boto3.resource("s3")


def get_ts(obj_key: str) -> int:
    file_name = obj_key.split("/")[-1].split(".")[0]

    if "_" not in file_name:
        return int(file_name)

    return int(file_name.split("_")[-1])


camera_timezone = timezone("America/Los_Angeles")
bucket = s3.Bucket(BUCKET_NAME)

cameras = [
    "backdoor",
    "backpatio",
    "continuous/backdoor",
    "continuous/backpatio",
]
for camera in cameras:
    raw_files = {get_ts(o.key): o for o in bucket.objects.filter(Prefix=f"{camera}/1")}
    processed = {get_ts(o.key): o for o in bucket.objects.filter(Prefix=f"{camera}/2")}

    missing = set(raw_files.keys()) - set(processed.keys())

    logging.info(f"{camera}: Missing {len(missing)}/{len(raw_files)}")

    copy_results = {}
    for ts in missing:
        orig_obj = raw_files[ts]
        if orig_obj.size == 0:
            if "continuous" not in camera:
                logging.error(f"{camera}: {orig_obj.key} is 0 bytes! Need a strategy!")
            continue

        copy_src = {"Bucket": BUCKET_NAME, "Key": orig_obj.key}
        ts_int = int(orig_obj.key.replace(f"{camera}/", "").replace(".jpg", ""))
        ts = datetime.fromtimestamp(ts_int, tz=camera_timezone)
        new_obj_key = f"{camera}/{ts.strftime('%Y-%m-%d_%H:%M:%S_%Z')}_{ts_int}.jpg"
        logging.info(f"{camera}: {orig_obj.key} -> {new_obj_key}")
        copy_results[ts] = s3.Object(BUCKET_NAME, new_obj_key).copy_from(
            CopySource=copy_src
        )
    failures = [
        (k, v)
        for k, v in copy_results.items()
        if v["ResponseMetadata"]["HTTPStatusCode"] != 200
    ]
    if failures:
        logging.info(f"Catching up {camera} had {len(failures)} failures")
        for k, v in failures:
            logging.info(f"{k}: {v}")
