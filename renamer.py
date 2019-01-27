import boto3
from datetime import datetime
from pytz import timezone
import logging
from typing import Dict, Tuple, Any


def get_ts(obj_key: str) -> int:
    file_name = obj_key.split("/")[-1].split(".")[0]

    if "_" not in file_name:
        return int(file_name)

    return int(file_name.split("_")[-1])


def main() -> None:
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(module)s: %(message)s", level=logging.INFO
    )

    bucket_name = "nestlapse"
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)

    camera_timezone = timezone("America/Los_Angeles")
    cameras = ["backdoor", "backpatio", "continuous/backdoor", "continuous/backpatio"]

    raw_files = {}
    processed = {}
    for camera in cameras:
        logging.info(f"{camera}: Fetching raw files")
        raw_files[camera] = {
            get_ts(o.key): o for o in bucket.objects.filter(Prefix=f"{camera}/1")
            if o.size > 0
        }

        if not raw_files:
            logging.info(f"{camera}: No raw files!")
            continue

        logging.info(f"{camera}: Fetching processed files")
        processed[camera] = {
            get_ts(o.key): o for o in bucket.objects.filter(Prefix=f"{camera}/2")
        }

        missing = set(raw_files[camera].keys()) - set(processed[camera].keys())

        logging.info(f"{camera}: Missing {len(missing)}/{len(raw_files[camera])}")

        copy_results = {}
        for ts in missing:
            orig_obj = raw_files[camera][ts]
            if orig_obj.size == 0:
                if "continuous" in camera:
                    continue

                logging.error(f"{camera}: {orig_obj.key} is 0 bytes?!")
                continue

            copy_src = {"Bucket": bucket_name, "Key": orig_obj.key}
            ts_int = int(orig_obj.key.replace(f"{camera}/", "").replace(".jpg", ""))
            ts = datetime.fromtimestamp(ts_int, tz=camera_timezone)
            new_obj_key = f"{camera}/{ts.strftime('%Y-%m-%d_%H:%M:%S_%Z')}_{ts_int}.jpg"
            logging.info(f"{camera}: {orig_obj.key} -> {new_obj_key}")
            copy_results[ts] = s3.Object(bucket_name, new_obj_key).copy_from(
                CopySource=copy_src
            )
        failures = [
            (k, v)
            for k, v in copy_results.items()
            if v["ResponseMetadata"]["HTTPStatusCode"] != 200
        ]
        if failures:
            logging.info(f"{camera}: Catching up had {len(failures)} failures")
            for k, v in failures:
                logging.info(f"{k}: {v}")


if __name__ == "__main__":
    main()
