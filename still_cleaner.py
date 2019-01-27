#!/usr/bin/env python3

import boto3
import logging


def get_ts(obj_key: str) -> int:
    file_name = obj_key.split("/")[-1].split(".")[0]

    if "_" not in file_name:
        return int(file_name)

    return int(file_name.split("_")[-1])


def find_safe_to_delete(raw_files, processed):
    return [
        k
        for k, v in raw_files.items()
        if k in processed and raw_files[k].size == processed[k].size
    ]


def main() -> None:
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(module)s: %(message)s", level=logging.INFO
    )

    bucket_name = "nestlapse"
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)

    # If we had 24 hrs of daylight there'd be this many in the continuous folder per day
    # Since we don't live in extreme latitudes that's not the case and this is a safe
    # upper limit that'll let us get through any backlog, eventually.
    batch_size = (24 * 60) / 5

    cameras = ["backdoor", "backpatio", "continuous/backdoor", "continuous/backpatio"]

    for camera in cameras:
        logging.info(f"{camera}: Fetching raw files")
        raw_files = {
            get_ts(o.key): o for o in bucket.objects.filter(Prefix=f"{camera}/1")
        }

        logging.info(f"{camera}: Fetching processed files")
        processed = {
            get_ts(o.key): o for o in bucket.objects.filter(Prefix=f"{camera}/2")
        }

        to_del = sorted(find_safe_to_delete(raw_files, processed))
        if not to_del:
            logging.info(f"{camera}: Nothing safe to clean up!")
            continue

        logging.info(f"{camera}: {len(to_del)}/{len(raw_files)} can go")

        batch = to_del[:min(len(to_del), batch_size)]
        delete_req = {
            'Objects': [{'Key': f"{camera}/{k}.jpg"} for k in batch],
        }
        logging.info(f"{camera}: Going to delete {len(batch)}")
        res = bucket.delete_objects(Delete=delete_req)
        logging.info(f"{camera}: Successfully deleted {len(res['Deleted'])}/{len(batch)}")


if __name__ == "__main__":
    main()
