#!/usr/bin/env python3

import boto3
import logging


def main() -> None:
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(module)s: %(message)s",
        level=logging.INFO,
    )

    bucket_name = "nestlapse"
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)

    cameras = ["backdoor", "backpatio", "continuous/backdoor", "continuous/backpatio"]

    batch_size = 1000

    for camera in cameras:
        logging.info(f"{camera}: Fetching raw files")
        to_del = [
            o.key for o in bucket.objects.filter(Prefix=f"{camera}/1") if o.size == 0
        ]

        if not to_del:
            logging.info(f"{camera}: No empty raw files to delete! Skip for now")
            continue

        logging.info(f"{camera}: {len(to_del)} can go!")

        batch = to_del[: min(len(to_del), batch_size)]
        delete_req = {"Objects": [{"Key": k} for k in batch]}
        logging.info(f"{camera}: Going to delete {len(batch)}")
        res = bucket.delete_objects(Delete=delete_req)
        logging.info(
            f"{camera}: Successfully deleted {len(res['Deleted'])}/{len(batch)}"
        )


if __name__ == "__main__":
    main()
