#!/usr/bin/env python3

import argparse
import logging

from chexus import Client, BucketItem

LOG = logging.getLogger("download-file")


def main():
    LOG.setLevel(logging.INFO)
    logging.basicConfig(format="%(message)s", level=logging.INFO)

    parser = argparse.ArgumentParser(
        description="Download a file from an S3 bucket."
    )
    parser.add_argument(
        "--object-key",
        required=True,
        help="The file object's key in the S3 bucket",
    )
    parser.add_argument(
        "--file-path",
        required=True,
        help="Local filesystem path at which to save the file.",
    )
    parser.add_argument(
        "--bucket", required=True, help="S3 bucket in which to upload file."
    )
    parser.add_argument(
        "--aws-access-id",
        default=None,
        help="Access ID for Amazon services. If no ID is provided, attempts to"
        " find it among environment variables and ~/.aws/config file will"
        " be made",
    )
    parser.add_argument(
        "--aws-access-key",
        default=None,
        help="Access key for Amazon services. If no key is provided, attempts"
        " to find it among environment variables and ~/.aws/config file"
        " will be made",
    )
    parser.add_argument(
        "--aws-session-token",
        default=None,
        help="Session token for Amazon services. If no token is provided,"
        " attempts to find it among environment variables and"
        " ~/.aws/config file will be made",
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        help="Don't execute the action, only log what would otherwise be done.",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Include debug logging."
    )

    p = parser.parse_args()

    if p.debug:
        logging.getLogger("chexus").setLevel(logging.DEBUG)
        LOG.setLevel(logging.DEBUG)

    client = Client(
        access_id=p.aws_access_id,
        access_key=p.aws_access_key,
        session_token=p.aws_session_token,
    )

    download_item = BucketItem(file_path=p.file_path, key=p.object_key)

    if download_item.checksum:
        resp = input(
            "File already exists. Do you want to overwrite it? [y/N]:  "
        )
        if str(resp).lower() not in ("yes", "y"):
            LOG.info("Aborting...")
            return

    client.download(items=download_item, bucket_name=p.bucket, dryrun=p.dryrun)

    downloaded_item = BucketItem(file_path=p.file_path, key=p.object_key)
    assert downloaded_item.checksum


if __name__ == "__main__":
    main()
