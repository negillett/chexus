import json
import logging
import time

import boto3
from more_executors import Executors

from ..models import BucketItem, TableItem

LOG = logging.getLogger("chexus")


class Client(object):
    """A client for interacting with AWS S3 and DynamoDB using the
    boto3 library.

    This class provides methods for uploading files to an S3 bucket
    and putting items into a DynamoDB table.
    """

    def __init__(
        self,
        access_id=None,
        access_key=None,
        session_token=None,
        default_region=None,
        workers_count=4,
        retry_count=3,
    ):
        self._access_key_id = access_id
        self._access_key = access_key
        self._session_token = session_token
        self._default_region = default_region

        self._session = boto3.Session(
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._access_key,
            aws_session_token=self._session_token,
            region_name=self._default_region,
        )

        self._executor = Executors.thread_pool(
            max_workers=workers_count
        ).with_retry(max_attempts=retry_count)

    @staticmethod
    def _should_upload_file(checksum, bucket):
        if list(bucket.objects.filter(Prefix=checksum)):
            LOG.info("Content already present in s3 bucket")
            return False
        return True

    def _do_upload_file(self, item, bucket):
        if not self._should_upload_file(item.checksum, bucket):
            return

        LOG.info("Uploading %s...", item.name)

        bucket.upload_files(item.path, item.checksum)

    def upload_files(self, items, bucket_name, dryrun=False):
        """Efficiently puts items into the specified S3 bucket without
        risk of overwriting or duplicating data.

        Args:
            items (:class:`~pubtools.aws.BucketItem`, list)
                One or more representations of an item to upload to the
                bucket.

            bucket_name (str)
                The name of the bucket to which the item will be
                uploaded.

            dryrun (bool)
                If true, only log what would be uploaded.
        """

        # Coerce items to list
        if not isinstance(items, (list, tuple)):
            items = [items]
        if isinstance(items, tuple):
            items = list(items)

        bucket = self._session.resource("s3").Bucket(bucket_name)

        upload_fts = []
        for item in items:
            if not isinstance(item, BucketItem):
                LOG.error(
                    "Expected type 'BucketItem', got '%s' instead", type(item),
                )
                continue

            if dryrun:
                LOG.info(
                    "Would upload %s to the '%s' bucket",
                    item.name,
                    bucket_name,
                )
                continue

            upload_fts.append(
                self._executor.submit(self._do_upload_file, item, bucket)
            )

        # Block until all futures have completed
        # Not using concurrent.futures.as_completed() due to Python 2.7
        while not all([ft.done() for ft in upload_fts]):
            time.sleep(1)

        # Report any upload failures as errors -- raising them could
        # prevent other files from uploading
        upload_errs = [ft.exception() for ft in upload_fts]
        # Filter possible NoneTypes
        if [err for err in upload_errs if err]:
            LOG.error(
                "One or more exceptions occurred during upload\n\t%s",
                "\n\t".join(str(err) for err in upload_errs),
            )

        LOG.info("Upload complete")

    @staticmethod
    def _query_table_item(item, table):
        expr_vals = {}
        key_exprs = []
        for att in table.attribute_definitions:
            att_name = str(att["AttributeName"])
            expr_vals.update(
                {":%sval" % att_name: "%s" % getattr(item, att_name)}
            )
            key_exprs.append("%s = :%sval" % (att_name, att_name))

        return table.query(
            KeyConditionExpression=" and ".join(key_exprs),
            ExpressionAttributeValues=expr_vals,
        )

    def _should_put_item(self, item, table):
        response = self._query_table_item(item, table)

        if response["Items"]:
            LOG.info("Item already exists in table")
            return False

        return True

    def _do_put_item(self, item, table):
        for att in table.attribute_definitions:
            att_name = str(att["AttributeName"])
            if not hasattr(item, att_name):
                raise ValueError(
                    "Item to put is missing required key, '%s'" % att_name
                )

        if not self._should_put_item(item, table):
            return

        LOG.info(
            "Putting the following item into the '%s' table;\n\t%s",
            table.name,
            json.dumps(item.attrs, sort_keys=True, indent=4),
        )

        table.put_item(Item=item.attrs)

    def put_items(self, items, table_name, region=None, dryrun=False):
        """Efficiently puts items into the specified DynamoDB table
        without risk of overwriting or duplicating data.

        Args:
            items (:class:`~chexus.aws.TableItem`, list)
                One or more representations of an item to put into the
                table.

            table_name (str)
                The name of the table in which the item will be
                put.

            region (str)
                The name of the AWS region the desired table belongs
                to. If not provided here or to the calling client,
                attempts to find it among environment variables and
                configuration files will be made.

            dryrun (bool)
                If true, only log what would be put.
        """

        # Coerce items to list
        if not isinstance(items, (list, tuple)):
            items = [items]
        if isinstance(items, tuple):
            items = list(items)

        table = self._session.resource("dynamodb", region_name=region).Table(
            table_name
        )

        put_fts = []
        for item in items:
            if not isinstance(item, TableItem):
                LOG.error(
                    "Expected type 'TableItem', got '%s' instead", type(item),
                )
                continue

            if dryrun:
                LOG.info(
                    "Would put the following item to the '%s' table;\n\t%s",
                    table.name,
                    json.dumps(item.attrs, sort_keys=True, indent=4),
                )
                continue

            put_fts.append(
                self._executor.submit(self._do_put_item, item, table)
            )

        # Block until all futures have completed
        # Not using concurrent.futures.as_completed() due to Python 2.7
        while not all([ft.done() for ft in put_fts]):
            time.sleep(1)

        # Report any put failures as errors -- raising them could
        # prevent other item from being put
        put_errs = [ft.exception() for ft in put_fts]
        # Filter possible NoneTypes
        if [err for err in put_errs if err]:
            LOG.error(
                "One or more exceptions occurred during put\n\t%s",
                "\n\t".join(str(err) for err in put_errs),
            )

        LOG.info("Put complete")
