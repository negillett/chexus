import json
import logging
import time

import boto3
from more_executors import Executors

from ..models import BucketItem, TableItem

LOG = logging.getLogger("chexus")


class Client(object):
    """A client for interacting with Amazon S3 and DynamoDB.

    This class provides methods for uploading content to an S3 bucket
    and publishing data about these uploads to a DynamoDB table.
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
    def _should_upload(checksum, bucket):
        LOG.info("Checking upload item...")

        if list(bucket.objects.filter(Prefix=checksum)):
            LOG.info("Content already present in s3 bucket")
            return False
        return True

    def _do_upload(self, item, bucket):
        if not self._should_upload(item.checksum, bucket):
            return

        LOG.info("Uploading %s...", item.name)

        bucket.upload_file(item.path, item.checksum)

    def upload(self, items, bucket_name, dryrun=False):
        """Uploads an item to the specified S3 bucket.

        Args:
            items (:class:`~chexus.BucketItem`, list)
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
                self._executor.submit(self._do_upload, item, bucket)
            )

        # Block until all futures have completed
        # Not using concurrent.futures.as_completed() due to Python 2.7
        while not all([ft.done() for ft in upload_fts]):
            time.sleep(1)

        # Report any upload failures as errors -- raising them could
        # prevent other files from uploading
        upload_errs = [ft.exception() for ft in upload_fts]
        # List of errors can contain None types, which aren't helpful
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

    def _should_publish(self, item, table):
        LOG.info("Checking publish...")

        response = self._query_table_item(item, table)

        if response["Items"]:
            LOG.info("Item already exists in table")
            return False

        return True

    def _do_publish(self, item, table):
        for att in table.attribute_definitions:
            att_name = str(att["AttributeName"])
            if not hasattr(item, att_name):
                raise ValueError(
                    "Item to publish is missing required key, '%s'" % att_name
                )

        if not self._should_publish(item, table):
            return

        LOG.info(
            "Putting the following item into the '%s' table;\n\t%s",
            table.name,
            json.dumps(item.attrs, sort_keys=True, indent=4),
        )

        table.put_item(Item=item.attrs)

    def publish(self, items, table_name, region=None, dryrun=False):
        """Efficiently puts items into the specified DynamoDB table
        without risk of overwriting or duplicating data.

        Args:
            items (:class:`~chexus.TableItem`, list)
                One or more representations of an item to publish to the
                table.

            table_name (str)
                The name of the table in which the item will be
                published.

            region (str)
                The name of the AWS region the desired table belongs
                to. If not provided here or to the calling client,
                attempts to find it among environment variables and
                configuration files will be made.

            dryrun (bool)
                If true, only log what would be published.
        """

        # Coerce items to list
        if not isinstance(items, (list, tuple)):
            items = [items]
        if isinstance(items, tuple):
            items = list(items)

        table = self._session.resource("dynamodb", region_name=region).Table(
            table_name
        )

        publish_fts = []
        for item in items:
            if not isinstance(item, TableItem):
                LOG.error(
                    "Expected type 'TableItem', got '%s' instead", type(item),
                )
                continue

            if dryrun:
                LOG.info(
                    "Would publish the following item to the '%s' table;\n\t%s",
                    table.name,
                    json.dumps(item.attrs, sort_keys=True, indent=4),
                )
                continue

            publish_fts.append(
                self._executor.submit(self._do_publish, item, table)
            )

        # Block until all futures have completed
        # Not using concurrent.futures.as_completed() due to Python 2.7
        while not all([ft.done() for ft in publish_fts]):
            time.sleep(1)

        # Report any publish failures as errors -- raising them could
        # prevent other items from being published
        publish_errs = [ft.exception() for ft in publish_fts]
        # Filter possible NoneTypes
        if [err for err in publish_errs if err]:
            LOG.error(
                "One or more exceptions occurred during publish\n\t%s",
                "\n\t".join(str(err) for err in publish_errs),
            )

        LOG.info("Publish complete")
