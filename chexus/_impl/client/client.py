import json
import logging
import time

import boto3
from more_executors import Executors

from ..models import BucketItem, TableItem

LOG = logging.getLogger("chexus")


class Client(object):
    """A client for interacting with Amazon S3 and DynamoDB.

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
    def _should_upload(checksum, bucket):
        if list(bucket.objects.filter(Prefix=checksum)):
            LOG.info("Item already in s3 bucket")
            return False
        return True

    def _do_upload(self, item, bucket):
        if not self._should_upload(item.checksum, bucket):
            return

        LOG.info("Uploading %s...", item.name)

        bucket.upload_file(item.path, item.checksum)

    def upload(self, items, bucket_name, dryrun=False):
        """Efficiently uploads files into the specified S3 bucket
        without risk of overwriting or duplicating data.

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

        LOG.info("Starting upload...")

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
        fil_exprs = []

        for key, value in item.attrs.items():
            expr_vals.update({":%sval" % key: "%s" % value})
            if key in [
                str(a["AttributeName"]) for a in table.attribute_definitions
            ]:
                key_exprs.append("%s = :%sval" % (key, key))
            else:
                fil_exprs.append("%s = :%sval" % (key, key))

        criteria = {
            "ExpressionAttributeValues": expr_vals,
            "KeyConditionExpression": " and ".join(key_exprs),
            "Select": "ALL_ATTRIBUTES",
        }

        if fil_exprs:
            criteria.update({"FilterExpression": " and ".join(fil_exprs)})

        response = table.query(**criteria)

        if "LastEvaluatedKey" in response:
            LOG.warning(
                "Query limit reached, results truncated\n"
                "Consider increasing uniqueness of key(s)"
            )

        return response

    def query(self, item, table_name, region=None):
        """Queries the specified table using a TableItem as criteria.

        Args:
            item (:class:`~chexus.TableItem`)
                A representation of a DynamoDB table item.

            table_name (str)
                The name of the table to query.

            region (str)
                The name of the AWS region the desired table belongs
                to. If not provided here or to the calling client,
                attempts to find it among environment variables and
                configuration files will be made.
        """

        if not isinstance(item, TableItem):
            raise ValueError(
                "Expected type 'TableItem', got '%s' instead" % type(item)
            )

        table = self._session.resource("dynamodb", region_name=region).Table(
            table_name
        )

        return self._query_table_item(item, table)

    def _should_publish(self, item, table):
        for att in [
            str(a["AttributeName"]) for a in table.attribute_definitions
        ]:
            if not hasattr(item, att) or not getattr(item, att):
                LOG.error("Item to publish is missing required key, '%s'", att)
                return False

        response = self._query_table_item(item, table)

        if response["Items"]:
            LOG.info("Item already exists in table")
            return False

        return True

    def _do_publish(self, item, table):
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

        LOG.info("Starting publish...")

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
