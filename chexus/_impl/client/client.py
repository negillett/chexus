import json
import logging
import time

import boto3
from more_executors import Executors

from ..models import BucketItem, TableItem

LOG = logging.getLogger("chexus")


class Client(object):
    """A client for interacting with Amazon S3 and DynamoDB.

    This class provides methods for uploading to and downloading from
    an S3 bucket and methods for searching for and putting items into a
    DynamoDB table.

    Args:
        access_id (str)
            Access ID for Amazon services. If no ID is provided,
            attempts to find it among environment variables and
            configuration files will be made.

        access_key (str)
            Access key for Amazon services. If no key is provided,
            attempts to find it among environment variables and
            configuration files will be made.

        session_token (str)
            Session token for Amazon services. If no token is provided,
            attempts to find it among environment variables and
            configuration files will be made.

        default_region (str)
            Default region for Amazon services. If no region is
            provided, attempts to find it among environment variables
            and configuration files will be made.

        workers_count (int)
            Maximum number of threads in which a task may be executed.

        retry_count (int)
            Maximum number of times to retry a failed task.
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
    def _to_list(items):
        if not isinstance(items, (list, tuple)):
            items = [items]
        if isinstance(items, tuple):
            items = list(items)
        return items

    @staticmethod
    def _wait_for_futures(futures_list):
        # Not using concurrent.futures.as_completed() due to Python 2.7
        while not all([ft.done() for ft in futures_list]):
            time.sleep(1)

        # Report any failures as errors -- raising them could
        # prevent other files from uploading
        errors = [ft.exception() for ft in futures_list]
        # Filter possible NoneTypes
        if [err for err in errors if err]:
            LOG.error(
                "One or more exceptions occurred during last operation;\n\t%s",
                "\n\t".join(str(err) for err in errors),
            )

    @staticmethod
    def _should_upload(key, bucket):
        if list(bucket.objects.filter(Prefix=key)):
            LOG.info("Item already in s3 bucket")
            return False
        return True

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

        bucket = self._session.resource("s3").Bucket(bucket_name)

        LOG.info("Starting upload...")

        futures = []
        for item in self._to_list(items):
            if not isinstance(item, BucketItem):
                LOG.error(
                    "Expected type 'BucketItem', got '%s' instead", type(item)
                )
                continue

            if not self._should_upload(item.key, bucket):
                continue

            if dryrun:
                LOG.info(
                    "Would upload %s to the '%s' bucket",
                    item.name,
                    bucket_name,
                )
                continue

            futures.append(
                self._executor.submit(
                    bucket.upload_file,
                    item.path,
                    item.key,
                    ExtraArgs=item.content_type,
                )
            )

        self._wait_for_futures(futures)
        LOG.info("Upload complete")

    def download(self, items, bucket_name, dryrun=False):
        """Efficiently downloads files from the specified S3 bucket.

        Args:
            items (:class:`~chexus.BucketItem`, list)
                One or more representations of an item to download from
                the bucket.

            bucket_name (str)
                The name of the bucket from which the file will be
                downloaded.

            dryrun (bool)
                If true, only log what would be downloaded.
        """

        bucket = self._session.resource("s3").Bucket(bucket_name)

        LOG.info("Starting download...")

        futures = []
        for item in self._to_list(items):
            if not isinstance(item, BucketItem):
                LOG.error(
                    "Expected type 'BucketItem', got '%s' instead", type(item)
                )
                continue

            if dryrun:
                LOG.info(
                    "Would download %s from the '%s' bucket",
                    item.name,
                    bucket_name,
                )
                continue

            futures.append(
                self._executor.submit(
                    bucket.download_file, item.key, item.path
                )
            )

        self._wait_for_futures(futures)
        LOG.info("Download complete")

    @staticmethod
    def _search_table_item(item, table):
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

    def search(self, item, table_name, region=None):
        """Queries the specified table for an item matching the given
        TableItem.

        Args:
            item (:class:`~chexus.TableItem`)
                A representation of a DynamoDB table item.

            table_name (str)
                The name of the table to search.

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

        db_resource = self._session.resource("dynamodb", region_name=region)
        table = db_resource.Table(table_name)

        return self._search_table_item(item, table)

    def _should_publish(self, item, table):
        for att in [
            str(a["AttributeName"]) for a in table.attribute_definitions
        ]:
            if not hasattr(item, att) or not getattr(item, att):
                LOG.error("Item to publish is missing required key, '%s'", att)
                return False

        response = self._search_table_item(item, table)

        if response["Items"]:
            LOG.info("Item already exists in table")
            return False

        return True

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

        db_resource = self._session.resource("dynamodb", region_name=region)
        table = db_resource.Table(table_name)

        LOG.info("Starting publish...")

        futures = []
        for item in self._to_list(items):
            if not isinstance(item, TableItem):
                LOG.error(
                    "Expected type 'TableItem', got '%s' instead", type(item)
                )
                continue

            if not self._should_publish(item, table):
                continue

            if dryrun:
                LOG.info(
                    "Would publish the following item to the '%s' table;\n\t%s",
                    table.name,
                    json.dumps(item.attrs, sort_keys=True, indent=4),
                )
                continue

            futures.append(
                self._executor.submit(table.put_item, Item=item.attrs)
            )

        self._wait_for_futures(futures)
        LOG.info("Publish complete")

    def remove_bucket_items(self, items, bucket_name=None, dryrun=False):
        """Removes the given items from the specified S3 bucket.

        Args:
            items (list, :class:`~chexus.BucketItem`)
                One or more representations of an item to remove from a
                table and/or bucket.

            bucket_name (str)
                The name of the bucket to which the item will be
                uploaded.

            dryrun (bool)
                If true, only log what would be removed.
        """

        items = self._to_list(items)
        bucket_items = [item for item in items if isinstance(item, BucketItem)]
        non_bucket_items = [item for item in items if item not in bucket_items]

        if non_bucket_items:
            LOG.error(
                "Expected type 'BucketItem', got the following instead: %s",
                ", ".join(type(item) for item in non_bucket_items),
            )

        LOG.info("Starting removal...")

        futures = []

        assert bucket_name
        bucket = self._session.resource("s3").Bucket(bucket_name)

        item_keys = [item.key for item in bucket_items]
        key_list = "\n\t".join(item_keys)

        if dryrun:
            LOG.info(
                "Would remove the following items from the %s bucket;\n\t%s",
                bucket_name,
                key_list,
            )
        else:
            LOG.info("Removing the following items;\n\t%s", key_list)
            futures.append(
                self._executor.submit(
                    bucket.delete_objects,
                    Delete={"Object": [{"Key": key} for key in item_keys]},
                )
            )

        self._wait_for_futures(futures)
        LOG.info("Removal complete")

    def remove_table_items(
        self, items, table_name=None, region=None, dryrun=False
    ):
        """Removes the given items from their respective storage locations.

        Args:
            items (
                list, :class:`~chexus.TableItem`, :class:`~chexus.BucketItem`
            )
                One or more representations of an item to remove from a
                table and/or bucket.

            table_name (str)
                The name of the table in which the item will be
                published.

            region (str)
                The name of the AWS region the desired table belongs
                to. If not provided here or to the calling client,
                attempts to find it among environment variables and
                configuration files will be made.

            dryrun (bool)
                If true, only log what would be removed.
        """

        items = self._to_list(items)
        table_items = [item for item in items if isinstance(item, TableItem)]
        non_table_items = [item for item in items if item not in table_items]

        if non_table_items:
            LOG.error(
                "Expected type 'TableItem', got the following instead: %s",
                ", ".join(type(item) for item in non_table_items),
            )

        LOG.info("Starting removal...")

        futures = []
        if table_items:
            assert table_name
            db_client = self._session.client("dynamodb", region_name=region)
            table_desc = db_client.describe_table(TableName=table_name)

            # Identify primary key name
            key_name = [
                key["AttributeName"]
                for key in table_desc["Table"]["KeySchema"]
                if key["KeyType"] == "HASH"
            ][0]
            # Identify primary key type
            key_type = [
                d["AttributeType"]
                for d in table_desc["Table"]["AttributeDefinitions"]
                if d["AttributeName"] == key_name
            ][0]

            # String-friendly list of items
            item_list = "\n\t".join(
                [
                    json.dumps(item.attrs, sort_keys=True, indent=4)
                    for item in table_items
                ]
            )

            if dryrun:
                LOG.info(
                    "Would remove the following items from the %s table;\n\t%s",
                    table_name,
                    item_list,
                )
            else:
                LOG.info("Removing the following items;\n\t%s", item_list)
                futures.append(
                    self._executor.submit(
                        db_client.delete_item,
                        TableName=table_name,
                        Key={key_name: {key_type: getattr(item, key_name)}},
                    )
                    for item in table_items
                )

        self._wait_for_futures(futures)
        LOG.info("Removal complete")
