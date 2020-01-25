import json
import logging
import os

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from ..models import PushItem

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

    @staticmethod
    def _should_upload(checksum, bucket):
        LOG.info("Checking upload item...")

        if list(bucket.objects.filter(Prefix=checksum)):
            LOG.info("Content already present in s3 bucket")
            return False
        return True

    def upload(self, item, bucket_name, dryrun=False):
        """Uploads an item to the specified S3 bucket.

        Args:
            item (:class:`~pubtools.aws.UploadItem`)
                A representation of the item to upload to the bucket.

            bucket_name (str)
                The name of the bucket to which the item will be
                uploaded.

            dryrun (bool)
                If true, only log what would be uploaded.
        """

        s3_bucket = self._session.resource("s3").Bucket(bucket_name)

        if not self._should_upload(item.checksum, s3_bucket):
            return

        if dryrun:
            LOG.info(
                "Would upload %s to the '%s' bucket", item.name, bucket_name
            )
            return

        LOG.info("Uploading %s...", item.name)

        s3_bucket.upload_file(item.path, item.checksum)
        LOG.info("Upload complete")

    @staticmethod
    def _should_publish(object_key, ddb_table):
        LOG.info("Checking publish...")

        response = ddb_table.scan(
            ProjectionExpression="object_key",
            FilterExpression=Attr("object_key").eq(object_key),
        )

        if response["Items"]:
            LOG.info("Table already up to date")
            return False

        return True

    def publish(self, item, table_name, region=None, dryrun=False):
        """Publishes an item to the specified DynamoDB table.

        Args:
            item (:class:`~chexus.aws.PublishItem)
                A representation of the data to publish to the table.

            table_name (str)
                The name of the table to which the data will be
                published.

            region (str)
                The name of the AWS region the desired table belongs
                to. If not provided here or to the calling client,
                attempts to find it among environment variables and
                ~/.aws/config file will be made.

            dryrun (bool)
                If true, only log what would be published.
        """
        ddb_table = self._session.resource(
            "dynamodb", region_name=region
        ).Table(table_name)

        if not self._should_publish(item.object_key, ddb_table):
            return

        if dryrun:
            LOG.info(
                "Would publish %s to the '%s' table with the following data;\n%s",
                os.path.basename(item.web_uri),
                table_name,
                json.dumps(item.attrs, sort_keys=True, indent=4),
            )
            return

        LOG.info("Publishing %s...", os.path.basename(item.web_uri))

        attrs = ddb_table.attribute_definitions
        for attr in attrs:
            if not hasattr(item, attr["AttributeName"]):
                raise ValueError(
                    "Content to publish is missing key, '%s'"
                    % attr["AttributeName"],
                )

        ddb_table.put_item(Item=item.attrs)

        LOG.info("Publish complete")

    def push(self, items, bucket_name, table_name, region=None, dryrun=False):
        """Uploads items to the specified S3 bucket and publishes data
        about the item to the specified DynamoDB table.

        Args:
            items (:class:`~chexus.PushItem`, list)
                One or more representations of an item to upload to the
                bucket and publish to the table.

            bucket_name (str)
                The name of the bucket to which the item will be
                uploaded.

            table_name (str)
                The name of the table to which the data will be
                published.

            region (str)
                The name of the AWS region the desired table belongs
                to. If not provided here or to the calling client,
                attempts to find it among environment variables and
                ~/.aws/config file will be made.

            dryrun (bool)
                If true, only log what would be uploaded.
        """

        if not isinstance(items, (list, tuple)):
            items = [items]

        for item in items:
            if not isinstance(item, PushItem):
                LOG.error(
                    "Expected type 'PushItem', got '%s' instead", type(item)
                )
                continue

            self.upload(item=item, bucket_name=bucket_name, dryrun=dryrun)
            self.publish(
                item=item, table_name=table_name, region=region, dryrun=dryrun
            )
