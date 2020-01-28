import logging

import mock
from boto3.dynamodb.conditions import Attr

from chexus import UploadItem, PublishItem, PushItem
from . import MockedClient


def test_push(caplog):
    """Can upload and publish PushItems"""
    item = PushItem(
        file_path="tests/test_data/somefile.txt",
        web_uri="www.example.com/test/content/somefile.txt",
    )

    client = MockedClient()
    mocked_bucket = client._session.resource().Bucket()
    mocked_table = client._session.resource().Table()

    # Searching for the file returns an iterable of matches
    mocked_bucket.objects.filter.return_value = []
    mocked_table.scan.return_value = {"Items": []}

    with caplog.at_level(logging.DEBUG):
        client.push(item, "test_bucket", "test_table")

    # Should've checked bucket for duplicate file...
    mocked_bucket.objects.filter.assert_called_with(Prefix=item.checksum)

    # ...and proceeded with upload
    assert "Content already present in s3 bucket" not in caplog.text

    # Should've uploaded
    mocked_bucket.upload_file.assert_called_with(item.path, item.checksum)
    assert "Upload complete" in caplog.text

    # Should've checked table for existing record...
    mocked_table.scan.assert_called_with(
        ProjectionExpression="object_key",
        FilterExpression=Attr("object_key").eq(item.object_key),
    )

    # ...and proceeded with publish
    assert "Table already up to date" not in caplog.text

    # Should've published
    mocked_table.put_item.assert_called_with(Item=item.attrs)
    assert "Publish complete" in caplog.text


@mock.patch("chexus.Client.upload")
@mock.patch("chexus.Client.publish")
def test_push_with_invalid_item(mocked_publish, mocked_upload, caplog):
    """Doesn't attempt to push invalid items"""

    # Mock upload and publish, as they shouldn't be called
    mocked_upload.return_value = mock.MagicMock()
    mocked_publish.return_value = mock.MagicMock()

    # Bunch of invalid items
    items = [
        {"Item": "Invalid"},
        "Not going to happen",
        UploadItem("tests/test_data/somefile.txt"),
        PublishItem("a41ef6", "www.example.com/test/content/nope.src.rpm"),
        [2, 4, 6, 8],
    ]

    client = MockedClient()

    with caplog.at_level(logging.DEBUG):
        client.push(
            items=items, bucket_name="test_bucket", table_name="test_table"
        )

    for msg in [
        "Expected type 'PushItem'",
        "dict",
        "str",
        "UploadItem",
        "PublishItem",
        "list",
    ]:
        assert msg in caplog.text

    mocked_upload.assert_not_called()
    mocked_publish.assert_not_called()
