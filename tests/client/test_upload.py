import logging

import mock
import pytest
from boto3.exceptions import S3UploadFailedError

from chexus import UploadItem, PublishItem, PushItem
from . import MockedClient


@pytest.mark.parametrize("dryrun", [True, False])
def test_upload(dryrun, caplog):
    """Can upload UploadItems and PushItems"""

    items = (
        UploadItem("tests/test_data/somefile.txt"),
        PushItem(
            "tests/test_data/somefile2.txt",
            "www.example.com/test/content/somefile2.txt",
        ),
    )

    client = MockedClient()
    mocked_bucket = client._session.resource().Bucket()

    # Searching for the file returns an iterable of matches
    mocked_bucket.objects.filter.return_value = []

    with caplog.at_level(logging.DEBUG):
        client.upload(items, "test_bucket", dryrun=dryrun)

    # Expected calls to Bucket methods objects.filters and upload_file
    objects_calls = [mock.call(Prefix=item.checksum) for item in items]
    upload_calls = [mock.call(item.path, item.checksum) for item in items]

    if dryrun:
        # Should've only logged what would've been done
        for msg in ["Would upload", "somefile.txt", "somefile2.txt"]:
            assert msg in caplog.text
        mocked_bucket.upload_file.assert_not_called()
    else:
        # Should've checked bucket for duplicate file...
        mocked_bucket.objects.filter.assert_has_calls(
            objects_calls, any_order=True
        )

        # ...and proceeded with upload
        assert "Content already present in s3 bucket" not in caplog.text

        # Should've uploaded
        mocked_bucket.upload_file.assert_has_calls(
            upload_calls, any_order=True
        )

    assert "Upload complete" in caplog.text


def test_upload_duplicate(caplog):
    """Doesn't attempt to replace file objects"""

    item = UploadItem("tests/test_data/somefile.txt")

    client = MockedClient()
    mocked_bucket = client._session.resource().Bucket()

    # Searching for the file returns an iterable of matches
    mocked_bucket.objects.filter.return_value = [
        {
            "ObjectSummary_obj": {
                "key": item.checksum,
                "bucket": "mocked_bucket",
            }
        },
    ]

    with caplog.at_level(logging.DEBUG):
        client.upload(item, "mocked_bucket")

    # Should've checked bucket for duplicate file...
    mocked_bucket.objects.filter.assert_called_with(Prefix=item.checksum)
    # ...and found one
    assert "Content already present in s3 bucket" in caplog.text
    # Should not have tried to upload
    mocked_bucket.upload_file.assert_not_called()


def test_upload_invalid_item(caplog):
    """Doesn't attempt to upload invalid items"""

    # Bunch of invalid items
    items = [
        {"Item": "Invalid"},
        "Not going to happen",
        PublishItem("a41ef6", "www.example.com/test/content/nope.src.rpm"),
        [2, 4, 6, 8],
    ]

    client = MockedClient()

    with caplog.at_level(logging.DEBUG):
        client.upload(items=items, bucket_name="test_bucket")

    for msg in [
        "Expected type 'UploadItem' or 'PushItem'",
        "dict",
        "str",
        "PublishItem",
        "list",
    ]:
        assert msg in caplog.text

    client._session.resource().Bucket().upload_file.assert_not_called()


def test_upload_exceptions(caplog):
    """Exceptions raised from upload are expressed in error logging"""

    items = [
        UploadItem("tests/test_data/somefile3.txt"),
        UploadItem("tests/test_data/somefile2.txt"),
        UploadItem("tests/test_data/somefile.txt"),
    ]

    client = MockedClient()
    # Uploading fails twice before succeeding
    client._session.resource().Bucket().upload_file.side_effect = [
        S3UploadFailedError("Error uploading somefile3.txt"),
        S3UploadFailedError("Error uploading somefile2.txt"),
        mock.DEFAULT,
    ]

    with caplog.at_level(logging.DEBUG):
        client.upload(items, "test_bucket")

    for msg in [
        "One or more exceptions occurred during upload",
        "Error uploading somefile3.txt",
        "Error uploading somefile2.txt",
    ]:
        assert msg in caplog.text
