import logging

import mock
import pytest

from chexus import UploadItem
from . import MockedClient
from boto3.exceptions import S3UploadFailedError


@pytest.mark.parametrize("dryrun", [True, False])
def test_upload(dryrun, caplog):
    item = UploadItem("tests/test_data/somefile.txt")

    client = MockedClient()
    mocked_bucket = client._session.resource().Bucket()

    # Searching for the file returns an iterable of matches
    mocked_bucket.objects.filter.return_value = []

    with caplog.at_level(logging.DEBUG):
        client.upload(item, "mocked_bucket", dryrun=dryrun)

    if dryrun:
        # Should've only logged what would've been done
        assert "Would upload" in caplog.text
        mocked_bucket.upload_file.assert_not_called()
    else:
        # Should've checked bucket for duplicate file...
        mocked_bucket.objects.filter.assert_called_with(Prefix=item.checksum)
        # ...and proceeded with upload
        assert "Content already present in s3 bucket" not in caplog.text
        # Should've uploaded
        mocked_bucket.upload_file.assert_called_with(item.path, item.checksum)

    assert "Upload complete" in caplog.text


def test_upload_duplicate(caplog):
    """Upload doesn't attempt to replace file objects"""

    item = UploadItem("tests/test_data/somefile.txt")

    client = MockedClient()
    # Searching for the file returns an iterable of matches
    client._session.resource().Bucket().objects.filter.return_value = [
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
    mocked_bucket = client._session.resource().Bucket()
    mocked_bucket.objects.filter.assert_called_with(Prefix=item.checksum)
    # ...and found one
    assert "Content already present in s3 bucket" in caplog.text
    # Should not have tried to upload
    mocked_bucket.upload_file.assert_not_called()


def test_upload_invalid_item(caplog):
    """Upload only accepts upload items"""

    client = MockedClient()

    client.upload(items={"Item": "Invalid"}, bucket_name="mocked_bucket")
    assert "Expected type 'UploadItem'" in caplog.text


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
        client.upload(items, "mocked_bucket")

    for msg in [
        "One or more exceptions occurred during upload",
        "Error uploading somefile3.txt",
        "Error uploading somefile2.txt",
    ]:
        assert msg in caplog.text
