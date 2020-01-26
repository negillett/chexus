import logging

import pytest

from chexus import UploadItem
from .mocked_client import MockedClient


@pytest.mark.parametrize("dryrun", [True, False])
def test_upload(dryrun, caplog):
    # pylint disable=protected_member

    item = UploadItem("tests/test_data/somefile.txt")

    client = MockedClient()
    # Searching for the file returns an iterable of matches
    client._session.resource().Bucket().objects.filter.return_value = []

    with caplog.at_level(logging.DEBUG):
        client.upload(item, "test_bucket", dryrun=dryrun)

    # Should've checked bucket for duplicate file...
    tested_bucket = client._session.resource().Bucket()
    tested_bucket.objects.filter.assert_called_with(Prefix=item.checksum)
    # ...and proceeded with upload
    assert "Content already present in s3 bucket" not in caplog.text

    if dryrun:
        # Should've only logged what would've been done
        assert "Would upload" in caplog.text
        tested_bucket.upload_file.assert_not_called()
    else:
        # Should've uploaded
        tested_bucket.upload_file.assert_called_with(item.path, item.checksum)
        assert "Upload complete" in caplog.text


def test_upload_duplicate(caplog):
    # pylint disable=protected_member

    item = UploadItem("tests/test_data/somefile.txt")

    client = MockedClient()
    # Searching for the file returns an iterable of matches
    client._session.resource().Bucket().objects.filter.return_value = [
        {"ObjectSummary_obj": {"key": item.checksum, "bucket": "test_bucket"}},
    ]

    with caplog.at_level(logging.DEBUG):
        client.upload(item, "test_bucket")

    # Should've checked bucket for duplicate file...
    tested_bucket = client._session.resource().Bucket()
    tested_bucket.objects.filter.assert_called_with(Prefix=item.checksum)
    print(tested_bucket.objects.filter(Prefix=item.checksum))
    # ...and found one
    assert "Content already present in s3 bucket" in caplog.text
    # Should not have tried to upload
    tested_bucket.upload_file.assert_not_called()
