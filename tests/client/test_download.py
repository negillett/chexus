import logging

import mock
import pytest
from botocore.exceptions import ClientError
from chexus import BucketItem, TableItem
from .mocked_client import MockedClient


@pytest.mark.parametrize("dryrun", [True, False])
def test_download(dryrun, caplog):
    """Can download BucketItems"""

    items = (
        BucketItem("tests/test_data/somefile.txt"),
        BucketItem("tests/test_data/somefile2.txt"),
        BucketItem("tests/test_data/somefile3.txt"),
    )

    client = MockedClient()
    mocked_bucket = client._session.resource().Bucket()

    # Searching for the file returns an iterable of matches
    mocked_bucket.download_file.return_value = []

    with caplog.at_level(logging.DEBUG):
        client.download(items, "test_bucket", dryrun=dryrun)

    # Expected calls to download_file
    download_calls = [mock.call(item.key, item.path) for item in items]

    if dryrun:
        # Should've only logged what would've been done
        for msg in ["Would download", "somefile.txt", "somefile2.txt"]:
            assert msg in caplog.text
        mocked_bucket.download_file.assert_not_called()
    else:
        # Should've downloaded
        mocked_bucket.download_file.assert_has_calls(
            download_calls, any_order=True
        )

    assert "Download complete" in caplog.text


def test_download_invalid_item(caplog):
    """Doesn't attempt to download invalid items"""

    # Bunch of invalid items
    items = [
        {"Item": "Invalid"},
        "Not going to happen",
        TableItem(key1="test", key2=1234),
        [2, 4, 6, 8],
    ]

    client = MockedClient()

    with caplog.at_level(logging.DEBUG):
        client.download(items=items, bucket_name="test_bucket")

    for msg in [
        "Expected type 'BucketItem'",
        "dict",
        "str",
        "TableItem",
        "list",
    ]:
        assert msg in caplog.text

    client._session.resource().Bucket().download_file.assert_not_called()


def test_download_exceptions(caplog):
    """Exceptions raised from download are expressed in error logging"""

    item = BucketItem("tests/test_data/somefile3.txt")

    client = MockedClient()
    # Files not preset in the S3 bucket
    client._session.resource().Bucket().download_file.side_effect = [
        ClientError({"Error": {"Code": "404"}}, "download")
    ]

    with caplog.at_level(logging.DEBUG):
        client.download(item, "test_bucket")

    for msg in [
        "One or more exceptions occurred during download",
        "An error occurred (404) when calling the download operation: Unknown",
    ]:
        assert msg in caplog.text
