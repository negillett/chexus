import mock

from chexus import PushItem
from . import MockedClient


@mock.patch("chexus.Client.upload")
@mock.patch("chexus.Client.publish")
def test_push(mocked_publish, mocked_upload):
    item = PushItem(
        file_path="tests/test_data/somefile.txt",
        web_uri="www.example.com/test/content/somefile.txt",
    )

    # Mock upload and publish, as they're already tested
    mocked_upload.return_value = mock.MagicMock()
    mocked_publish.return_value = mock.MagicMock()

    client = MockedClient()
    client.push(item, "test_bucket", "test_table")


@mock.patch("chexus.Client.upload")
@mock.patch("chexus.Client.publish")
def test_push_with_invalid_item(mocked_publish, mocked_upload):
    """Push only accepts PushItem objects"""

    # Mock upload and publish, as they're already tested
    mocked_upload.return_value = mock.MagicMock()
    mocked_publish.return_value = mock.MagicMock()

    client = MockedClient()
    client.push({"Item": "Invalid"}, "test_bucket", "test_table")
