import logging

import mock
import pytest
from boto3.dynamodb.conditions import Attr

from chexus import UploadItem, PublishItem, PushItem
from . import MockedClient


@pytest.mark.parametrize("dryrun", [True, False])
def test_publish(dryrun, caplog):
    """Can publish PublishItems and PushItems""" ""

    items = [
        PublishItem("www.example.com/test/content/somefile.txt", "a6e9f3"),
        PushItem(
            "tests/test_data/somefile2.txt",
            "www.example.com/test/content/somefile2.txt",
        ),
    ]

    client = MockedClient()
    mocked_table = client._session.resource().Table()

    # Scanning the table returns a dictionary of matching record items
    mocked_table.scan.return_value = {"Items": []}

    with caplog.at_level(logging.DEBUG):
        client.publish(items, "test_table", dryrun=dryrun)

    # Expected calls to Bucket methods objects.filters and upload_file
    scan_calls = [
        mock.call(
            ProjectionExpression="object_key",
            FilterExpression=Attr("object_key").eq(item.object_key),
        )
        for item in items
    ]
    publish_calls = [mock.call(Item=item.attrs) for item in items]

    if dryrun:
        # Should've only logged what would've been done
        assert "Would publish" in caplog.text
        mocked_table.put_item.assert_not_called()
    else:
        # Should've checked table for existing record...
        mocked_table.scan.assert_has_calls(scan_calls, any_order=True)

        # ...and proceeded with publish
        assert "Table already up to date" not in caplog.text

        # Should've published
        mocked_table.put_item.assert_has_calls(publish_calls, any_order=True)
        assert "Publish complete" in caplog.text


def test_publish_duplicate(caplog):
    """Doesn't attempt to duplicate record items"""

    item = PublishItem("www.example.com/test/content/somefile.txt", "a6e9f3")

    client = MockedClient()
    mocked_table = client._session.resource().Table()

    # Scanning the table returns a dictionary of matching record items
    mocked_table.scan.return_value = {
        "Items": [
            {
                "web_uri": item.web_uri,
                "from_date": item.from_date,
                "object_key": item.object_key,
            }
        ]
    }

    # Expected table attributes
    mocked_table.attribute_definitions = [
        {"AttributeName": "object_key", "AttributeType": "S"},
        {"AttributeName": "from_date", "AttributeType": "S"},
    ]

    with caplog.at_level(logging.DEBUG):
        client.publish(item, "test_table")

    # Should've checked table for existing record...
    mocked_table.scan.assert_called_with(
        ProjectionExpression="object_key",
        FilterExpression=Attr("object_key").eq(item.object_key),
    )
    # ...and found one
    assert "Table already up to date" in caplog.text
    # Should not have tried to publish
    mocked_table.put_item.assert_not_called()


def test_publish_invalid_item(caplog):
    """Doesn't attempt to publish invalid items"""

    # Bunch of invalid items
    items = [
        {"Item": "Invalid"},
        "Not going to happen",
        UploadItem("tests/test_data/somefile.txt"),
        [2, 4, 6, 8],
    ]

    client = MockedClient()

    with caplog.at_level(logging.DEBUG):
        client.publish(items=items, table_name="test_table")

    for msg in [
        "Expected type 'PublishItem' or 'PushItem'",
        "dict",
        "str",
        "UploadItem",
        "list",
    ]:
        assert msg in caplog.text

    client._session.resource().Table().put_item.assert_not_called()


def test_publish_without_table_key(caplog):
    """Catches items missing keys required by the table"""

    item = PublishItem("www.example.com/test/content/somefile.txt", "a6e9f3")

    client = MockedClient()
    mocked_table = client._session.resource().Table()

    # Scanning the table returns a dictionary of matching record items
    mocked_table.scan.return_value = {"Items": []}

    # Table contains unexpected keys
    mocked_table.attribute_definitions = [
        {"AttributeName": "Nope", "AttributeType": "S"}
    ]

    with caplog.at_level(logging.DEBUG):
        client.publish(item, "test_table")

    assert "Content to publish is missing key, 'Nope'" in caplog.text
