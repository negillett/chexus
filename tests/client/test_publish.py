import logging

import mock
import pytest

from chexus import BucketItem, TableItem
from . import MockedClient


@pytest.mark.parametrize("dryrun", [True, False])
def test_publish(dryrun, caplog):
    """Can publish TableItems"""

    items = [
        TableItem(key1="test", key2=1234),
        TableItem(key1="testing", key2=5678),
    ]

    client = MockedClient()
    mocked_table = client._session.resource().Table()

    # Querying the table returns a dictionary with matching items
    mocked_table.query.return_value = {"Items": []}

    with caplog.at_level(logging.DEBUG):
        client.publish(items, "test_table", dryrun=dryrun)

    if dryrun:
        # Should've only logged what would've been done
        assert "Would publish" in caplog.text
        mocked_table.put_item.assert_not_called()
    else:
        # Should've checked table for existing record...
        assert mocked_table.query.call_count == 2

        # ...and proceeded with publish
        assert "Table already up to date" not in caplog.text

        mocked_table.put_item.assert_has_calls(
            [
                mock.call(Item={"key1": "test", "key2": 1234}),
                mock.call(Item={"key1": "testing", "key2": 5678}),
            ],
            any_order=True,
        )
        assert "Publish complete" in caplog.text


def test_publish_duplicate(caplog):
    """Doesn't attempt to duplicate table items"""

    item = TableItem(key1="test", key2=1234)

    client = MockedClient()
    mocked_table = client._session.resource().Table()

    # Querying the table returns a dictionary of matching record items
    mocked_table.query.return_value = {
        "Items": [{"key1": "test", "key2": 1234}]
    }

    # Expected table attributes
    mocked_table.attribute_definitions = [
        {"AttributeName": "key1", "AttributeType": "S"},
        {"AttributeName": "key2", "AttributeType": "S"},
    ]

    with caplog.at_level(logging.DEBUG):
        client.publish(item, "test_table")

    # Should've checked table for existing record...
    mocked_table.query.assert_called()
    # ...and found one
    assert "Item already exists in table" in caplog.text
    # Should not have tried to publish
    mocked_table.put_item.assert_not_called()


def test_publish_invalid_item(caplog):
    """Doesn't attempt to publish invalid items"""

    # Bunch of invalid items
    items = (
        {"Item": "Invalid"},
        "Not going to happen",
        BucketItem("tests/test_data/somefile.txt"),
        [2, 4, 6, 8],
    )

    client = MockedClient()

    with caplog.at_level(logging.DEBUG):
        client.publish(items=items, table_name="test_table")

    for msg in [
        "Expected type 'TableItem'",
        "dict",
        "str",
        "BucketItem",
        "list",
    ]:
        assert msg in caplog.text

    client._session.resource().Table().put_item.assert_not_called()


def test_publish_without_table_key(caplog):
    """Catches items missing keys required by the table"""

    item = TableItem(key1="test", key2=1234)

    client = MockedClient()
    mocked_table = client._session.resource().Table()

    # Querying the table returns a dictionary of matching record items
    mocked_table.query.return_value = {"Items": []}

    # Table contains unexpected keys
    mocked_table.attribute_definitions = [
        {"AttributeName": "Nope", "AttributeType": "S"},
    ]

    with caplog.at_level(logging.DEBUG):
        client.publish(item, "test_table")

    assert "Item to publish is missing required key, 'Nope'" in caplog.text
    # Should not have tried to publish
    mocked_table.put_item.assert_not_called()


def test_publish_error(caplog):
    item = TableItem(key1="test", key2=1234)

    client = MockedClient()
    mocked_table = client._session.resource().Table()

    # Querying the table returns a dictionary of matching record items
    mocked_table.query.return_value = {"Items": []}

    # Expected table attributes
    mocked_table.attribute_definitions = [
        {"AttributeName": "key1", "AttributeType": "S"},
        {"AttributeName": "key2", "AttributeType": "S"},
    ]

    # Some internal error when attempting to put the item
    mocked_table.put_item.side_effect = ValueError("Something went wrong")

    with caplog.at_level(logging.DEBUG):
        client.publish(item, "test_table")

    # Should've checked table for existing record...
    mocked_table.query.assert_called()

    for msg in [
        "One or more exceptions occurred during last operation",
        "Something went wrong",
    ]:
        assert msg in caplog.text
