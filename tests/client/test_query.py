import logging

import pytest

from chexus import TableItem
from . import MockedClient


def test_query():
    item = TableItem(key1=1234, attr1="hello")

    client = MockedClient()
    mocked_table = client._session.resource().Table()

    # Querying the table returns a dictionary with matching items
    mocked_table.query.return_value = {
        "Items": [{"key1": 1234, "att1": "foo"}]
    }

    # Expected table attributes
    mocked_table.attribute_definitions = [
        {"AttributeName": "key1", "AttributeType": "S"},
    ]

    client.search(item, "test_table")

    mocked_table.query.assert_called_with(
        ExpressionAttributeValues={":key1val": "1234", ":attr1val": "hello"},
        FilterExpression="attr1 = :attr1val",
        KeyConditionExpression="key1 = :key1val",
        Select="ALL_ATTRIBUTES",
    )


def test_query_invalid_item(caplog):
    client = MockedClient()
    mocked_table = client._session.resource().Table()

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(ValueError) as err:
            client.search("not a table item", "test_table")

    for msg in ["Expected type 'TableItem'", "str"]:
        assert msg in str(err.value)

    mocked_table.query.assert_not_called()


def test_query_response_too_large(caplog):
    item = TableItem(key1=1234, attr1="hello")

    client = MockedClient()
    mocked_table = client._session.resource().Table()

    # Querying the table returns a dictionary with matching items
    mocked_table.query.return_value = {
        "Items": ["Lots of items..."],
        "LastEvaluatedKey": {"key1": "test"},
    }

    # Expected table attributes
    mocked_table.attribute_definitions = [
        {"AttributeName": "key1", "AttributeType": "S"},
    ]

    with caplog.at_level(logging.DEBUG):
        client.search(item, "test_table")

    mocked_table.query.assert_called_with(
        ExpressionAttributeValues={":key1val": "1234", ":attr1val": "hello"},
        FilterExpression="attr1 = :attr1val",
        KeyConditionExpression="key1 = :key1val",
        Select="ALL_ATTRIBUTES",
    )

    assert "Query limit reached, results truncated" in caplog.text
