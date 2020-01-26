import logging

import pytest
from boto3.dynamodb.conditions import Attr

from chexus import PublishItem
from .mocked_client import MockedClient


@pytest.mark.parametrize("dryrun", [True, False])
def test_publish(dryrun, caplog):
    # pylint disable=protected_member

    item = PublishItem("www.example.com/test/content/somefile.txt", "a6e9f3")

    client = MockedClient()
    # Scanning the table returns a dictionary of matching record items
    client._session.resource().Table().scan.return_value = {"Items": []}

    with caplog.at_level(logging.DEBUG):
        client.publish(item, "test_table", dryrun=dryrun)

    # Should've checked table for existing record...
    tested_table = client._session.resource().Table()
    tested_table.scan.assert_called_with(
        ProjectionExpression="object_key",
        FilterExpression=Attr("object_key").eq(item.object_key),
    )
    # ...and proceeded with publish
    assert "Table already up to date" not in caplog.text

    if dryrun:
        # Should've only logged what would've been done
        assert "Would publish" in caplog.text
        tested_table.put_item.assert_not_called()
    else:
        # Should've published
        tested_table.put_item.assert_called_with(Item=item.attrs)
        assert "Publish complete" in caplog.text


def test_upload_duplicate(caplog):
    # pylint disable=protected_member

    item = PublishItem("www.example.com/test/content/somefile.txt", "a6e9f3")

    client = MockedClient()
    # Scanning the table returns a dictionary of matching record items
    client._session.resource().Table().scan.return_value = {
        "Items": [
            {
                "web_uri": item.web_uri,
                "from_date": item.from_date,
                "object_key": item.object_key,
            }
        ]
    }

    # Expected table attributes
    client._session.resource().Table.attribute_definitions = [
        {"AttributeName": "object_key", "AttributeType": "S"},
        {"AttributeName": "from_date", "AttributeType": "S"},
    ]

    with caplog.at_level(logging.DEBUG):
        client.publish(item, "test_table")

    # Should've checked table for existing record...
    tested_table = client._session.resource().Table()
    tested_table.scan.assert_called_with(
        ProjectionExpression="object_key",
        FilterExpression=Attr("object_key").eq(item.object_key),
    )
    # ...and found one
    assert "Table already up to date" in caplog.text
    # Should not have tried to publish
    tested_table.put_item.assert_not_called()


def test_publish_without_table_key(caplog):
    # pylint disable=protected_member

    item = PublishItem("www.example.com/test/content/somefile.txt", "a6e9f3")

    client = MockedClient()
    # Scanning the table returns a dictionary of matching record items
    client._session.resource().Table().scan.return_value = {"Items": []}

    # Table contains unexpected keys
    client._session.resource().Table().attribute_definitions = [
        {"AttributeName": "Nope", "AttributeType": "S"}
    ]

    with pytest.raises(ValueError) as err:
        print(item.attrs)
        client.publish(item, "test_table")

    assert str(err.value) == "Content to publish is missing key, 'Nope'"
