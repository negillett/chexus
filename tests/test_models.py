import datetime

from chexus import BucketItem, TableItem, PushItem


def test_bucket_item():
    item = BucketItem(file_path="tests/test_data/somefile.txt")
    assert item.name == "somefile.txt"
    assert item.path == "tests/test_data/somefile.txt"
    assert (
        item.checksum
        == "ee21ae5cd21ff1bb2263f7c98a8557d42646ed1ec660d9c1f7c3f4e781bc6710"
    )


def test_table_item():
    item = TableItem(
        web_uri="www.example.com/nothing",
        object_key="a8d83f1e",
        first=1,
        second=2,
        third=3,
        metadata={"some": {"thing": [4, 5, 6]}},
    )

    # Should have added all attributes to it's attribute list
    assert item.attrs == {
        "web_uri": "www.example.com/nothing",
        "object_key": "a8d83f1e",
        "from_date": str(datetime.datetime.now().date()),
        "first": 1,
        "second": 2,
        "third": 3,
        "metadata": '{"some": {"thing": [4, 5, 6]}}',
    }


def test_push_item():
    item = PushItem(
        file_path="tests/test_data/somefile.txt",
        web_uri="www.example.com/nothing",
        from_date="Nov 19 2022",
        first=1,
        second=2,
        third=3,
        metadata={"some": {"thing": [4, 5, 6]}},
    )

    assert item.name == "somefile.txt"
    assert item.path == "tests/test_data/somefile.txt"
    assert (
        item.checksum
        == "ee21ae5cd21ff1bb2263f7c98a8557d42646ed1ec660d9c1f7c3f4e781bc6710"
    )

    # Should have added all attributes to it's attribute list
    assert item.attrs == {
        "web_uri": "www.example.com/nothing",
        "object_key": "ee21ae5cd21ff1bb2263f7c98a8557d42646ed1ec660d9c1f7c3f4e781bc6710",
        "from_date": "2022-11-19",
        "first": 1,
        "second": 2,
        "third": 3,
        "metadata": '{"some": {"thing": [4, 5, 6]}}',
    }
