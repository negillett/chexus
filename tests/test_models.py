from chexus import BucketItem, TableItem


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
        from_date="Jan. 30, 2020",
        metadata={"some": {"thing": [4, 5, 6]}},
    )

    # Should have added all attributes to it's attribute list
    assert item.attrs == {
        "web_uri": "www.example.com/nothing",
        "object_key": "a8d83f1e",
        "first": 1,
        "second": 2,
        "third": 3,
        "from_date": "2020-01-30",
        "metadata": '{"some": {"thing": [4, 5, 6]}}',
    }
