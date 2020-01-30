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
        first=1,
        second=2,
        third=3,
        date="Jan. 30, 2020",
        metadata={"some": {"thing": [4, 5, 6]}},
    )

    # Should've stored kwargs in attrs and serialized metadata
    assert item.attrs == {
        "first": 1,
        "second": 2,
        "third": 3,
        "date": "2020-01-30",
        "metadata": '{"some": {"thing": [4, 5, 6]}}',
    }

    # Should have created class attributes for the given kwargs
    assert hasattr(item, "first")
    assert hasattr(item, "second")
    assert hasattr(item, "third")
    assert hasattr(item, "metadata")
