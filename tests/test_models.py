from chexus import BucketItem, TableItem


def test_bucket_item():
    # Create BucketItem
    item = BucketItem(file_path="tests/test_data/somefile.txt")

    # Should have derived name from path
    assert item.path == "tests/test_data/somefile.txt"
    assert item.name == "somefile.txt"
    # Should have computed, assigned checksum
    assert (
        item.checksum
        == "ee21ae5cd21ff1bb2263f7c98a8557d42646ed1ec660d9c1f7c3f4e781bc6710"
    )
    # Should have assigned name to key
    assert item.key == item.name


def test_bucket_item_bad_path():
    # Create BucketItem
    item = BucketItem(file_path="bad/path/to/nowhere")

    # Should not have set a checksum
    assert item.checksum is None


def test_table_item():
    # Create TableItem
    item = TableItem(
        file_name="somefile",
        file_path="path/to/somefile",
        file_url="www.example.com/content/path/to/somefile",
        status=None,
        release_date="Feb. 1, 2020",
        release_time="12:30AM",
        release="On Feb. 1, 2020 at 12:30AM",
        bad_datetime="bats",
        metadata={"some": {"thing": [4, 5, 6]}},
    )

    # Should have added all attributes to it's attribute list,
    # converted valid datetimes to UTC, ignored invalid datetimes,
    # and allowed NoneTypes
    assert item.attrs == {
        "file_name": "somefile",
        "file_path": "path/to/somefile",
        "file_url": "www.example.com/content/path/to/somefile",
        "status": None,
        "release_date": "2020-02-01",
        "release_time": "05:30:00",
        "release": "2020-02-01T05:30:00+00:00",
        "bad_datetime": "bats",
        "metadata": '{"some": {"thing": [4, 5, 6]}}',
    }
    # Should have created class attributes for each kwarg
    for key, value in item.attrs.items():
        assert getattr(item, key) == value
