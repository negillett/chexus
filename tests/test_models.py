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
    # Create TableItem
    item = TableItem(
        file_name="somefile",
        file_path="path/to/somefile",
        file_url="www.example.com/content/path/to/somefile",
        release_date="Jan. 30, 2020",
        release_time="12:30AM",
        release_datetime="On Jan. 30, 2020 at 12:30AM",
        metadata={"some": {"thing": [4, 5, 6]}},
    )

    # Should have added all attributes to it's attribute list and
    # converted dates and times
    assert item.attrs == {
        "file_name": "somefile",
        "file_path": "path/to/somefile",
        "file_url": "www.example.com/content/path/to/somefile",
        "release_date": "2020-01-30",
        "release_time": "05:30:00",
        "release_datetime": "2020-01-30T05:30:00+00:00",
        "metadata": '{"some": {"thing": [4, 5, 6]}}',
    }
    # Should have created class attributes for each kwarg
    assert item.file_name == item.attrs["file_name"]
    assert item.file_path == item.attrs["file_path"]
    assert item.file_url == item.attrs["file_url"]
    assert item.release_date == item.attrs["release_date"]
    assert item.release_time == item.attrs["release_time"]
    assert item.release_datetime == item.attrs["release_datetime"]
    assert item.metadata == item.attrs["metadata"]
