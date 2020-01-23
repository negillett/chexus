import datetime

from chexus import UploadItem, PublishItem, PushItem


def test_upload_item():
    """Item created from file path creates name and checksum attrs"""

    item = UploadItem(file_path="tests/test_data/somefile.txt")
    assert item.name == "somefile.txt"
    assert (
        item.checksum
        == "ee21ae5cd21ff1bb2263f7c98a8557d42646ed1ec660d9c1f7c3f4e781bc6710"
    )


def test_publish_item():
    """Item is created with correct attributes"""

    item = PublishItem(
        web_uri="www.example.com/nothing",
        object_key="a8d83f1e",
        first=1,
        second=2,
        third=3,
        metadata={"some": {"thing": [4, 5, 6]}},
    )

    # Should have dynamically created the following attributes
    attrs = ("first", "second", "third", "metadata")
    for attr in attrs:
        assert hasattr(item, attr)

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
        first=1,
        second=2,
        third=3,
        metadata={"some": {"thing": [4, 5, 6]}},
    )

    assert item.name == "somefile.txt"
    assert (
        item.checksum
        == "ee21ae5cd21ff1bb2263f7c98a8557d42646ed1ec660d9c1f7c3f4e781bc6710"
    )

    # Should have dynamically created the following attributes
    attrs = ("first", "second", "third", "metadata")
    for attr in attrs:
        assert hasattr(item, attr)

    # Should have added all attributes to it's attribute list
    assert item.attrs == {
        "web_uri": "www.example.com/nothing",
        "object_key": "ee21ae5cd21ff1bb2263f7c98a8557d42646ed1ec660d9c1f7c3f4e781bc6710",
        "from_date": str(datetime.datetime.now().date()),
        "first": 1,
        "second": 2,
        "third": 3,
        "metadata": '{"some": {"thing": [4, 5, 6]}}',
    }
