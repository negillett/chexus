import hashlib
import json
import os
from datetime import datetime

import dateutil


class UploadItem(object):
    """Represents an item to upload to S3"""

    def __init__(self, file_path, file_name=None, checksum=None):
        self.path = file_path
        self.name = file_name or os.path.basename(self.path)
        self.checksum = checksum

        if not self.checksum:
            sha256 = hashlib.sha256()
            with open(self.path, "rb") as binary:
                while True:
                    chunk = binary.read(4096)
                    if not chunk:
                        break
                    sha256.update(chunk)

            self.checksum = sha256.hexdigest()


class PublishItem(object):
    """Represents item data to publish to DynamoDB"""

    def __init__(self, web_uri, object_key, from_date=None, **kwargs):
        self.web_uri = web_uri
        self.object_key = object_key
        self.from_date = self._valid_date(from_date)
        self.attrs = kwargs
        self.attrs.update(
            {
                "web_uri": self.web_uri,
                "object_key": self.object_key,
                "from_date": self.from_date,
            }
        )

        # Serialize any
        if self.attrs.get("metadata", None):
            self.attrs["metadata"] = json.dumps(self.attrs["metadata"])

        for key, value in self.attrs.items():
            setattr(self, key, value)

    @staticmethod
    def _valid_date(date):
        if date:
            date_obj = dateutil.parser.parse(date)
            return str(date_obj.date())
        return str(datetime.now().date())


class PushItem(UploadItem, PublishItem):
    """Represents an item to upload to S3 and publish to DynamoDB"""

    def __init__(self, file_path, web_uri, from_date=None, **kwargs):
        self.file_name = kwargs.pop("file_name", None)
        self.checksum = kwargs.pop("checksum", None)

        UploadItem.__init__(self, file_path, self.file_name, self.checksum)
        PublishItem.__init__(self, web_uri, self.checksum, from_date, **kwargs)
