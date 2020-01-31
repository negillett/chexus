import hashlib
import json
import os

import dateutil


class BucketItem(object):
    """Represents an object in an AWS S3 bucket"""

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


class TableItem(object):
    """Represents an item in an AWS DynamoDB table"""

    def __init__(self, **kwargs):
        self.attrs = kwargs

        for key, value in self.attrs.items():
            # Coerce any dates to a standard format
            if "date" in key and value:
                value = str(dateutil.parser.parse(value).date())
                self.attrs[key] = value

            # Serialize any dictionaries
            if isinstance(value, dict):
                value = json.dumps(value)
                self.attrs[key] = value

            # Create class attribute from kwarg
            if not hasattr(self, key):
                setattr(self, key, value)
