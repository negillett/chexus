import hashlib
import json
import os

import dateutil
import pytz


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
        self._set_attributes()

    def _set_attributes(self):
        for key, value in self.attrs.items():
            # Skip any NoneTypes
            if not value:
                continue

            # Coerce any dates or times to a standard format
            if "date" in key or "time" in key:
                value = self._valid_datetime(key, value)
                self.attrs[key] = value

            # Serialize any dictionaries
            if isinstance(value, dict):
                value = json.dumps(value)
                self.attrs[key] = value

            # Create class attribute from kwarg
            if not hasattr(self, key):
                setattr(self, key, value)

    @staticmethod
    def _valid_datetime(key, value):
        try:
            # Parse string for datetime object
            naive_dt = dateutil.parser.parse(value, fuzzy=True)
            # Make the datetime timezone-aware
            aware_dt = pytz.timezone("US/Eastern").localize(naive_dt)
            # Convert the timezone to UTC
            value = aware_dt.astimezone(pytz.utc)
        except (TypeError, dateutil.parser.ParserError):
            # String doesn't appear to be a date or time
            return value

        if "date" in key and "time" in key:
            return str(value.isoformat())
        if "date" in key:
            return str(value.date().isoformat())
        if "time" in key:
            return str(value.time().isoformat())
