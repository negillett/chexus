import hashlib
import json
import os

import dateutil
import pytz


class BucketItem(object):
    """Represents an object in an AWS S3 bucket

    Args:
        file_path (str):
            The path to the file.

        file_name (str):
            The Name of the file.
            This attribute is set according to the file's path if a
            name is not provided.

        checksum (str):
            The checksum of the file.
            This attribute is set if a checksum is not provided and the
            file exists.

        key (str):
            The object key of the S3 file object.
            This attribute is set with the name attribute if no key is
            provided.
    """

    def __init__(self, file_path, file_name=None, checksum=None, key=None):
        self.path = file_path
        self.name = file_name or os.path.basename(self.path)
        self.checksum = checksum or self._generate_checksum()
        self.key = key or self.name

    def _generate_checksum(self):
        if os.path.isfile(self.path):
            sha256 = hashlib.sha256()
            with open(self.path, "rb") as binary:
                while True:
                    chunk = binary.read(4096)
                    if not chunk:
                        break
                    sha256.update(chunk)

            return sha256.hexdigest()

        return None


class TableItem(object):
    """Represents an item in an AWS DynamoDB table"""

    def __init__(self, **kwargs):
        self.attrs = kwargs

        for key, value in self.attrs.items():
            value = self._sanitize_value(key, value) if value else value

            self.attrs[key] = value

            if not hasattr(self, key):
                setattr(self, key, value)

    def _sanitize_value(self, key, value):
        # Coerce any dates, times to standard format
        value = self._valid_datetime(key, value)

        # Serialize any dictionaries
        if isinstance(value, dict):
            return json.dumps(value)

        return value

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

        if "date" in key and "time" not in key:
            return str(value.date().isoformat())

        if "time" in key and "date" not in key:
            return str(value.time().isoformat())

        return str(value.isoformat())
