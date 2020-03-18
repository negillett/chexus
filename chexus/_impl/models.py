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
        self.content_type = self._generate_content_type()

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

    def _generate_content_type(self):
        ext = os.path.splitext(self.path)[1]
        if ext == ".xml":
            return {"ContentType": "application/xml"}
        if ext == ".gz":
            return {"ContentType": "application/x-gzip"}
        if ext == ".bz2":
            return {"ContentType": "application/x-bzip"}

        return {}


class TableItem(object):
    """Represents an item in an AWS DynamoDB table.

    Args:
        kwargs
            Keyword arguments from which to create attributes.
            Dictionary values are converted to JSON strings.
            Values able to be parsed as dates and/or times are
            converted to UTC timezone, ISO format datetime strings.
    """

    def __init__(self, **kwargs):
        self.attrs = kwargs

        for key, value in self.attrs.items():
            value = self._sanitize_value(value) if value else value

            self.attrs[key] = value

            if not hasattr(self, key):
                setattr(self, key, value)

    def _sanitize_value(self, value):
        # Coerce any dates, times to standard format
        value = self._valid_datetime(value)

        # Serialize any dictionaries
        if isinstance(value, dict):
            return json.dumps(value)

        return value

    @staticmethod
    def _valid_datetime(value):
        try:
            # Parse string for datetime object
            naive_dt = dateutil.parser.parse(value)
            # Make datetime timezone-aware
            aware_dt = pytz.timezone("US/Eastern").localize(naive_dt)
            # Convert timezone to UTC
            value = aware_dt.astimezone(pytz.utc).replace(tzinfo=None)
            # Apply final formatting
            return value.replace(tzinfo=None).isoformat()
        except (TypeError, ValueError):
            # String doesn't appear to be a date or time
            return value
