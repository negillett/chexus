chexus
================

A Python client for [boto3](https://aws.amazon.com/sdk-for-python/).

[![PyPI version](https://badge.fury.io/py/chexus.svg)](https://badge.fury.io/py/chexus)
[![Build Status](https://travis-ci.org/nathanegillett/chexus.svg?branch=master)](https://travis-ci.org/nathanegillett/chexus)
[![Coverage Status](https://coveralls.io/repos/github/nathanegillett/chexus/badge.svg?branch=master)](https://coveralls.io/github/nathanegillett/chexus?branch=master)

- [Source](https://github.com/nathanegillett/chexus)
- [Documentation](https://nathanegillett.github.io/chexus)
- [PyPI](https://pypi.org/project/chexus)

Installation
------------

Install the chexus package from PyPI.

```
pip install chexus
```


Usage Example
-------------

```python
from chexus import Client, BucketItem, TableItem

# Make a client pointing at an AWS account using IAM credentials.
client = Client(
    access_id="AKIAUB24K6XOZITDKHY",
    access_key="iJg8jG0DLIrzVpQs4Sj5LerxPtVyY4QG7sYv8bk",
    default_region="us-west-1"
)

# Create an item to upload to an S3 bucket...
upload_item = BucketItem(file_path="mnt/my/os-3/new-file")

# ...and/or an item to put into a DynamoDB table.
put_item = TableItem(Name=upload_item.name, Checksum=upload_item.checksum)

# Perform the actions using the client.
client.upload_files(items=upload_item, bucket_name="my-bucket")
client.put_items(items=put_item, table_name="my-table")
```

Development
-----------

Patches may be contributed via pull requests to
https://github.com/nathanegillett/chexus.

All changes must pass the automated test suite, along with various static
checks.

The [Black](https://black.readthedocs.io/) code style is enforced.
Enabling autoformatting via a pre-commit hook is recommended:

```
pip install -r requirements-dev.txt
pre-commit install
```

License
-------

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.