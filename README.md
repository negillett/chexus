chexus
================

A Python client for [boto3](https://aws.amazon.com/sdk-for-python/), used by
[release-engineering](https://github.com/release-engineering)'s publishing tools.

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
from chexus import Client, PushItem

# Make a client pointing at an AWS account using IAM credentials.
client = Client(
    access_id="AKIAUB24K6XOZITDKHY",
    access_key="iJg8jG0DLIrzVpQs4Sj5LerxPtVyY4QG7sYv8bk",
    default_region="us-west-1"
)

# Create a push item using information about the content.
item = PushItem(
    file_path="mnt/co/os-3/new-file",
    web_uri="www.example.com/content/os-3/new-file",
    metadata={"build": 1234, "owner": "me"},
    from_date="Nov. 19, 2022"
)

# Push the item, creating an object in an S3 bucket
# and a related record in a DynamoDB table.
client.push(
    items=item,
    bucket_name="co-bucket",
    table_name="co-table",    
)

# Now, a service such as AWS Lambda can make the file available at the
# provided URI by pairing the table record's "object_key" attribute
# with the key of the object in the bucket.
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