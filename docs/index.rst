chexus
======

A library for interacting with AWS S3 and DynamoDB.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   api-reference

Quick Start
-----------

Install chexus from PyPI:

::

    pip install chexus

In your Python code, construct a ``chexus.Client`` and call
the desired methods to perform actions on AWS S3 or DynamoDB.

.. code-block:: python

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