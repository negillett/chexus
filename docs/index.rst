chexus
======

A Python client for `boto3 <https://aws.amazon.com/sdk-for-python/>`.

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