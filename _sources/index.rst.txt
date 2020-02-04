chexus
======

A Python library for interacting with AWS S3 and DynamoDB

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   api-reference

Quick Start
-----------

Install chexus from PyPI:

::

    pip install chexus

In your Python code, construct a ``chexus.Client`` and call the desired
methods to perform actions on an AWS S3 bucket or a DynamoDB table.

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

    # ...and/or an item to publish to a DynamoDB table.
    put_item = TableItem(Name=upload_item.name, Checksum=upload_item.checksum)

    # Then perform these actions using the client.
    client.upload(items=upload_item, bucket_name="my-bucket")
    client.publish(items=put_item, table_name="my-table")