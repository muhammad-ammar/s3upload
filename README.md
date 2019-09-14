S3Upload
--------
A simple python3 utility to upload files in parallel from your system to AWS S3 Bucket.


Installation
------------

```bash
virtualenv -p python3.6 s3upload-venv
source ./s3upload-venv/bin/activate

pip install boto3
```

Usage
-----

```python
from s3upload import S3
S3.upload_data_to_bucket('/Users/ma1/Documents/data/site', 'site.ma1.com')
```

