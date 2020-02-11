import unittest

from moto import (
    mock_s3,
)

from dpss.config import config
from src.dpss.s3_service import s3service


@mock_s3
class S3TestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.managed_buckets = {config.s3_matrix_bucket_name, config.s3_figure_bucket_name}
        for bucket in self.managed_buckets:
            s3service.client.create_bucket(Bucket=bucket)

    def tearDown(self) -> None:
        for bucket in self.managed_buckets:
            s3service.client.delete_objects(
                Bucket=bucket,
                Delete={
                    'Objects': [
                        {'Key': obj['Key']}
                        for obj
                        in s3service.client.list_objects_v2(Bucket=bucket).get('Contents', [])
                    ]
                }
            )
            s3service.client.delete_bucket(Bucket=bucket)


if __name__ == '__main__':
    unittest.main()
