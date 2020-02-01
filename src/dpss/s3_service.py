import logging
from typing import (
    List,
    Dict,
    Iterable,
    Union,
)

import boto3

from dpss.config import config
from dpss.matrix_summary_stats import MatrixSummaryStats
from dpss.utils import setup_log

log = logging.getLogger(__name__)
setup_log(__name__, logging.INFO, logging.StreamHandler())


class S3Service:
    bucket_names = {
        'matrices': config.s3_matrix_bucket_name,
        'figures': config.s3_figure_bucket_name
    }

    key_prefixes = {
        'matrices': config.s3_canned_matrix_prefix,
        'figures': config.s3_figures_prefix
    }

    def __init__(self):
        # client initialization is postponed until the client is used so that
        # the client will initialized under mock conditions during unit testing.
        # if a client is instantiated outside of a mock environment, it will
        # interfere with mocking even if another client is created and used
        # later independently.
        self._client = None

    @property
    def client(self):
        if self._client is None:
            log.info('Initializing S3 service...')
            self._client = boto3.client('s3')
        return self._client

    def sort_by_size(self, target: str, keys: Iterable[str]) -> List[str]:
        """
        Order object keys by the size of their respective objects in the specified bucket.
        """
        bucket = self.bucket_names[target]
        return sorted(
            keys,
            key=lambda k: self.client.head_object(Bucket=bucket, Key=k)['ContentLength']
        )

    def list_bucket(
        self,
        target: str,
        map_fields: Union[None, str, Iterable[str]] = None
    ) -> Union[Iterable[str], Dict[str, str], Dict[str, Dict[str, str]]]:
        """
        List the keys in a bucket, with additional descriptive fields.
        If `map_fields` is None, only returns keys. If `map_fields` is a str,
        return a dict mapping keys to the provided AWS field (e.g. LastModified).
        If `map_fields` is an iterable of strs, return a dict mapping keys to
        dicts that map the specified fields to their values.
        """
        response = self.client.list_objects_v2(
            Bucket=self.bucket_names[target],
            Prefix=self.key_prefixes[target]
        )
        contents = response.get('Contents', [])
        if map_fields is None:
            return [obj['Key'] for obj in contents]
        else:
            return {
                obj['Key']: (
                    obj[map_fields]
                    if isinstance(map_fields, str) else
                    {field: obj[field] for field in map_fields}
                )
                for obj
                in response.get('Contents', [])
            }

    def download(self, target: str, filename: str) -> None:
        """
        Download a file. Saves the bother of typing out full strings.
        """
        self.client.download_file(
            Bucket=self.bucket_names[target],
            Key=self.key_prefixes[target] + filename,
            Filename=filename
        )

    def get_blacklist(self) -> List[str]:
        """
        Read blacklisted uuids from S3.
        """
        response = self.client.get_object(Bucket=self.bucket_names['matrices'], Key='blacklist')
        bytes_string = response['Body'].read()
        return bytes_string.decode().strip('\n').split('\n')

    def upload_figure(self, folder: str, figure: str) -> None:
        """
        Upload figures generated from the downloaded matrix.
        """
        figure = f'{figure}.{MatrixSummaryStats.figure_format}'
        bucket = self.bucket_names['figures']
        key = f'{self.key_prefixes["figures"]}{folder}{figure}'
        self.client.upload_file(
            Filename=f'figures/{figure}',
            Bucket=bucket,
            Key=key,
            ExtraArgs={
                'ACL': 'public-read',
                'ContentDisposition': 'inline',
                'ContentType': f'image/{MatrixSummaryStats.figure_format}'
            }
        )
        log.info(f'Uploading {figure} to S3 bucket {bucket} as {key}')


s3service = S3Service()
