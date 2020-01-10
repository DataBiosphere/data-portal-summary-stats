import logging
import os
from typing import (
    List,
)

import boto3

from dpss.config import config
from dpss.matrix_info import MatrixInfo

log = logging.getLogger(__name__)


class S3Service:

    def __init__(self):
        log.info('Initializing S3 service...')
        self.client = boto3.client('s3')
        self.bucket_names = {
            'matrices': config.s3_matrix_bucket_name,
            'figures': config.s3_figure_bucket_name
        }
        self.key_prefixes = {
            'matrices': config.s3_canned_matrix_prefix,
            'figures': config.s3_figures_prefix
        }

    def list_bucket(self, target: str, sort: str = None) -> List[str]:
        response = self.client.list_objects_v2(Bucket=self.bucket_names[target],
                                               Prefix=self.key_prefixes[target])
        keys = [obj['Key'] for obj in response.get('Contents', [])]
        if sort is None:
            return keys
        elif sort == 'size':
            def sizer(key):
                return self.client.head_object(Bucket=self.bucket_names[target], Key=key)['ContentLength']
            keys.sort(key=sizer)
            return keys
        else:
            raise ValueError(f'Unknown sort: {sort}')

    def download(self, target: str, filename: str) -> None:
        self.client.download_file(Bucket=self.bucket_names[target],
                                  Key=self.key_prefixes[target] + filename,
                                  Filename=filename)

    def get_blacklist(self) -> List[str]:
        response = self.client.get_object(Bucket=self.bucket_names['matrices'], Key='blacklist')
        bytes_string = response['Body'].read()
        return bytes_string.decode().strip('\n').split('\n')

    def upload_figures(self, mtx_info: MatrixInfo) -> None:
        figures = os.listdir('figures/')
        bucket = self.bucket_names['figures']
        for figure in figures:
            key = f'{self.key_prefixes["figures"]}{mtx_info.figures_folder}{figure}'
            log.info(f'Uploading {figure} to S3 bucket {bucket} as {key}')
            self.client.upload_file(Filename=f'figures/{figure}',
                                    Bucket=bucket,
                                    Key=key,
                                    ExtraArgs={
                                        'ACL': 'public-read',
                                        'ContentDisposition': 'inline',
                                        'ContentType': 'image/png'
                                    })
