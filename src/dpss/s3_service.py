import logging
import os
from typing import (
    List,
    Dict,
    Iterable,
    Any,
    Union,
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

    def sort_by_size(self, target: str, keys: Iterable[str]) -> List[str]:
        bucket = self.bucket_names[target]
        return sorted(keys, key=lambda k: self.client.head_object(Bucket=bucket, Key=k)['ContentLength'])

    def list_bucket(self, target: str, map_fields: Union[str, Iterable[str]]) -> Dict[str, Any]:
        response = self.client.list_objects_v2(Bucket=self.bucket_names[target],
                                               Prefix=self.key_prefixes[target])
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


s3service = S3Service()
