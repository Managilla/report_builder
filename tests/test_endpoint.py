from pathlib import Path

import pytest
from pydantic import ValidationError
from report_builder.src.endpoint import Endpoint, S3endpoint


class TestEndpoint:

    @pytest.mark.parametrize(
        'config_vars',
        [
            ({
                'endpoint_type_name': 'S3',
                'endpoint_name': 'a',
                'bucket_name': 'a',
                'lockbox_secret_id': 'a',
            }),
            (
                {
                    'endpoint_type_name': 'S3',
                    'endpoint_type_name2': 'S3',  # doesnt affect
                    'endpoint_name': 'a',
                    'bucket_name': 'a',
                    'lockbox_secret_id': 'a',
                }
            ),
        ]
    )
    def test_endpoint_config(self, config_vars):
        assert S3endpoint(**config_vars)

    @pytest.mark.parametrize(
        'config_vars', [
            ({
                'endpoint_type_name': 'S3',
                'endpoint_name': 'a',
                'bucket_name': 'a',
            }),
            ({
                'endpoint_type_name': 'SSSS',
                'endpoint_name': 'a',
                'bucket_name': 'a',
                'lockbox_secret_id': 'a',
            }),
        ]
    )
    def test_endpoint_config_err(self, config_vars):
        with pytest.raises(ValidationError):
            S3endpoint(**config_vars)

    @pytest.mark.parametrize('ep_name', [
        ('s3_default'),
        ('aaaa'),
    ])
    def test_load_config(self, ep_name):
        cfg_path = Path(__file__).resolve().parent.joinpath('configs').joinpath('endpoints.yaml')
        ep = Endpoint().load(cfg_path, ep_name)
        assert ep.endpoint_type_name == 'S3'
        assert ep.endpoint_name == ep_name
