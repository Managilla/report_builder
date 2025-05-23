from pathlib import Path

from pydantic import BaseModel, field_validator

from .yaml_helper import yaml_client


class S3endpoint(BaseModel):
    endpoint_type_name: str
    endpoint_name: str
    bucket_name: str
    lockbox_secret_id: str

    @field_validator('endpoint_type_name')
    @classmethod
    def have_2b_s3(cls, v: str) -> str:
        if v != 'S3':
            raise ValueError
        return v


class Endpoint:
    config_map = {
        'S3': S3endpoint,
    }

    @classmethod
    def load(self, cfg_path: Path, endpoint_name: str):

        cfg_list = yaml_client.load(cfg_path)
        cnt = 0

        for cfg in cfg_list:
            if cfg.get('endpoint_name') == endpoint_name:
                try:
                    config_type = self.config_map.get(cfg.get('endpoint_type_name'))
                    if config_type:
                        return config_type(**cfg)
                        # return
                    else:
                        raise ValueError(
                            f'endpoint_type_name should be one of following: {list(self.config_map.values())}'
                        )
                except Exception as e:
                    print(f'Error occured during parsing endpoint config {cfg}')
                    cnt += 1
                    # raise e
        if cnt:
            print(f'There were {cnt} errors during loading config')
