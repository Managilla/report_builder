import os
import subprocess
import sys
from pathlib import Path

fintech_dwh_lib_base_dir = Path(__file__).parents[3].joinpath('lib')
sys.path.insert(0, fintech_dwh_lib_base_dir.resolve().as_posix())

folder_dir = Path(__file__).parents[1]
sys.path.insert(0, folder_dir.resolve().as_posix())

os.environ.setdefault('DWH_ENV_NAME', 'playground')
from fintech_dwh_lib.conf import s3
from fintech_dwh_lib.lockbox import Lockbox

# get and set env vars before running tests
if not os.environ.get('IAM_TOKEN'):
    result = subprocess.run('yc iam create-token', capture_output=True, check=True, shell=True)
    os.environ.setdefault('IAM_TOKEN', result.stdout.decode('utf-8').strip())
if not os.environ.get('AWS_ACCESS_KEY_ID') or not os.environ.get('AWS_SECRET_ACCESS_KEY'):
    s3_secret = Lockbox.get_secret_by_id_or_raise(secret_id=s3.S3_SECRET_ID, exception_class=Exception)
    os.environ.setdefault('AWS_ACCESS_KEY_ID', s3_secret['aws_access_key_id'])
    os.environ.setdefault('AWS_SECRET_ACCESS_KEY', s3_secret['aws_secret_access_key'])
