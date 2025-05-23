from importlib import import_module
from pathlib import Path

if __name__ == '__main__':
    from fintech_dwh_lib.utils import inject_dependencies
    inject_dependencies(cur_dir=Path(__file__).parent)

    import findspark
    findspark.init()

    from fintech_dwh_lib.env import DWH_ENV_NAME, init_envvars

    cur_dir = Path(__file__).parent
    for file in cur_dir.glob('*.env'):
        print(f'Initialize envvars for environment {DWH_ENV_NAME}')
        init_envvars(file)
        job_name = file.stem.split(".")[0]

    print(f'{job_name = }')
    job_module = import_module(f'{job_name}')
    job_module.main(job_name)
