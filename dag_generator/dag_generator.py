import os
import shutil
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from fintech_dwh_lib.airflow_api.dwh_airflow_utils import DagConfig, get_dag_configs
from fintech_dwh_lib.env import DWH_ENV_NAME
from fintech_dwh_lib.greenplum.dwh_sqlalchemy import DwhGpSqlalchemyProcessor

from airflow.models.dagbag import DagBag
from jinja2 import Environment, FileSystemLoader
from ruamel.yaml import YAML


@dataclass
class TaskGroupItem:
    task_id: str
    endpoint_file: str
    config_file: str
    s3_endpoint: Optional[str]
    reports: Optional[str]
    spark_params: Dict[str, str]
    gp_sensors: Optional[Dict[str, str]]
    s3_sensors: Optional[List[str]]
    dataset_sensors: Optional[List[str]]
    templates: Optional[List[str]]
    sql: Optional[List[str]]
    main_args: str
    s3_indicator: Optional[str]

    def __init__(
        self,
        task_id: str,
        endpoint_file: str,
        config_file: str,
        s3_endpoint: Optional[str],
        reports: str = None,
        spark_params: Optional[Dict[str, str]] = None,
        gp_sensors: Optional[Dict[str, str]] = None,
        s3_sensors: Optional[List[str]] = None,
        dataset_sensors: Optional[List[str]] = None,
        templates: Optional[List[str]] = None,
        sql: Optional[List[str]] = None,
        s3_indicator: Optional[str] = '',
    ):
        self.task_id = task_id
        self.endpoint_file = endpoint_file
        self.config_file = config_file
        self.s3_endpoint = s3_endpoint
        self.reports = reports
        self.spark_params = spark_params or {}
        self.gp_sensors = gp_sensors
        self.s3_sensors = s3_sensors
        self.dataset_sensors = dataset_sensors
        self.templates = templates
        self.sql = sql
        self.main_args = self.build_main_args()
        self.s3_indicator = s3_indicator

    def build_main_args(self):
        args = []
        for arg_name in [
            'endpoint_file',
            'config_file',
            's3_endpoint',
            'reports',
        ]:
            if self.__getattribute__(arg_name) is not None:
                args.append(f'--{arg_name} {self.__getattribute__(arg_name)}')
        return ' '.join(args)


def dag_generate_by_configs(s3_bucket, dag_configs_dir, dag_generate_dir):
    print(f'{dag_configs_dir = }')
    print(f'{dag_generate_dir = }')

    last_error = None

    try:
        print(f'Generate dags from folder {dag_configs_dir}')
        generated_dag_files = dag_generate_by_config(s3_bucket, dag_configs_dir, dag_generate_dir)

    except Exception as ex:
        print(traceback.format_exc())
        last_error = ex

    if last_error:
        raise last_error
    delete_unused_dags(dag_generate_dir, generated_dag_files)

    dagbag = DagBag(
        dag_folder=dag_generate_dir, read_dags_from_db=False, safe_mode=True, include_examples=False, collect_dags=True
    )
    dagbag.sync_to_db()


class DagConfig:

    def __init__(self, dag_config) -> None:
        path = Path(dag_config)
        self.dag_config = dag_config
        self.dag_file = str(path.name)


def dag_generate_by_config(s3_bucket, s3_configs_path: str, dag_generate_dir: str):
    print(f'Creating directory if not exists {dag_generate_dir}')
    Path(dag_generate_dir).mkdir(parents=True, exist_ok=True)

    dag_names = set()
    s3_files = s3_bucket.find_folders_by_regexp(s3_configs_path, "(.*)yaml")
    print(s3_files)

    generated_dag_files = []
    dag_dict = {}

    for file in s3_files:
        if file.endswith('.yaml'):
            item = DagConfig(file)
            dag_dict[item.dag_file] = item

    for item in dag_dict.values():
        print(f'Start DAG generating for {item.dag_config}')
        dag_file_name = dag_generate_by_template(s3_bucket, item, dag_generate_dir, s3_configs_path)
        if dag_file_name in dag_names:
            print(f'Duplicate DAG name {dag_file_name} for {item}')
            continue

        dag_names.add(dag_file_name)
        dag_file_name = os.path.basename(dag_file_name)
        generated_dag_files.append(dag_file_name)

    return generated_dag_files


def delete_unused_dags(dag_generate_dir: str, generated_dag_files: list):
    print(*os.listdir(dag_generate_dir))
    for dag_file in os.listdir(dag_generate_dir):
        dag_file_full = os.path.join(dag_generate_dir, dag_file)
        if os.path.isfile(dag_file_full) and dag_file not in generated_dag_files:
            dag_file_path = os.path.join(dag_generate_dir, dag_file)
            print(f'Remove old dag file {dag_file_path} without .yaml descriptor')
            os.remove(dag_file_path)
    shutil.rmtree(os.path.join(dag_generate_dir, '__pycache__'), ignore_errors=True)


def fill_schedule(schedule):
    proc = DwhGpSqlalchemyProcessor()
    [workdays] = proc.execute_statement(
        '''
        select extract('day' from calendar_date)::int
        from dds_f.dict_calendar_cft
        where calendar_date>=current_date
            and holiday_flag=False
        order by calendar_date
        limit 1
        '''
    ).fetchone()

    return schedule.format(workdays=workdays)


def dag_generate_by_template(s3_bucket, item: DagConfig, dag_generate_dir: str, s3_configs_path: str) -> str:

    dag_config_body = s3_bucket.get_object_data(item.dag_config)
    dag_config_dict = YAML().load(dag_config_body)

    if dag_config_dict['dag_args'].get('schedule') and '{' in dag_config_dict['dag_args']['schedule']:
        dag_config_dict['dag_args']['schedule'] = fill_schedule(dag_config_dict['dag_args']['schedule'])

    dag_id = item.dag_file.replace('.yaml', '')
    task_groups = dag_config_dict.get('task_groups')

    task_list = []

    for task_group in task_groups:
        tg = TaskGroupItem(
            task_group['task_id'],
            task_group['endpoint_file'],
            task_group['config_file'],
            task_group.get('s3_endpoint'),
            task_group.get('reports'),
            task_group.get('spark_params'),
            task_group.get('gp_sensors'),
            task_group.get('s3_sensors'),
            task_group.get('dataset_sensors'),
            task_group.get('templates'),
            task_group.get('sql'),
            task_group.get('s3_indicator'),
        )

        task_list.append(tg)

    task_list.sort(key=lambda x: x.task_id, reverse=False)
    jinja_env = Environment(loader=FileSystemLoader(Path(__file__).parent))
    template = jinja_env.get_template('dag_template.jinja')

    dag_file_name = os.path.join(dag_generate_dir, f'{dag_id}.py')
    print(f'Creating file {dag_file_name}')

    with open(dag_file_name, 'w') as file:
        context = {
            'DAG_ID': dag_id,
            'DAG_CONFIG_DICT': dag_config_dict,
            'TASK_LIST': task_list,
            'CONFIG_PATH': f's3a://dwh-analytics-s3-prod/spark-jobs/sl_loader/configs/{DWH_ENV_NAME}/'
        }
        file.write(template.render(context))

    return dag_file_name
