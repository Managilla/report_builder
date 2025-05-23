import os
import re
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Optional, Protocol, runtime_checkable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *

from fintech_dwh_lib.conf.startrek import STARTREK_CERT_PATH, STARTREK_SECRET_ID, STARTREK_URL
from fintech_dwh_lib.greenplum import GreenplumAuthUtil
from fintech_dwh_lib.lockbox import Lockbox
from fintech_dwh_lib.notification.mail import DwhPostman
from fintech_dwh_lib.spark import S3SparkProcessor
from fintech_dwh_lib.utils import retry

import pendulum
from docxtpl import DocxTemplate
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel, Field, field_validator
from yandex_tracker_client import TrackerClient

from .context import ReportContext
from .events import Events
from .yaml_helper import yaml_client


class AttachText(str):
    pass


class AttachmentType(Enum):
    s3 = 's3'  # переосмыслить
    ticket = 'ticket'
    ticket_text = 'ticket_text'
    email = 'email'
    email_text = 'email_text'


class IndicatorType(Enum):
    SUCCESS = 'success'
    FAILURE = 'failure'
    NONE = 'none'


class Attachment(BaseModel):
    sql_condition: str
    file_template: Optional[str] = Field(default=None)
    sql_fill_template: Optional[str] = Field(default=None)
    add_text: Optional[str] = Field(default=None)


@runtime_checkable
class Report(Protocol):

    def run(self, context: ReportContext, events: Optional[Events] = None) -> None:
        ...


def get_sql(filename) -> str:
    with open(Path(filename)) as f:
        return f.read()


def get_jinja(filename, filler: Optional[Dict] = None) -> str:
    env = Environment(loader=FileSystemLoader(Path.cwd()))
    template = env.get_template(filename)
    return template.render(**filler or {})


def dates_dict(report: Report) -> str:
    frm = {}

    months = {
        'января': 'январь',
        'февраля': 'февраль',
        'марта': 'март',
        'апреля': 'апрель',
        'мая': 'май',
        'июня': 'июнь',
        'июля': 'июль',
        'августа': 'август',
        'сентября': 'сентябрь',
        'октября': 'октябрь',
        'ноября': 'ноябрь',
        'декабря': 'декабрь',
    }
    prev_mnth = pendulum.today().add(months=-1)
    temp_mnth_cyr = prev_mnth.format('MMMM YYYY', locale='ru')

    rep = dict((re.escape(k), v) for k, v in months.items())
    pattern = re.compile("|".join(rep.keys()))
    frm['PREV_CYR_MONTH'] = pattern.sub(lambda m: rep[re.escape(m.group(0))], temp_mnth_cyr)
    frm['PREV_CYR_MONTH_SLASH'] = prev_mnth.format('YYYY/MM', locale='ru')

    frm['CYR_DT'] = pendulum.today().add(days=report.DT).format('"D" MMMM YYYY', locale='ru')
    frm['CYR_DT_1ST'] = pendulum.today().add(days=report.DT).format('"01" MMMM YYYY', locale='ru')
    frm['DT'] = pendulum.today().add(days=report.DT).to_date_string()
    frm['DT_SLASH'] = pendulum.today().add(days=report.DT).format('YYYY/MM/DD')
    frm['DTTM'] = pendulum.now().strftime("%Y-%m-%d %H_%M_%S")
    return frm


def storage_options(context: ReportContext) -> Dict:
    return {
        "client_kwargs": {
            'endpoint_url': 'https://storage.yandexcloud.net',
            'aws_access_key_id': context.processor.s3_conf['fs.s3a.access.key'],
            'aws_secret_access_key': context.processor.s3_conf['fs.s3a.secret.key'],
            'region_name': 'ru-central1',
        }
    }


def str_format(string: str, report: Report, filler: Dict = None, replace_slash=None) -> str:
    frm = {}
    frm_dt = dates_dict(report)
    if filler:
        frm = {**frm, **filler}
    if replace_slash and frm:
        frm = {k: v.replace('/', replace_slash) for k, v in frm.items() if isinstance(v, str)}
    return string.format(**frm, **frm_dt)


class Indicator:
    path = 'users/indicator'

    def __init__(self, name: str):
        self.name = name

    def is_exist(self, context: ReportContext):
        return context.processor.s3bucket.is_object_exist(os.path.join(self.path, self.name))

    def create_indicator(self, context: ReportContext):
        context.processor.s3bucket.put_object(body=' ', key=os.path.join(self.path, self.name))


def s3a_path(context: ReportContext, report: Report, filename: str) -> str:
    if report.s3_bucket:
        return os.path.join(f's3a://{report.s3_bucket}', filename)
    else:
        return os.path.join(f's3a://{context.processor.bucket}', filename)


def spark_transform(report: Report, context: ReportContext) -> None:
    query = report.sql_query
    if report.sql_query.endswith('.sql'):
        query = get_sql(report.sql_query)
    query = str_format(query, report)
    df = context.processor.session.sql(query)
    df.createOrReplaceTempView('result')
    return df


def write_single_csv(
    proc: S3SparkProcessor,
    df: DataFrame,
    out_path: str,
    bucket: str = None,
    mode='overwrite',
    header: bool = True,
    empty_value: str = None
) -> None:
    if not bucket:
        bucket = proc.bucket
    tmp_path = os.path.join('data', out_path + '_' + str(pendulum.now()))
    backup_url = f'{proc.service_name}://{proc.bucket}/{tmp_path}'

    df.coalesce(1).write.csv(backup_url, header=header, mode=mode, emptyValue=empty_value)
    for obj in proc.s3bucket._bucket.objects.filter(Prefix=tmp_path):
        if '.csv' == obj.key[-4:]:
            break
    proc.s3bucket.resource.Object(bucket, out_path).copy_from(CopySource=f'{proc.bucket}/{obj.key}')

    if bucket == proc.bucket or bucket is None:
        for obj in proc.s3bucket._bucket.objects.filter(Prefix=tmp_path):
            obj.delete()


def send_email(report: Report, context: ReportContext, attachments: List[str]) -> None:

    context.logger.info('Started creating email')
    email_params = report.email

    body = str_format(report.email.pop('body'), report)

    email_params['subject'] = str_format(email_params['subject'], report)

    paths, text = [], []

    # забираем файлы с s3
    for attach in attachments:
        if isinstance(attach, AttachText):
            if AttachmentType.email_text in report.attach_type:
                text.append(attach)
        else:
            if AttachmentType.email in report.attach_type:
                filename = attach.split('/')[-1]
                context.processor.s3bucket.client.download_file(
                    report.s3_bucket or context.processor.bucket, attach, filename
                )
                paths.append(filename)

    email_params['body_html'] = DwhPostman.wrap_txt_as_html('<br/>'.join([body] + text))
    email_params['attachments_paths'] = paths

    context.logger.info(f'attachment_paths = {str(email_params["attachments_paths"])}')
    DwhPostman(cert_path=str(Path().cwd().resolve()), ).create_and_send_email(**email_params)


def get_values_for_template(context: ReportContext, attachment: Attachment) -> Dict:
    if not attachment.sql_fill_template:
        return {}
    df_for_template = context.processor.session.sql(attachment.sql_fill_template).limit(1)
    if df_for_template.count() == 1:
        [field_values] = [row.asDict() for row in df_for_template.collect()]
        return field_values
    else:
        return {col: None for col in df_for_template.columns}


class gp2s3Report(BaseModel):
    # TODO: объединить s3_path и filename_pattern по аналогии с filename_pattern в dwh2notification
    name: str
    report_type: str
    filename_pattern: str
    sql_query: str
    s3_path: str  # TODO засунуть в filename_pattern
    s3_bucket: Optional[str] = Field(default=None)
    DT: int = Field(default=0)

    @field_validator('report_type')
    @classmethod
    def have_2b_gp2s3(cls, v: str) -> str:
        if v != 'gp2s3':
            raise ValueError
        return v

    def run(self, context: ReportContext, events: Optional[Events] = None) -> None:
        df = self.get(context)
        self.put(context, df)

    @retry(exception=Exception, n_tries=3, delay=60)
    def get(self, context: ReportContext) -> DataFrame:
        context.logger.info(f'Started calculating SQL query for {self.name}')

        gp_auth_util = GreenplumAuthUtil()
        gp_options = gp_auth_util.build_credentials(
            secret_id=context.processor.gp_lockbox_secret_id,
            with_hikari_parameters=True,
        )

        spark = SparkSession.builder.getOrCreate()
        return spark.read.format("jdbc").options(**gp_options, query=self.sql_query).load()

    def put(self, context: ReportContext, df: DataFrame) -> None:

        filled_filename = os.path.join(self.s3_path, str_format(self.filename_pattern, self, replace_slash='_'))
        s3a_filled_filename = s3a_path(context, self, filled_filename)
        df.write.format("com.crealytics.spark.excel")\
            .option("header", "true")\
            .option("dateFormat", "yyyy-mm-dd")\
            .mode("overwrite")\
            .save(s3a_filled_filename)


class dwh2notificationReport(BaseModel):
    name: str
    report_type: str
    s3_sources: Dict[str, str] = Field(default_factory=dict)
    sql_query: str
    filename_pattern: str
    DT: int = Field(default=0)
    ticket: Optional[Dict] = Field(default=None)
    s3_bucket: Optional[str] = Field(default=None)
    indicator_name: Optional[str] = Field(default=None)
    indicator_type: Optional[IndicatorType] = Field(default='none')
    attach_type: Optional[List[AttachmentType]] = Field(default=['s3'])
    attachment: Optional[List[Attachment]] = Field(default=None)
    email_on_failture: Optional[Dict] = Field(default=None)
    email: Optional[Dict] = Field(default=None)

    @field_validator('report_type')
    @classmethod
    def have_2b_dwh2notification(cls, v: str) -> str:
        if v != 'dwh2notification':
            raise ValueError
        return v

    def run(self, context: ReportContext, events: Optional[Events] = None) -> None:
        self.get(context)
        df = spark_transform(self, context)
        df.persist()
        attachment_paths = self.process_attachments(context)
        if attachment_paths:
            if self.indicator_type == IndicatorType.FAILURE:
                events.add(result='FAILED')
            if self.indicator_type == IndicatorType.SUCCESS:
                events.add(result='SUCCESS')
            events.add(attachment_paths=[att for att in attachment_paths if not isinstance(att, AttachText)])

            if self.ticket:
                context.logger.info('Ticket will be created/updated')
                key = self.create_ticket(context, attachment_paths)
                events.add(ticket_key=key)
            if self.email:
                context.logger.info('Email will be sent')
                send_email(self, context, attachment_paths)
        else:
            if self.indicator_type == IndicatorType.FAILURE:
                events.add(result='SUCCESS')
            if self.indicator_type == IndicatorType.SUCCESS:
                events.add(result='FAILED')

            if self.email_on_failture:
                context.logger.info('Email on failure will be sent')
                self.send_email(context)
        df.unpersist()

    def get(self, context: ReportContext):
        for name, path in self.s3_sources.items():
            today = pendulum.today(tz='Europe/Moscow').to_date_string()
            formatted_path = path.format(today=today)
            if "hudi" in formatted_path.lower():
                context.processor.read_hudi(formatted_path).createOrReplaceTempView(name)
            else:
                context.processor.read(formatted_path, format='parquet').createOrReplaceTempView(name)

    def process_attachments(self, context: ReportContext) -> None:

        attachments_paths = []
        for attachment in self.attachment:
            df_condition = context.processor.session.sql(attachment.sql_condition)
            condition = df_condition.limit(1).collect()
            if len(condition) == 1 and condition[0][0]:
                context.logger.info(f'Condition {attachment.sql_condition} satisfied')
                filler = get_values_for_template(context, attachment)

                filled_filename = str_format(self.filename_pattern, self, filler=filler, replace_slash='_')
                s3a_filled_filename = s3a_path(context, self, filled_filename)

                context.logger.info(s3a_filled_filename)

                if filled_filename.endswith('.csv'):
                    df = context.processor.session.sql('select * from result')
                    write_single_csv(context.processor, df, filled_filename, self.s3_bucket)
                    attachments_paths.append(filled_filename)
                elif filled_filename.endswith('.docx'):
                    doc = DocxTemplate(attachment.file_template)
                    doc.render({**filler, **dates_dict(self)})
                    doc.save(filled_filename.split('/')[-1])
                    attachments_paths.append(filled_filename)

                    context.processor.s3bucket.client.upload_file(
                        Bucket=self.s3_bucket or context.processor.bucket,
                        Filename=filled_filename.split('/')[-1],
                        Key=filled_filename,
                    )
                elif filled_filename.endswith('.xlsx'):
                    df = context.processor.session.sql('select * from result')
                    df.write.format("com.crealytics.spark.excel")\
                        .option("header", "true")\
                        .option("dateFormat", "yyyy-mm-dd")\
                        .mode("overwrite")\
                        .save(s3a_filled_filename)

                    attachments_paths.append(filled_filename)

                else:
                    context.logger.info(
                        f'Аttachment "{filled_filename}" will be skipped because only ".csv", ".xlsx" and ".docx" formats availible'
                    )

                # Добавление опционального текста
                if attachment.add_text:
                    text = attachment.add_text
                    if text.endswith('.j2'):
                        text = get_jinja(text, filler)
                    else:
                        text = str_format(text, self, filler)
                    attachments_paths.append(AttachText(text))

                if self.indicator_type == IndicatorType.SUCCESS:
                    ind_name = str_format(self.indicator_name, self, filler=filler, replace_slash='_')
                    ind = Indicator(ind_name)
                    if ind.is_exist(context):
                        context.logger.info(
                            f'Indicator indicates that "{ind.name}" already exists so attachment "{filled_filename}" will be skipped'
                        )
                        continue
                    ind.create_indicator(context)
                    context.logger.info(
                        f'Indicator "{ind.name}" was created so attachment "{filled_filename}" will be attached'
                    )
                else:
                    context.logger.info(
                        f'Attachment {attachment} will be skipped ({condition = }, sql_condition = {attachment.sql_condition})'
                    )
            else:
                if self.indicator_type == IndicatorType.FAILURE:
                    ind_name = str_format(self.indicator_name, self).replace('/', '_')
                    ind = Indicator(ind_name)
                    if ind.is_exist(context):
                        context.logger.info(f'Indicator indicates that "{ind.name}" already exists')

                    ind.create_indicator(context)
                    context.logger.info(f'Indicator "{ind.name}" was created')
                else:
                    context.logger.info(
                        f'Attachment {attachment} will be skipped ({condition = }, sql_condition = {attachment.sql_condition})'
                    )
        return attachments_paths

    # вынести из класса
    def create_ticket(self, context: ReportContext, attachments: List[str]) -> None:

        startrek_secrets = Lockbox.get_secret_by_secret_id(STARTREK_SECRET_ID)
        client = TrackerClient(
            base_url=STARTREK_URL, token=startrek_secrets['token'], verify=os.path.split(STARTREK_CERT_PATH)[1]
        )

        ticket_params = self.ticket
        ticket_params['summary'] = str_format(ticket_params['summary'], self)
        ticket_params['description'] = (dedent(self.ticket.get('description')).strip() or '')

        paths, text = [], []

        # забираем файлы с s3
        for attach in attachments:
            if isinstance(attach, AttachText):
                if AttachmentType.ticket_text in self.attach_type:
                    text.append(attach)
            else:
                if AttachmentType.ticket in self.attach_type:
                    filename = attach.split('/')[-1]
                    context.processor.s3bucket.client.download_file(
                        self.s3_bucket or context.processor.bucket, attach, filename
                    )
                    paths.append(filename)
                else:
                    context.logger.info(f'Attachement {attach} wouldnt be addded to ticket')
        context.logger.info(f'Paths to attachemnts {paths}')
        ticket_params['description'] = '\n'.join([ticket_params['description']] + text)

        issues = client.issues.find(f"Queue: {ticket_params['queue']} summary: {ticket_params['summary']}")
        issues = [i for i in issues if i.summary == ticket_params['summary']]

        if len(issues) > 0:
            context.logger.info(str(issues[0]))
            ticket = issues[0]
            ticket.comments.create(text=f"{ticket_params['description']}", attachments=paths)
            return ticket.key
        else:
            context.logger.info(str(issues))
            ticket_params['deadline'] = pendulum.now().add(days=5).to_date_string()
            ticket = client.issues.create(**ticket_params)
            for p in paths:
                ticket.attachments.create(p)
            return ticket.key

    # заменить на функцию send_email (сделать параметр email/email_on_failture)
    def send_email(self, context: ReportContext) -> None:

        context.logger.info('Started creating email')
        email_params = self.email_on_failture

        email_params['body_html'] = DwhPostman.wrap_txt_as_html(self.email_on_failture.pop('body'))
        email_params['subject'] = str_format(email_params['subject'], self)

        DwhPostman(cert_path=str(Path().cwd().resolve()), ).create_and_send_email(**email_params)


class dwh2s3Report(BaseModel):
    name: str
    report_type: str
    s3_sources: Dict[str, str] = Field(default_factory=dict)
    sql_query: str
    s3_path: str
    DT: int = Field(default=0)

    @field_validator('report_type')
    @classmethod
    def have_2b_dwh2s3(cls, v: str) -> str:
        if v != 'dwh2s3':
            raise ValueError
        return v

    def run(self, context: ReportContext, events: Optional[Events] = None) -> None:
        self.get(context)
        df = spark_transform(self, context)
        self.put(context, df)

    def get(self, context: ReportContext):
        for name, path in self.s3_sources.items():
            today = pendulum.today(tz='Europe/Moscow').to_date_string()
            context.processor.read(path.format(today=today), format='parquet').createOrReplaceTempView(name)

    def put(self, context: ReportContext, df: DataFrame) -> None:
        context.processor.write(df, self.s3_path, mode='overwrite', repartition_num_partitions=5)


class ReportList:
    config_map = {
        'gp2s3': gp2s3Report,
        'dwh2s3': dwh2s3Report,
        'dwh2notification': dwh2notificationReport,
    }

    @classmethod
    def load(self, cfg_path: Path, reports: Optional[List[str]] = None):

        cfg_list = yaml_client.load(cfg_path)

        temp_lst = []
        cnt = 0

        for cfg in cfg_list:
            if reports is None or (cfg.get('name') in reports):
                try:
                    config_type = self.config_map.get(cfg.get('report_type'))
                    if config_type:
                        cfg_ = config_type(**cfg)
                        temp_lst.append(cfg_)
                    else:
                        raise ValueError(f'Report_type should be one of following: {list(self.config_map.values())}')
                except Exception as e:
                    print(f'Error {e} occured during parsing config {cfg}')
                    cnt += 1
                    # raise e
        if cnt:
            print(f'There were {cnt} errors during loading config')
        return temp_lst or []
