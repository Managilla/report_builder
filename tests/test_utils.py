import os
from pathlib import Path

import pendulum
import pytest
from jinja2.exceptions import UndefinedError
from report_builder.src.context import ReportContext
from report_builder.src.endpoint import S3endpoint
from report_builder.src.pipeline import (
    Attachment,
    AttachmentType,
    AttachText,
    Indicator,
    IndicatorType,
    ReportList,
    get_jinja,
    get_sql,
    s3a_path,
    spark_transform,
    str_format,
    write_single_csv,
)

import pyspark
from pyspark.sql import Row, SparkSession

from fintech_dwh_lib import conf
from fintech_dwh_lib.testing_utils import TestingUtil


class TestCommon:

    def test_AttachText(self):
        at = AttachText('aaa')

        assert isinstance(at, AttachText)
        assert isinstance(at, str)

        assert isinstance('aaa', str)
        assert not isinstance('aaa', AttachText)

    def test_AttachmentType(self):
        assert len(list(AttachmentType)) == 5
        assert {val.value for val in set(AttachmentType)} == set(['s3', 'ticket', 'ticket_text', 'email', 'email_text'])

    def test_IndicatorType(self):
        assert len(list(IndicatorType)) == 3
        assert {val.value for val in set(IndicatorType)} == set(['success', 'failure', 'none'])

    @pytest.mark.parametrize(
        'config_vars', [
            (dict(
                file_template='a',
                add_text='a',
                sql_fill_template='a',
                sql_condition='a',
            )),
            (dict(
                file_template='a',
                sql_fill_template='a',
                sql_condition='a',
            )),
            (dict(
                add_text='a',
                sql_fill_template='a',
                sql_condition='a',
            )),
            (dict(
                file_template='a',
                add_text='a',
                sql_condition='a',
            )),
            (dict(sql_condition='a', )),
        ]
    )
    def test_Attachment(self, config_vars):
        Attachment(**config_vars)

    def test_get_jinja(self, monkeypatch):
        path = Path(__file__).resolve().parent.joinpath('templates')
        monkeypatch.setattr(Path, 'cwd', lambda: path)

        fill = dict(status='aaa', difference_payments_amount=21.1234, aaa='aaa')

        assert get_jinja('test.j2') == 'Сверка данных по эквайрингу'
        assert get_jinja('test.j2', fill) == 'Сверка данных по эквайрингу'

        with pytest.raises(UndefinedError):
            get_jinja('test2.j2')
        assert get_jinja(
            'test2.j2', fill
        ) == '<p>Сверка данных по эквайрингу <strong>aaa</strong>.</p>\n<p><br></p>\n<p>Дельта по оплатам: 21.12 рублей.</p>'


class TestGetSql:

    def test_get_sql(self):
        path = Path(__file__).resolve().parent.joinpath('sql').joinpath('test.sql')
        assert get_sql(path) == 'select 1 as one, 2 as two, 3 as three'


class TestIndicatorType:

    @pytest.mark.parametrize(
        'type, expected', [
            ('success', IndicatorType.SUCCESS),
            ('failure', IndicatorType.FAILURE),
            ('none', IndicatorType.NONE),
        ]
    )
    def test_IndicatorType(self, type, expected):
        assert IndicatorType(type) == expected

    @pytest.mark.parametrize('type', [
        (None),
        ('aaa'),
    ])
    def test_IndicatorType_err(self, type):
        with pytest.raises(ValueError):
            IndicatorType(type)


class TestIndicator:

    def setup_method(self):
        self.test_util = TestingUtil()
        self.s3_indicator_path = self.test_util.s3_tmp_path(
            'indicator_path', service='spark_jobs', include_test_case=False
        )

        self.job_name = 'report_builder_test'

        self.s3buck = S3endpoint(
            endpoint_type_name='S3',
            endpoint_name='some_name',
            bucket_name=conf.s3.S3_BUCKET,
            lockbox_secret_id=conf.s3.S3_SECRET_ID
        )

        self.config_file = Path(__file__).resolve().parent.joinpath('configs').joinpath('dwh2notification.yaml')
        self.reports = [
            'ps_mir',
        ]

        self.report_list = ReportList().load(
            self.config_file,
            self.reports,
        )

        self.context = ReportContext(
            self.job_name,
            self.s3buck,
        )

        self.context.processor.logger.info(f'{self.context.processor.session.version = }')
        self.context.processor.logger.info(f'{pyspark.__version__ = }')
        self.context.processor.logger.info(
            f'scala {self.context.processor.session.sparkContext._gateway.jvm.scala.util.Properties.versionString()}'
        )

    def teardown_method(self):
        ss = SparkSession.builder.getOrCreate()
        ss.stop()
        self.context.processor.s3bucket.delete_folder(self.s3_indicator_path)

    def test_indicator(self, monkeypatch):
        monkeypatch.setattr(Indicator, 'path', self.s3_indicator_path)
        report = self.report_list[0]
        ind_name = 'aaa/{DT}'
        ind = Indicator(str_format(ind_name, report))
        assert ind.is_exist(self.context) == False
        ind.create_indicator(self.context)
        assert ind.is_exist(self.context) == True


class TestStrFormat:

    def setup_method(self):
        self.s3buck = S3endpoint(
            endpoint_type_name='S3',
            endpoint_name='some_name',
            bucket_name=conf.s3.S3_BUCKET,
            lockbox_secret_id=conf.s3.S3_SECRET_ID
        )

        self.config_file = Path(__file__).resolve().parent.joinpath('configs').joinpath('dwh2notification.yaml')
        self.reports = [
            'ps_mir',
        ]

        self.report_list = ReportList().load(
            self.config_file,
            self.reports,
        )

    @pytest.fixture
    def mock(self, monkeypatch):
        monkeypatch.setattr(pendulum, 'now', self.now_mock)

    def now_mock(self, *args, **kwargs):
        return pendulum.datetime(year=2024, month=10, day=3, hour=22, minute=32, second=5)

    @pytest.mark.parametrize(
        'string, filler, replace_slash, expected', [
            ('{DT}', None, None, '2024-10-02'),
            ('{PREV_CYR_MONTH}', None, None, 'сентябрь 2024'),
            ('{CYR_DT}', None, None, '"2" октября 2024'),
            ('{CYR_DT_1ST}', None, None, '"1" октября 2024'),
            ('{DT}{CYR_DT}', None, None, '2024-10-02"2" октября 2024'),
            ('{DTTM}', None, None, '2024-10-03 22_32_05'),
            ('{test}', {
                'test': 'aaa/aaa'
            }, None, 'aaa/aaa'),
            ('{test}', {
                'test': 'aaa/aaa'
            }, '-', 'aaa-aaa'),
        ]
    )
    def test_str_format(self, mock, string, filler, replace_slash, expected):
        report = self.report_list[0]
        assert str_format(string, report, filler=filler, replace_slash=replace_slash) == expected

    @pytest.mark.parametrize(
        'string, filler, expected', [
            ('{DT} {a}', dict(a='1'), '2024-10-02 1'),
            ('{DT} {a}', dict(a='1', b='2'), '2024-10-02 1'),
            ('{a}', dict(a='1', b='2'), '1'),
            ('{a}', dict(a=1), "1"),
        ]
    )
    def test_str_format_fill(self, mock, string, filler, expected):
        report = self.report_list[0]
        assert str_format(string, report, filler) == expected

    @pytest.mark.parametrize('string', [
        ('{aaa}'),
        ('{DT}{aaa}'),
    ])
    def test_str_format_err(
        self,
        mock,
        string,
    ):
        with pytest.raises(KeyError):
            report = self.report_list[0]
            str_format(string, report)


class TestWithContext:

    def setup_method(self):
        self.s3buck = S3endpoint(
            endpoint_type_name='S3',
            endpoint_name='some_name',
            bucket_name=conf.s3.S3_BUCKET,
            lockbox_secret_id=conf.s3.S3_SECRET_ID
        )
        self.test_util = TestingUtil()
        self.s3_source_path = self.test_util.s3_tmp_path('source_path', service='spark_jobs', include_test_case=False)
        self.path_csv = self.test_util.s3_tmp_path('path', service='spark_jobs', include_test_case=False)

        self.config_file = Path(__file__).resolve().parent.joinpath('configs').joinpath('dwh2notification.yaml')

        self.reports = [
            'ps_mir',
        ]

        self.report_list = ReportList().load(
            self.config_file,
            self.reports,
        )

        self.context = ReportContext(
            'test_job',
            self.s3buck,
        )

        self.test_df = self.upload_test_data()

        for report in self.report_list:
            for name in report.s3_sources.keys():
                report.s3_sources[name] = self.s3_source_path

        print([report.s3_sources for report in self.report_list])

    def teardown_method(self):
        ss = SparkSession.builder.getOrCreate()
        ss.stop()
        self.context.processor.s3bucket.delete_folder(self.s3_source_path)
        self.context.processor.s3bucket.delete_folder(self.path_csv)

    def upload_test_data(self):
        rows = [
            Row(
                col_a=1,
                col_b=2,
            ),
            Row(
                col_a=3,
                col_b=4,
            ),
        ]
        df = self.context.processor.session.createDataFrame(rows)
        self.context.processor.write(df, self.s3_source_path, format='parquet')
        return df

    @pytest.mark.parametrize(
        'string, bucket, expected', [
            ('aaa/aaa/aaa', None, 's3a://dwh-test/aaa/aaa/aaa'),
            ('aaa/aaa/aaa', 'dwh-test', 's3a://dwh-test/aaa/aaa/aaa'),
            ('aaa/aaa/aaa', 'aaa', 's3a://aaa/aaa/aaa/aaa'),
        ]
    )
    def test_s3a_path(self, string, bucket, expected):
        self.report_list[0].s3_bucket = bucket
        assert s3a_path(self.context, self.report_list[0], string) == expected

    def test_transform(self):
        report = self.report_list[0]
        for name, path in report.s3_sources.items():
            self.context.processor.read(path, format='parquet').createOrReplaceTempView(name)
        df = spark_transform(report, self.context)
        df.show()

        assert df.collect() == [Row(cnt=2)]

    def test_write_single_csv(self):
        file_path = os.path.join(self.path_csv, 'filename.csv')
        assert self.context.processor.s3bucket.is_object_exist(file_path) == False
        write_single_csv(self.context.processor, self.test_df, file_path)
        assert self.context.processor.s3bucket.is_object_exist(file_path) == True

        df = self.context.processor.read(file_path, format='csv')

        assert df.collect() == [Row(_c0='col_a', _c1='col_b'), Row(_c0='1', _c1='2'), Row(_c0='3', _c1='4')]
        assert df.count() == 3
