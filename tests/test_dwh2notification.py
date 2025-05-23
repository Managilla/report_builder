import logging
from pathlib import Path

import pendulum
import pytest
from pydantic import ValidationError
from report_builder.src.context import ReportContext
from report_builder.src.endpoint import S3endpoint
from report_builder.src.events import Events
from report_builder.src.pipeline import Report, ReportList, dwh2notificationReport, spark_transform

import pyspark
from pyspark.sql import DataFrame, Row, SparkSession

from fintech_dwh_lib import conf
from fintech_dwh_lib.logging import getLogger
from fintech_dwh_lib.testing_utils import TestingUtil

logger = getLogger('test_report_builder').setLevel(logging.INFO)


class TestReportList:

    @pytest.mark.parametrize(
        'reports, expected', [
            (None, 2),
            ([], 0),
            (['ps_mir2'], 1),
            (['ps_mir', 'ps_mir2'], 2),
            (['ps_mir', 'ps_mir3'], 1),
        ]
    )
    def test_init(self, reports, expected):

        cfg_path = Path(__file__).resolve().parent.joinpath('configs').joinpath('dwh2notification.yaml')
        print(f'{cfg_path = }')
        rep_list = ReportList().load(cfg_path, reports=reports)
        assert type(rep_list) == list
        assert len(rep_list) == expected
        for rep in rep_list:
            assert type(rep) == dwh2notificationReport
            assert isinstance(rep, Report)


class Testdwh2notificationReport:

    @pytest.mark.parametrize(
        'config_vars', [
            (
                {
                    'name': 'a',
                    'report_type': 'dwh2notification',
                    'sql_query': 'a',
                    'ticket': dict(a='a'),
                    'attach_result': ['s3'],
                    'filename_pattern': 'a',
                }
            ),
            (
                {
                    'name': 'a',
                    'report_type': 'dwh2notification',
                    'sql_query': 'a',
                    'indicator_name': 'a',
                    'ticket': dict(a='a'),
                    'attach_result': ['s3'],
                    'filename_pattern': 'a',
                }
            ),
            (
                {
                    'name': 'a',
                    'report_type': 'dwh2notification',
                    'sql_query': 'a',
                    'ticket': dict(a='a'),
                    'filename_pattern': 'a',
                }
            ),
            (
                {
                    'name': 'a',
                    'report_type': 'dwh2notification',
                    's3_sources': dict(a='a'),
                    'sql_query': 'a',
                    'ticket': dict(a='a'),
                    'attach_result': ['s3'],
                    'filename_pattern': 'a',
                }
            ),
            (
                {
                    'name': 'a',
                    'report_type': 'dwh2notification',
                    's3_sources': dict(a='a'),
                    'sql_query': 'a',
                    'ticket': dict(a='a'),
                    'attach_result': ['s3'],
                    'filename_pattern': 'a',
                    'email_on_failture': dict(a='a')
                }
            ),
        ]
    )
    def test_pipeline(self, config_vars):
        assert dwh2notificationReport(**config_vars)

    @pytest.mark.parametrize(
        'config_vars', [
            (
                {
                    'name': 'a',
                    'report_type': 'dwh22notification',
                    'sql_query': 'a',
                    'ticket': dict(a='a'),
                    'filename_pattern': 'a',
                }
            ),
            ({
                'name': 'a',
                'report_type': 'dwh2notification',
                'sql_query': 'a',
                'ticket': dict(a='a'),
            }),
            ({
                'name': 'a',
                'report_type': 'dwh2notification',
                'ticket': dict(a='a'),
                'filename_pattern': 'a',
            }),
        ]
    )
    def test_pipeline_err(self, config_vars):
        with pytest.raises(ValidationError):
            dwh2notificationReport(**config_vars)


class TestMainParts:

    def setup_method(self):
        self.test_util = TestingUtil()
        self.s3_source_path = self.test_util.s3_tmp_path('source_path', service='spark_jobs', include_test_case=False)

        self.job_name = 'report_builder_test'

        self.s3buck = S3endpoint(
            endpoint_type_name='S3',
            endpoint_name='some_name',
            bucket_name=conf.s3.S3_BUCKET,
            lockbox_secret_id=conf.s3.S3_SECRET_ID
        )

        self.config_file = Path(__file__).resolve().parent.joinpath('configs').joinpath('dwh2notification.yaml')
        self.reports = ['ps_mir', 'ps_mir2']

        self.report_list = ReportList().load(
            self.config_file,
            self.reports,
        )

        for report in self.report_list:
            for name in report.s3_sources.keys():
                report.s3_sources[name] = self.s3_source_path

        self.context = ReportContext(
            self.job_name,
            self.s3buck,
        )

        self.context.processor.logger.info(f'{self.context.processor.session.version = }')
        self.context.processor.logger.info(f'{pyspark.__version__ = }')
        self.context.processor.logger.info(
            f'scala {self.context.processor.session.sparkContext._gateway.jvm.scala.util.Properties.versionString()}'
        )

        self.upload_test_data()

    def teardown_method(self):
        ss = SparkSession.builder.getOrCreate()
        ss.stop()
        self.context.processor.s3bucket.delete_folder(self.s3_source_path)

    def upload_test_data(self):
        print(self.s3_source_path)
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

    def test_get(self):
        for report in self.report_list:
            report.get(self.context)

            for name in report.s3_sources.keys():
                assert self.context.processor.session.sql(f'select count(*) from {name}').collect()[0][0] == 2

    def test_process_attachments(self):

        # test csv format
        report = self.report_list[0]
        report.get(self.context)
        spark_transform(report, self.context)
        attachment_paths = report.process_attachments(self.context)

        assert len(attachment_paths) == 1
        assert attachment_paths == ['test/test/Сверка1 2024-12-11.csv']

        # test xlsx format
        report = self.report_list[0]
        report.filename_pattern = report.filename_pattern.replace('.csv', '.xlsx')
        report.get(self.context)
        spark_transform(report, self.context)
        attachment_paths = report.process_attachments(self.context)

        assert len(attachment_paths) == 1
        assert attachment_paths == ['test/test/Сверка1 2024-12-11.xlsx']

    @pytest.mark.skipif(conf.startrek.STARTREK_SECRET_ID is None, reason="no way of currently testing this")
    def test_create_ticket(self):
        for report in self.report_list:
            report.get(self.context, self.context)
            df = report.transform(self.context)
            report.create_ticket(self.context, df)

    @pytest.mark.skipif(conf.mail.SMTP_SECRET_ID is None, reason="no way of currently testing this")
    def test_send_email(self):
        for report in self.report_list:
            report.get(self.context)
            _ = report.transform(self.context)
            report.send_email(self.context)
