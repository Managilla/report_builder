import logging
import os
import sys
from pathlib import Path

import pendulum
import pytest
from pydantic import ValidationError
from report_builder import processing  # import main, parser
from report_builder.src.context import ReportContext
from report_builder.src.endpoint import S3endpoint
from report_builder.src.pipeline import Report, ReportList, gp2s3Report

import pyspark
from pyspark.sql import DataFrame, SparkSession

from fintech_dwh_lib import conf
from fintech_dwh_lib.logging import getLogger
from fintech_dwh_lib.testing_utils import TestingUtil

logger = getLogger('test_report_builder').setLevel(logging.INFO)


class TestReportList:

    @pytest.mark.parametrize(
        'reports, expected', [
            (None, 2),
            ([], 0),
            (['solar_mistakes_report'], 1),
            (['solar_mistakes_report', 'solar_mistakes_report2'], 2),
            (['solar_mistakes_report', 'solar_mistakes_report10'], 1),
        ]
    )
    def test_init(self, reports, expected):

        cfg_path = Path(__file__).resolve().parent.joinpath('configs').joinpath('gp2s3.yaml')
        rep_list = ReportList().load(cfg_path, reports=reports)
        assert type(rep_list) == list
        assert len(rep_list) == expected
        for rep in rep_list:
            assert type(rep) == gp2s3Report
            assert isinstance(rep, Report)


class Testgp2s3Report:

    @pytest.mark.parametrize(
        'config_vars',
        [
            ({
                'name': 'a',
                'report_type': 'gp2s3',
                'filename_pattern': 'a',
                'sql_query': 'a',
                's3_path': 'a',
            }),
            (
                {
                    'name': 'a',
                    'report_type': 'gp2s3',
                    'filename_pattern': 'a',
                    'sql_query': 'a',
                    's3_path': 'a',
                    's3_bucket': 'a',
                }
            ),
            (
                {
                    'name': 'a',
                    'name2': 'a',  # doesnt affect
                    'report_type': 'gp2s3',
                    'filename_pattern': 'a',
                    'sql_query': 'a',
                    's3_path': 'a',
                }
            ),
        ]
    )
    def test_pipeline(self, config_vars):
        assert gp2s3Report(**config_vars)

    @pytest.mark.parametrize(
        'config_vars', [
            ({
                'name': 'a',
                'report_type': 'gp2s3',
                'filename_pattern': 'a',
                'sql_query': 'a',
            }),
            ({
                'name': 'a',
                'report_type': 'a',
                'filename_pattern': 'a',
                'sql_query': 'a',
                's3_path': 'a',
            }),
        ]
    )
    def test_pipeline_err(self, config_vars):
        with pytest.raises(ValidationError):
            gp2s3Report(**config_vars)


class TestContext:

    def setup_method(self):
        self.job_name = 'report_builder_test'
        self.s3buck = S3endpoint(
            endpoint_type_name='S3',
            endpoint_name='some_name',
            bucket_name=conf.s3.S3_BUCKET,
            lockbox_secret_id=conf.s3.S3_SECRET_ID
        )

        self.config_file = Path(__file__).resolve().parent.joinpath('configs').joinpath('gp2s3.yaml')

    def teardown_method(self):

        ss = SparkSession.builder.getOrCreate()
        ss.stop()

    @pytest.mark.parametrize(
        'reports, expected', [
            (None, 2),
            ([], 0),
            (['solar_mistakes_report'], 1),
            (['solar_mistakes_report', 'solar_mistakes_report2'], 2),
        ]
    )
    def test_context(self, reports, expected):
        report_list = ReportList().load(
            self.config_file,
            reports,
        )

        assert len(report_list) == expected

        context = ReportContext(
            self.job_name,
            self.s3buck,
        )

        assert context.job_name == self.job_name


class TestMainParts:

    def setup_method(self):
        self.test_util = TestingUtil()
        self.s3_path = self.test_util.s3_tmp_path('test', service='spark_jobs', include_test_case=False)

        self.job_name = 'report_builder_test'

        self.s3buck = S3endpoint(
            endpoint_type_name='S3',
            endpoint_name='some_name',
            bucket_name=conf.s3.S3_BUCKET,
            lockbox_secret_id=conf.s3.S3_SECRET_ID
        )

        self.config_file = Path(__file__).resolve().parent.joinpath('configs').joinpath('gp2s3.yaml')
        self.reports = ['solar_mistakes_report', 'solar_mistakes_report2']

        self.report_list = ReportList().load(
            self.config_file,
            self.reports,
        )

        for report in self.report_list:
            report.s3_path = self.s3_path

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
        self.context.processor.s3bucket.delete_folder(self.s3_path)

    def test_get(self):
        for report in self.report_list:
            df = report.get(self.context)
            assert df.count() == 2

    def test_put(self):

        for report in self.report_list:

            df = report.get(self.context)
            report.put(self.context, df)

            path = os.path.join(self.s3_path, report.filename_pattern.format(DT=pendulum.now().format('YYYY-MM-DD')))

            assert self.context.processor.s3bucket.is_object_exist(path)


class TestMain:

    def test_main(self, monkeypatch):
        monkeypatch.setattr(
            sys,
            'argv', [
                'main.py',
                '--config_file tests/configs/gp2s3.yaml --endpoint_file tests/configs/endpoints.yaml --reports aaa'
            ],
            raising=True
        )
        processing.main('test')
