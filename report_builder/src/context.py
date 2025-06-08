import sys
from dataclasses import dataclass
from logging import Logger

from fintech_dwh_lib import conf
from fintech_dwh_lib.events_kafka import KafkaEventsSender
from fintech_dwh_lib.greenplum.dwh_sqlalchemy import DwhGpSqlalchemyProcessor
from fintech_dwh_lib.logging import build_logger_message_template, getLogger
from fintech_dwh_lib.spark import S3SparkProcessor, get_job_id
from fintech_dwh_lib.spark.conf import spark_conf

from .endpoint import S3endpoint


@dataclass(init=False)
class ReportContext:
    job_name: str
    processor: S3SparkProcessor
    logger: Logger

    def __init__(
        self,
        job_name: str,
        s3_ep: S3endpoint,
    ):
        self.job_name = job_name
        self._set_logger()
        self._set_spark_proc(s3_ep)
        self._set_gp_proc()

    def _get_spark_params(self):
        return spark_conf

    def _set_logger(self):
        message_template = build_logger_message_template(
            job_id=get_job_id(), job_name=self.job_name, command=sys.argv[0], arguments=sys.argv[1:]
        )
        self.logger = getLogger(self.job_name, message_template=message_template)

    def _set_spark_proc(self, s3_ep):
        self.processor = S3SparkProcessor(
            app_name=self.job_name,
            bucket=s3_ep.bucket_name,
            lockbox_secret_id=s3_ep.lockbox_secret_id,
            logger=self.logger,
            spark_conf=self._get_spark_params(),
            gp_lockbox_secret_id=conf.gp.GP_SECRET_ID,
            events_kafka=KafkaEventsSender(self.logger),
        )

    def _set_gp_proc(self):
        self.gp_processor = DwhGpSqlalchemyProcessor(
            job_id=get_job_id(),
            job_name=self.job_name,
            use_schema_writer=True,
        )
