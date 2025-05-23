import json
import traceback

from fintech_dwh_lib.events_kafka import KafkaEventsSender
from fintech_dwh_lib.spark import utils

from .context import ReportContext


class Events:

    def __init__(self, name: str, context: ReportContext):
        self.context = context
        self.data = {}

        context.processor.events_kafka.set_template(
            service_name='REPORT_BUILDER',
            job_name=name,
            task_group_name='',
            task_name='',
            application_id=utils.get_job_id(),
            tags=['core_team', 'report_builder']
        )

    def add(self, **kwargs):
        for key, value in kwargs.items():
            self.data[key] = value

    def start(self):
        self.context.processor.events_kafka.push_event(
            self.context.processor.events_kafka.build_event_from_template(
                event_type='START', description='Job start', etc=self.data
            )
        )

    def end(self):
        self.context.processor.events_kafka.push_event(
            self.context.processor.events_kafka.build_event_from_template(
                event_type='END', description='Job end', etc=self.data
            )
        )

    def error(self):
        self.add(result='ERROR')
        self.context.processor.events_kafka.push_event(
            self.context.processor.events_kafka.build_event_from_template(
                event_type='ERROR',
                description='Job failed',
                etc={
                    **{
                        'error_msg': traceback.format_exc()
                    },
                    **self.data
                }
            )
        )
