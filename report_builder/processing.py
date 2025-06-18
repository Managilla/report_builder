import shlex
import sys
from argparse import ArgumentParser
from pathlib import Path

from fintech_dwh_lib import conf

import pendulum

from .src.context import ReportContext
from .src.endpoint import Endpoint, S3endpoint
from .src.events import Events
from .src.pipeline import ReportList, ReportStatus


def get_args():
    args_raw = shlex.split(sys.argv[1])

    parser = ArgumentParser()

    parser.add_argument('--config_file', default=None, required=True)
    parser.add_argument('--endpoint_file', default='endpoints.yaml', required=False)

    parser.add_argument('--reports', default=None, required=False)
    parser.add_argument('--s3_endpoint', default=None, required=False)

    parser.add_argument('--DT', default=None, required=False)

    return parser.parse_known_args(args_raw)[0]


def main(job_name):
    print('Started job')
    print("Arguments received:", sys.argv)

    parsed_args = get_args()
    print(parsed_args)

    config_file_resolved = Path(parsed_args.config_file)
    if not config_file_resolved.exists():
        raise ValueError(f'Path {config_file_resolved} doesnt exist')

    if parsed_args.reports:
        report_list = ReportList().load(config_file_resolved, reports=parsed_args.reports.split(','))
    else:
        report_list = ReportList().load(config_file_resolved)

    endpoint_file_resolved = Path(parsed_args.endpoint_file)
    if not endpoint_file_resolved.exists():
        raise ValueError(f'Path {endpoint_file_resolved} doesnt exist')

    if parsed_args.s3_endpoint:
        s3_ep = Endpoint().load(endpoint_file_resolved, endpoint_name=parsed_args.s3_endpoint)
    else:
        s3_ep = S3endpoint(
            endpoint_type_name='S3',
            endpoint_name='default',
            bucket_name=conf.s3.S3_BUCKET,
            lockbox_secret_id=conf.s3.S3_SECRET_ID
        )

    context = ReportContext(job_name, s3_ep)

    context.logger.info(str(len(report_list)))
    context.logger.info(str(report_list))

    context.logger.info('Started processing reports')

    for report in report_list:
        if parsed_args.DT:
            report.DT = (pendulum.from_format(parsed_args.DT, 'YYYY-MM-DD') - pendulum.today()).days

        events = Events(report.name, context)
        try:
            events.add(result='IN PROCESS')
            events.start()
            if getattr(report, 'include_in_normative_reports', False):
                ReportStatus.start(report, context)
            context.logger.info(f'Started processing report {report.name}')
            report.run(context, events)
            events.end()
            if getattr(report, 'include_in_normative_reports', False):
                ReportStatus.end(context)
        except Exception as e:
            events.error()
            if getattr(report, 'include_in_normative_reports', False):
                ReportStatus.error(context)
            raise e

    context.processor.session.stop()
