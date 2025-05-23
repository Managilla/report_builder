cd ~/arcadia/fintech/dwh/spark_jobs/report_builder

dwhcli job.build -p `pwd` -m report_builder
dwhcli job.upload-npe -p `pwd` -m report_builder

s3cmd -c ~/.s3cfg-npe sync --delete-removed ~/arcadia/fintech/dwh/spark_jobs/report_builder/tests/configs/ \
  s3://bank-dwh-analytics-s3-npe/spark-jobs/report_builder/tests/configs/

s3cmd -c ~/.s3cfg-npe sync --delete-removed ~/arcadia/fintech/dwh/spark_jobs/report_builder/tests/templates/ \
  s3://bank-dwh-analytics-s3-npe/spark-jobs/report_builder/tests/templates/

s3cmd -c ~/.s3cfg-npe sync --delete-removed ~/arcadia/fintech/dwh/spark_jobs/report_builder/tests/sql/ \
  s3://bank-dwh-analytics-s3-npe/spark-jobs/report_builder/tests/sql/
