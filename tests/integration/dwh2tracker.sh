export ENDPOINT_FILE=endpoints.yaml
export ENDPOINT_FILE_PATH=tests/configs/endpoints.yaml

export CONFIG_FILE=dwh2tracker.yaml
export CONFIG_FILE_PATH=tests/configs/dwh2tracker.yaml

yc dataproc job create-pyspark --name ${CONFIG_FILE} --cluster-id c9q1lrmmp5id9eg9fl3a \
    --file-uris s3a://bank-dwh-analytics-s3-npe/spark-jobs/report_builder/YandexCA.crt \
    --file-uris s3a://bank-dwh-analytics-s3-npe/spark-jobs/report_builder/YandexBankNPERootCA.crt \
    --file-uris s3a://bank-dwh-analytics-s3-npe/spark-jobs/report_builder/requirements.txt \
    --file-uris s3a://bank-dwh-analytics-s3-npe/spark-jobs/report_builder/env_name \
    --file-uris s3a://bank-dwh-analytics-s3-npe/spark-jobs/report_builder/report_builder.npe.env \
    --file-uris s3a://bank-dwh-analytics-s3-npe/spark-jobs/report_builder/${ENDPOINT_FILE_PATH} \
    --file-uris s3a://bank-dwh-analytics-s3-npe/spark-jobs/report_builder/${CONFIG_FILE_PATH} \
    --python-file-uris s3a://bank-dwh-analytics-s3-npe/spark-jobs/report_builder/report_builder.zip \
    --jar-file-uris s3a://bank-dwh-analytics-s3-npe/jars_packages/ojdbc8.jar \
    --jar-file-uris s3a://bank-dwh-analytics-s3-npe/jars_packages/greenplum-connector-apache-spark-scala_2.12-2.1.4.jar \
    --jar-file-uris s3a://bank-dwh-analytics-s3-npe/jars_packages/spark-excel_2.12-3.3.1_0.18.7.jar \
    --main-python-file-uri s3a://bank-dwh-analytics-s3-npe/spark-jobs/report_builder/main.py \
    --args "--endpoint_file ${ENDPOINT_FILE} --config_file ${CONFIG_FILE} --reports ps_mir2" \
    --properties spark.yarn.maxAppAttempts=1
