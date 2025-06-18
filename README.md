# PySpark job report_builder

## Документация
Документация инструмента поддерживается в [Docs](https://docs.yandex-team.ru/fintech/dwh-service/reconciliation/reconciliation#ssylki).

## Отладка на NPE

```shell
cd ~/arcadia/fintech/dwh/spark_jobs/report_builder
poetry shell
dwhcli job.build -p `pwd` -m report_builder
dwhcli job.upload-npe -p `pwd` -m report_builder
```

Для запуска тестов локально необходимо добавить следующий jar в папку c jar файлами (Версия spark 3.3.):
https://mvnrepository.com/artifact/com.crealytics/spark-excel_2.12/3.3.1_0.18.7

## VIEW для дашборда

```sql
CREATE VIEW dwh.events_report_builder ON CLUSTER '{cluster}' AS
SELECT e.*
	, trim(BOTH ' "' FROM visitParamExtractRaw(e.etc, 'result')) AS result
	, trim(BOTH ' "' FROM visitParamExtractRaw(etc, 'ticket_key')) AS ticket_key
	, trim(BOTH ' "' FROM visitParamExtractRaw(etc, 'error_msg')) AS error_msg
	, trim(BOTH ' "' FROM visitParamExtractRaw(etc, 'attachment_paths')) AS attachment_paths
	, JSONExtract(attachment_paths, 'Array(String)') AS arrays
	, arrayJoin(arrays) AS filepath
from dwh.events AS e
WHERE service_name = 'REPORT_BUILDER'
UNION ALL
SELECT e.*
	, trim(BOTH ' "' FROM visitParamExtractRaw(e.etc, 'result')) AS result
	, trim(BOTH ' "' FROM visitParamExtractRaw(etc, 'ticket_key')) AS ticket_key
	, trim(BOTH ' "' FROM visitParamExtractRaw(etc, 'error_msg')) AS error_msg
	, trim(BOTH ' "' FROM visitParamExtractRaw(etc, 'attachment_paths')) AS attachment_paths
	, JSONExtract(attachment_paths, 'Array(String)') AS arrays
	, '' AS filepath
from dwh.events AS e
WHERE service_name = 'REPORT_BUILDER'
	AND attachment_paths = ''
```

