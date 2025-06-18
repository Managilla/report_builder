SELECT
    r_report_robot.md_upsert_report
    (
        'ps_mir_transaction_duplicate',
        'Сверка ПС МИР на дубли транзакций',
        DATE '2024-01-01',
        NULL,
        NULL,
        'DWH_RECON',
        ARRAY
        [
            r_report_robot.md_rec_report_sparameter(
            'p_report_dt',
            'Отчетная дата',
            'null', NULL,
            'S',
            'report_dt = {{' || 'p_report_dt' || '}}'
            ),
            r_report_robot.md_rec_report_sparameter(
            'p_processing_dttm',
            'Дата и время запуска отчета',
            'null', NULL,
            'S',
            'processing_dttm = {{' || 'p_processing_dttm' || '}}'
            )
        ],
        ARRAY
        [
            r_report_robot.md_rec_report_dataset
            (
                'DET', 'Сверка ПС МИР на дубли транзакций', 'reconciliation.ps_mir_transaction_duplicate', NULL,
                 ARRAY
                 [
                    r_report_robot.md_rec_report_dataset_column('report_name', 'Наименование отчёта', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('banking_date', 'Дата транзакции', 'D', 'yyyy-mm-dd'),
                    r_report_robot.md_rec_report_dataset_column('ttype', 'Тип транзакции', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('amount', 'Сумма операции', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('ret_ref_number', 'RRN', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('trace_ref_number', 'TRN', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('acq_ref_number', 'ARN', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('abs_nazn', 'Назначение платежа из АБС', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('root_txn_id', 'root_txn_id', 'S', NULL)
                ],
                NULL
            )
        ]
    )

