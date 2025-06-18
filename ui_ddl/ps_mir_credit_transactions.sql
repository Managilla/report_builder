SELECT
    r_report_robot.md_upsert_report
    (
        'ps_mir_credit_transactions',
        'Сверка по кредитным транзакциям с документами АБС',
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
                'DET', 'Сверка по кредитным транзакциям с документами АБС', 'reconciliation.ps_mir_credit_transactions', NULL,
                 ARRAY
                 [  

                    r_report_robot.md_rec_report_dataset_column('report_date', 'Дата отчета', 'D', 'yyyy-mm-dd'),
                    r_report_robot.md_rec_report_dataset_column('report_name', 'Наименование отчёта', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('error_name', 'Наименование ошибки', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('txn_date', 'Дата транзакции', 'D', 'yyyy-mm-dd'),
                    r_report_robot.md_rec_report_dataset_column('debit_account_number', 'debit_account_number', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('credit_account_number', 'credit_account_number', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('ttype', 'Тип транзакции', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('direction', 'Направление', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('t_direction', 'Тип направления', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('amount', 'Сумма операции', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('sum_settl_amount', 'sum_settl_amount', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('sum_c_sum', 'sum_c_sum', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('sum_settl_amount_rrn', 'sum_settl_amount_rrn', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('sum_sum_nazn_rrn', 'sum_sum_nazn_rrn', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('sum_sum_rrn', 'sum_sum_rrn', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('agr_number', 'Номер договора', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('rrn', 'RRN', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('acq_ref_number', 'ARN', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('trace_ref_number', 'TRN', 'S', NULL)
                ],
                NULL
            )
        ]
    )
