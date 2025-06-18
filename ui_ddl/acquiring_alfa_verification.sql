SELECT
    r_report_robot.md_upsert_report
    (
        'acquiring_alfa_verification',
        'Сверка по эквайрингу через Альфу',
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
                'DET', 'Сверка по эквайрингу через Альфу', 'reconciliation.acquiring_alfa_verification', NULL,
                 ARRAY
                 [
                    r_report_robot.md_rec_report_dataset_column('payments_within_bank_amount', 'Сумма платежей внутри банка', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('refunds_within_bank_amount', 'Сумма возвратов внутри банка', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('payments_alfa_amount', 'Сумма платежей от Альфы', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('refunds_alfa_amount', 'Сумма возвратов от Альфы', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('difference_refunds_amount', 'Разница между возвратами внутри банка и от Альфы', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('difference_payments_amount', 'Разница между платежами внутри банка и от Альфы', 'N', '#,#0.00')
                ],
                NULL
            )
        ]
    )
