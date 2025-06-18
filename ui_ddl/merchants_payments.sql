SELECT
    r_report_robot.md_upsert_report
    (
        'merchants_payments',
        'Сверка возмещений по выплатам мерчантам',
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
                'DET', 'Сверка', 'reconciliation.merchants_payments', NULL,
                 ARRAY
                 [
                    r_report_robot.md_rec_report_dataset_column('agreement_number', 'Номер договора', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('client_name', 'Наименование клиента', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('agreement_type', 'Тип договора', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('account_number', 'Номер счета', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('out_balance_amount', 'out_balance_amount', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('payment_documents_amount', 'payment_documents_amount', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('diff_amount', 'Объем расхождений', 'N', '#,#0.00')
                ],
                NULL
            )
        ]
    )
