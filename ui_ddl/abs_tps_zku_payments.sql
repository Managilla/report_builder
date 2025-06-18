SELECT
    r_report_robot.md_upsert_report
    (
        'abs_tps_zku_payments',
        'Сверка платежных документов с сообщениями в ГИС ЖКХ',
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
                'DET', 'Сверка', 'reconciliation.abs_tps_zku_payments', NULL,
                 ARRAY
                 [
                    r_report_robot.md_rec_report_dataset_column('check_date', 'Дата сверки', 'D', 'yyyy-mm-dd'),
                    r_report_robot.md_rec_report_dataset_column('document_date', 'Дата документа', 'D', 'yyyy-mm-dd'),
                    r_report_robot.md_rec_report_dataset_column('document_num', 'Номер документа', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('document_sum', 'Сумма документа', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('payer_account', 'Счет плательщика', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('recepient_account', 'Счет получателя', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('payment_purpose', 'Назначение платежа', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('ftc_identifier', '', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('diff_type', 'Тип ошибки', 'S', NULL)
                ],
                NULL
            )
        ]
    )








