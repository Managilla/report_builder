SELECT
    r_report_robot.md_upsert_report
    (
        'auto_funding',
        'Сверка распоряжений на автопополнения и автофондирование ABS-FTC',
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
                'DET', 'Сверка распоряжений на автопополнения и автофондирование ABS-FTC', 'reconciliation.auto_funding', NULL,
                 ARRAY
                 [
                    r_report_robot.md_rec_report_dataset_column('number', 'Номер', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('buid', 'buid', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('ftc_start_date', 'Дата начала (FTC)', 'D', 'yyyy-mm-dd'),
                    r_report_robot.md_rec_report_dataset_column('abs_start_date', 'Дата начала (ABS)', 'D', 'yyyy-mm-dd'),
                    r_report_robot.md_rec_report_dataset_column('ftc_end_date', 'Дата окончания (FTC)', 'D', 'yyyy-mm-dd'),
                    r_report_robot.md_rec_report_dataset_column('abs_end_date', 'Дата окончания (ABS)', 'D', 'yyyy-mm-dd')
                ],
                NULL
            )
        ]
    )
