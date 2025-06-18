SELECT
    r_report_robot.md_upsert_report
    (
        'agreements_solar_abs',
        'Сверка договоров АБС и Солар',
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
                'DET', 'Сверка договоров АБС и Солар', 'reconciliation.agreements_solar_abs', NULL,
                 ARRAY
                 [
                    r_report_robot.md_rec_report_dataset_column('error_name', 'Название ошибки', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('agr_number', 'Номер договора', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('abs_open_date', 'Дата открытия договора (АБС)', 'D', 'yyyy-mm-dd'),
                    r_report_robot.md_rec_report_dataset_column('solar_open_date', 'Дата открытия договора (Солар)', 'D', 'yyyy-mm-dd'),
                    r_report_robot.md_rec_report_dataset_column('abs_close_date', 'Дата закрытия договора (АБС)', 'D', 'yyyy-mm-dd'),
                    r_report_robot.md_rec_report_dataset_column('solar_close_date', 'Дата закрытия договора (Солар)', 'D', 'yyyy-mm-dd')
                ],
                NULL
            )
        ]
    )
