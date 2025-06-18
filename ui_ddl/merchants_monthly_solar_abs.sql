SELECT
    r_report_robot.md_upsert_report
    (
        'merchants_monthly_solar_abs',
        'Ежемесячная свервка по мерчантам Солар и АБС',
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
                'DET', 'Сверка', 'reconciliation.merchants_monthly_solar_abs', NULL,
                 ARRAY
                 [
                    r_report_robot.md_rec_report_dataset_column('ttype', 'Тип', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('agr_number', 'Номер договора', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('solar_amt', 'Сумма Солар', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('acc', 'acc', 'S', NULL),
                    r_report_robot.md_rec_report_dataset_column('abs_amt', 'Сумма АБС', 'N', '#,#0.00'),
                    r_report_robot.md_rec_report_dataset_column('diff_amt', 'Разность', 'N', '#,#0.00')
                ],
                NULL
            )
        ]
    )
