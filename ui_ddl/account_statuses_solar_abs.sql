SELECT
    r_report_robot.md_upsert_report
    (
        'account_statuses_solar_abs',
        'Сверка статусов счетов АБС и Солар',
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
                'DET', 'Сверка статусов счетов АБС и Солар', 'reconciliation.account_statuses_solar_abs', NULL,
                 ARRAY
                 [
					r_report_robot.md_rec_report_dataset_column('abs_account', 'Лицевой счет (АБС)', 'S', NULL),
					r_report_robot.md_rec_report_dataset_column('sol_account', 'Лицевой счет (Солар)', 'S', NULL),
					r_report_robot.md_rec_report_dataset_column('abs_date_open', 'Дата открытия счета (АБС)', 'D', 'yyyy-mm-dd'),
					r_report_robot.md_rec_report_dataset_column('sol_date_open', 'Дата открытия счета (Солар)', 'D', 'yyyy-mm-dd'),
					r_report_robot.md_rec_report_dataset_column('abs_date_close', 'Дата закрытия счета (АБС)', 'D', 'yyyy-mm-dd'),
					r_report_robot.md_rec_report_dataset_column('sol_date_close', 'Дата закрытия счета (Солар)', 'D', 'yyyy-mm-dd'),
					r_report_robot.md_rec_report_dataset_column('abs_acc_status', 'Статус счета (АБС)', 'S', NULL),
					r_report_robot.md_rec_report_dataset_column('sol_status', 'Статус счета (Солар)', 'S', NULL),
					r_report_robot.md_rec_report_dataset_column('dqc_description', 'Описание расхождения', 'S', NULL)
                ],
                NULL
            )
        ]
    )
