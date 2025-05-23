SELECT md.C_DATE_PROV as report_date
    , sum(case when substring(c_num_dt, 1, 5)='30110' and dkt.amount_type='payments_within_bank' then md.c_sum end) as payments_within_bank_amount
    , sum(case when ddt.amount_type='refunds_within_bank' and substring(md.c_num_kt, 1, 5)='30110' then md.c_sum end) as refunds_within_bank_amount
    , sum(case when ddt.amount_type='payments_alfa' and dkt.amount_type='payments_alfa' then md.c_sum end) as payments_alfa_amount
    , sum(case when ddt.amount_type='refunds_alfa' and dkt.amount_type='refunds_alfa' then md.c_sum end) as refunds_alfa_amount
FROM (
    select
        cast(md.C_DATE_PROV as date) as C_DATE_PROV,
        md.c_num_kt,
        md.c_num_dt,
        md.id,
        md.c_sum,
        row_number() over (partition by md.id order by md.txn_ts desc, md.txn_id desc) as rn
    from main_docum_inc md
    where 1=1
        and md.state_id='PROV'
        and cast(md.C_DATE_PROV as date) = '{DT}'
    ) md
            LEFT JOIN dict_acquiring_alpha ddt on ddt.c_num=md.c_num_dt and ddt.c_type='DT'
            LEFT JOIN dict_acquiring_alpha dkt on dkt.c_num=md.c_num_kt and dkt.c_type='KT'
WHERE md.rn=1 and
(
    (substring(c_num_dt, 1, 5)='30110' and dkt.amount_type='payments_within_bank')
    or (ddt.amount_type='refunds_within_bank' and substring(md.c_num_kt, 1, 5)='30110')
    or (ddt.amount_type='payments_alfa' and dkt.amount_type='payments_alfa')
    or (ddt.amount_type='refunds_alfa' and dkt.amount_type='refunds_alfa')
)
GROUP BY C_DATE_PROV