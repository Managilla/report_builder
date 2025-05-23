with smev as (
    select 
        instruction_id,
        get_json_object(request, '$.recipientInfo.recipientName') as recipientName,
        get_json_object(request, '$.recipientInfo.accountNumber') as accountNumber,
        get_json_object(request, '$.recipientInfo.bic') as bic,
        get_json_object(request, '$.recipientInfo.inn') as inn,
        get_json_object(request, '$.orderInfo.santiAmount') as santiAmount,
        get_json_object(request, '$.orderInfo.paymentPurpose') as paymentPurpose,
        status,
        type,
        request_time,
        ROW_NUMBER() OVER (PARTITION BY instruction_id ORDER BY request_time DESC) as rn
    from smev_message
    where (cast(get_json_object(request, '$.orderInfo.date') as date) >= current_date() - interval '31 days'
    and cast(get_json_object(request, '$.orderInfo.date') as date) < current_date())
)

--Отбираем данные из ABS

, document_sorted as(
    select *
    from (
        select id,
            txn_id,
            max(txn_id) over (partition by id) as max_txn_id
        from document
        where c_comment = '^~zkuPayment~true~^'
        )
    where txn_id = max_txn_id
)

, df_abs as (
    select 
        md.id, 
        md.C_DATE_DOC, 
        md.C_DOCUMENT_NUM, 
        md.STATE_ID, 
        md.`C_KL_DT#2#1`, 
        md.`C_KL_DT#2#2`, 
        md.`C_KL_KT#2#1`, 
        md.`C_KL_KT#2#2`, 
        md.`C_KL_KT#2#INN`, 
        md.C_SUM, 
        md.C_NAZN, 
        md.txn_id,
        msg.C_FTC_ID__PAYMENT_INSTRUCTION_ID
    from main_docum md
    inner join document_sorted doc
        on (md.id = doc.id)
    left join message msg
        on (md.id = msg.c_docum_id)
    where md.C_DATE_DOC >= current_date() - interval '31 days'
        and md.C_DATE_DOC < current_date()
)

--Объединяем данные ABS и core-tps

, common_table as (
    select *
    from df_abs a
    full join smev s
    on (a.C_FTC_ID__PAYMENT_INSTRUCTION_ID = s.instruction_id)
    where s.rn = 1
)

--Ищем дубли из ABS

, abs_doubles_group as(
    select 
        id,
        max(txn_id) as txn,
        count(*) as cnt
    from common_table
    where id is not null
    group by id
)
, common_table_with_doubles as(
    select
        c.*,
        d.txn
    from common_table c
    left join abs_doubles_group d
    on (c.id = d.id and d.cnt > 1)

)

--Добавляем типы расхождений

, common_table_unduplicated as (
    select 
        *,
        case when txn is not null then 'double' else null end as abs_doubles 
    from common_table_with_doubles
    where 
        txn is null
        or txn=txn_id
)

, common_table_diffs as(
    select 
    *,
    case
        when (instruction_id = C_FTC_ID__PAYMENT_INSTRUCTION_ID
                and type = 'HCS_NOTIFICATION'
                and state_id = 'PROV'
                and status = 'FINISHED_SUCCESS')
            or (instruction_id = C_FTC_ID__PAYMENT_INSTRUCTION_ID
                and type = 'HCS_CANCELLATION'
                and STATE_ID != 'PROV'
                and status = 'FINISHED_SUCCESS')
            then 'success'
        when (instruction_id = C_FTC_ID__PAYMENT_INSTRUCTION_ID
                and type = 'HCS_NOTIFICATION'
                and state_id = 'PROV'
                and status != 'FINISHED_SUCCESS')
            then 'hcs_notification_not_finished_success'
        when (instruction_id = C_FTC_ID__PAYMENT_INSTRUCTION_ID
                and type = 'HCS_NOTIFICATION'
                and state_id != 'PROV'
                and status = 'FINISHED_SUCCESS')
            then 'hcs_notification_not_prov'
        when (instruction_id = C_FTC_ID__PAYMENT_INSTRUCTION_ID
                and type = 'HCS_NOTIFICATION'
                and state_id != 'PROV'
                and status != 'FINISHED_SUCCESS')
            then 'hcs_notification_not_prov_not_success'
        when (instruction_id = C_FTC_ID__PAYMENT_INSTRUCTION_ID
                and type = 'HCS_CANCELLATION'
                and state_id = 'PROV'
                and status = 'FINISHED_SUCCESS')
            then 'hcs_cancellation_prov_success'
        when (instruction_id = C_FTC_ID__PAYMENT_INSTRUCTION_ID
                and type = 'HCS_CANCELLATION'
                and state_id = 'PROV'
                and status != 'FINISHED_SUCCESS')
            then 'hcs_cancellation_prov_not_success'
        when (instruction_id = C_FTC_ID__PAYMENT_INSTRUCTION_ID
                and type = 'HCS_CANCELLATION'
                and state_id != 'PROV'
                and status != 'FINISHED_SUCCESS')
            then 'hcs_cancellation_not_prov_not_success'
        when instruction_id is null
            then 'not_in_ftc'
        when C_FTC_ID__PAYMENT_INSTRUCTION_ID is null
            then 'not_in_abs'
        end as diff_type
    from common_table_unduplicated
)

--Выбираем строки, имеющие расхождения

select 
    current_date() as check_date,
    cast(C_DATE_DOC as date) as document_date,
    C_DOCUMENT_NUM as document_num,
    coalesce(C_SUM, santiAmount/100.00) as document_sum,
    `C_KL_DT#2#1` as payer_account,
    coalesce(`C_KL_KT#2#1`, accountNumber) as recepient_account,
    coalesce(C_NAZN, paymentPurpose)as payment_purpose,
    coalesce(C_FTC_ID__PAYMENT_INSTRUCTION_ID, instruction_id) as ftc_identifier,
    diff_type
from common_table_diffs
where diff_type != 'success'
UNION ALL
select 
    current_date() as check_date,
    cast(C_DATE_DOC as date) as document_date,
    C_DOCUMENT_NUM as document_num,
    coalesce(C_SUM, santiAmount/100.00) as document_sum,
    `C_KL_DT#2#1` as payer_account,
    coalesce(`C_KL_KT#2#1`, accountNumber) as recepient_account,
    coalesce(C_NAZN, paymentPurpose)as payment_purpose,
    coalesce(C_FTC_ID__PAYMENT_INSTRUCTION_ID, instruction_id) as ftc_identifier,
    'abs_doubles' as diff_type
from common_table_diffs
where abs_doubles = 'double'
