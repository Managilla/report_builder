with abs_ as( --get_abs_df --abs_df
select *
    , CAST(ac_fin.id AS BIGINT) AS ID
    , ac_fin.C_MAIN_V_ID as ABS_account
    , CAST(TO_DATE(account.C_DATE_OP) AS DATE) AS ABS_date_open
    , CAST(TO_DATE(account.C_DATE_CLOSE) AS DATE) AS ABS_date_close
    , CASE
            WHEN com_status_prd.C_NAME IS NULL THEN 'Открыт'
            ELSE com_status_prd.C_NAME
        END AS ABS_status_name
    , CASE
            WHEN LOWER(C_CODE) = 'to_close' THEN 'CLOSE'
            WHEN LOWER(C_CODE) = 'close' THEN 'CLOSE'
            WHEN C_CODE IS NULL THEN 'OPEN'
            ELSE C_CODE
        END AS ABS_acc_status
    , account.C_NAME AS ABS_acc_name
from ac_fin
    left join com_status_prd on ac_fin.C_COM_STATUS = com_status_prd.ID
    join account on ac_fin.id = account.id
WHERE 1=1
and (ac_fin.C_MAIN_V_ID LIKE '40903%'
   OR ac_fin.C_MAIN_V_ID LIKE '40914%'
   OR ac_fin.C_MAIN_V_ID LIKE '3023_810_1%'
   OR ac_fin.C_MAIN_V_ID LIKE '3023_810_2%'
   OR ac_fin.C_MAIN_V_ID LIKE '47423810_1%'
   OR ac_fin.C_MAIN_V_ID LIKE '47423810_2%'
   )
   and (account.C_DATE_OP = "{DT}" OR account.C_DATE_CLOSE = "{DT}")
)
, solar as ( --union_solar -- solar_account_df
-- get_solar_pay_iss_acc_acc_account_df
select *
    , CAST(account_type_id AS BIGINT) AS account_type_id
    , account_number AS SOL_account
    , CASE
            WHEN status = 'A' THEN 'open'
            ELSE 'close'
        END AS SOL_status
    , CAST(opening_date AS DATE) AS SOL_date_open
    , CAST(closing_date AS DATE) AS SOL_date_close
from iss_acc_account --get_solar_pay_iss_acc_acc_account_df
where category = 'A'
    and (
    account_number LIKE '40903%'
   OR account_number LIKE '40914%'
   OR account_number LIKE '3023_810_1%'
   OR account_number LIKE '3023_810_2%'
   OR account_number LIKE '47423810_1%'
   OR account_number LIKE '47423810_2%'
   )
   and (opening_date = "{DT}" OR closing_date = "{DT}")
union all
-- get_solar_pay_acq_acc_acc_account_df
select *
    , CAST(account_type_id AS BIGINT) AS account_type_id
    , account_number AS SOL_account
    , CASE
            WHEN status = 'A' THEN 'open'
            ELSE 'close'
        END AS SOL_status
    , CAST(opening_date AS DATE) AS SOL_date_open
    , CAST(closing_date AS DATE) AS SOL_date_close
from acq_acc_account --get_solar_pay_acq_acc_acc_account_df
where category = 'A'
    and (
    account_number LIKE '3023_810_1%'
    OR account_number LIKE '3023_810_2%'
    OR account_number LIKE '47423810_1%'
    )
    and (opening_date = "{DT}" OR closing_date = "{DT}")
)
, acc_unmatched as ( -- get_acc_unmatched_df
    select *
        , case when SOL_account is null then 'Счёт не найден в Solar' else 'Счёт не найден в АБС' end as dqc_description
    from solar
        full join abs_ on solar.SOL_account = abs_.ABS_account
    WHERE solar.SOL_account IS NULL 
        OR abs_.ABS_account IS NULL
)
, acc_matched as( --get_acc_matched_df -- matched_df
    SELECT *
    FROM solar
        INNER JOIN abs_ ON solar.SOL_account = abs_.ABS_account
)
, open_date_unmatched as(
    select t.*
    , 'Дата открытия в АБС и Solar не совпадает' as dqc_description
    from acc_matched t
    where SOL_date_open != ABS_date_open
)
, close_date_unmatched as(
    select t.*
    , 'Дата закрытия в АБС и Solar не совпадает' as dqc_description
    from acc_matched t
    where SOL_date_close != ABS_date_close
)
, status_unmatched as (
    select t.*
    , 'Статус в АБС и Solar не совпадает' as dqc_description
    from acc_matched t
    where SOL_status != lower(ABS_acc_status)
)
, final as(
    select * from open_date_unmatched
    union
    select * from close_date_unmatched
    union
    select * from status_unmatched
    union
    select * from acc_unmatched
)
select to_date("{DT}", "yyyy-MM-dd") as report_dt
    , ABS_account
    , SOL_account
    , ABS_date_open
    , SOL_date_open
    , ABS_date_close
    , SOL_date_close
    , ABS_acc_status
    , SOL_status
    , dqc_description
from final
