with records as (
    select * from (
            select *,
                row_number() over (partition by C_DOC,COLLECTION_ID order by txn_ts desc, txn_id desc) as rn --новая логика фильтрации дублей
            from records_inc
            where dt>CURRENT_DATE-30
            ) t
        where t.rn = 1
)
, main_docum as (
    select * from (
            select *,
                row_number() over (partition by id order by txn_ts desc, txn_id desc) as rn
            from main_docum_inc
            where dt>CURRENT_DATE-30
            ) t
        where t.rn = 1
)
,  calendar_1 as (
  select
    case when lag(holiday_flag)over( order by calendar_date) is true and  holiday_flag  is not true then 2  --конец
          when lead(holiday_flag) over( order by calendar_date) is true and  holiday_flag  is not true then 1 --начало
          else null
    end as day_type,
    holiday_flag,
    calendar_date

  from dict_calendar_cft
  ),
calendar_2 as (
    select
        sum (day_type) over( order by calendar_date)  as group_num,
        holiday_flag,
        calendar_date
    from calendar_1
  )
  , calendar_holiday(
select
    min(calendar_date) as start_date,
    max(calendar_date) + 1  as finish_date
from calendar_2
group by group_num
having min(case when holiday_flag is False then 1 else 0 end) = 0
)

, calendar_holiday_1 as (
    select
        cast(start_date as date) as start_date,
        cast(finish_date as date) as finish_date
    from calendar_holiday
    where current_date /*cast('2024-11-12'as date) */ between cast(start_date as date) + 1 and finish_date
),

record_cdm as (select 
    r.ID,
    af.C_MAIN_V_ID,
    af.ID as ACCOUNT_NK,
    COLLECTION_ID,
    CAST(C_DATE AS DATE) AS RECORD_DATE,
    C_DATE as RECORD_DTTM,
    C_DT as RECORD_DEBIT_FLAG,
    C_START_SUM as RECORD_IN_BALANCE,
    C_SUMMA as RECORD_AMOUNT,
    C_START_SUM_NAT as RECORD_RUB_IN_BALANCE,
    C_SUMMA_NAT as RECORD_RUB_AMOUNT
    from RECORDS as r
    inner join AC_FIN as af
        on 1=1
        and C_ARC_MOVE = COLLECTION_ID
    where 1=1
        and case when (select start_date from calendar_holiday_1 ) is not null 
            then CAST(C_DATE AS DATE) between (select start_date from calendar_holiday_1 )  and     
                                                (select finish_date - 1 from calendar_holiday_1)
            else CAST(C_DATE AS DATE) =  (current_date /*cast('2024-11-28'as date)*/ - 1) end  
    and af.C_MAIN_V_ID like '30232%' 
),

record_cdm_2  as  (
SELECT
    C_MAIN_V_ID,
    COLLECTION_ID,
    ACCOUNT_NK,
    ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NK ORDER BY RECORD_DTTM ASC, ID ASC) AS TEMP_ATTR,
    RECORD_DATE AS ACCOUNT_BALANCE_EFFECTIVE_FROM_DATE,
    RECORD_RUB_IN_BALANCE AS ACCOUNT_BALANCE_IN_AMOUNT,
    -1*SUM(CASE 
        WHEN RECORD_DEBIT_FLAG IS TRUE 
        THEN RECORD_RUB_AMOUNT 
        ELSE 0 
    END) OVER (PARTITION BY ACCOUNT_NK) AS ACCOUNT_DEBIT_TURN_AMOUNT,
    SUM(CASE 
        WHEN RECORD_DEBIT_FLAG IS FALSE 
        THEN RECORD_RUB_AMOUNT
        ELSE 0 
    END) OVER (PARTITION BY ACCOUNT_NK) AS ACCOUNT_CREDIT_TURN_AMOUNT
FROM record_cdm
)
, account_balance(
SELECT
    C_MAIN_V_ID,
    ACCOUNT_NK,
    COLLECTION_ID,
    ACCOUNT_BALANCE_EFFECTIVE_FROM_DATE,
    ACCOUNT_BALANCE_IN_AMOUNT,
     ACCOUNT_DEBIT_TURN_AMOUNT,
     ACCOUNT_CREDIT_TURN_AMOUNT,
    ACCOUNT_BALANCE_IN_AMOUNT + ACCOUNT_DEBIT_TURN_AMOUNT + ACCOUNT_CREDIT_TURN_AMOUNT AS ACCOUNT_BALANCE_OUT_AMOUNT
FROM record_cdm_2
where TEMP_ATTR = 1
)

, payment_documents as (
        select
            m.C_NUM_DOG as agreement_number
            ,coalesce(sum(md.C_SUM),0) as payment_documents_amount
        from ya_merchant m
        left join hoz_op_acc h on h.COLLECTION_ID=m.C_ACCOUNT_DOG
        left join main_docum md on h.`C_ACCOUNT_DOG#1#2`=md.C_ACC_DT
        where 1=1 
            and date(md.C_DATE_DOC) = current_date /*cast('2024-11-28'as date)*/
            and md.C_NUM_DT like '30232%'
            and (
            (md.C_NUM_KT like '301%' and md.C_VID_DOC='1916953')
            or (md.C_NUM_KT like '407%' and md.C_VID_DOC='11451550'))
            and STATE_ID not in ('ISNULL','TO_RETURN','DELETED','VOZV')
        group by m.C_NUM_DOG,md.C_ACC_DT,md.C_NUM_DT
    )
    ,out_balance_amount as (
        select 
              m.C_NUM_DOG as agreement_number
              ,a.C_MAIN_V_ID as account_number
              ,c.C_NAME as client_name
              ,uc.C_NAME as agreement_type
              ,coalesce(r.account_balance_out_amount,0) as out_balance_amount
        from ya_merchant m
        left join client c on c.ID=m.C_CLIENT
        left join ud_code_name uc on uc.ID=m.C_TYPE_DOG
        left join hoz_op_acc h on h.COLLECTION_ID=m.C_ACCOUNT_DOG
        left join ac_fin a on h.`C_ACCOUNT_DOG#1#2`=a.ID
        left join account_balance r on a.ID = r.account_nk
        where 1=1
            and a.C_MAIN_V_ID like '30232%' 
    )
    select
        ba.agreement_number
        ,ba.client_name
        ,ba.agreement_type
        ,ba.account_number
        ,ba.out_balance_amount
        ,coalesce(pd.payment_documents_amount,0) as payment_documents_amount
        ,coalesce(pd.payment_documents_amount,0)-ba.out_balance_amount as diff_amount
    from out_balance_amount ba
    left join payment_documents pd on ba.agreement_number=pd.agreement_number
    order by coalesce(pd.payment_documents_amount,0) - ba.out_balance_amount
