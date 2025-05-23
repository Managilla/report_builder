with main_docum as(
    select * from (
            select *,
                row_number() over (partition by id order by txn_ts desc, txn_id desc) as rn
            from main_docum_inc
            where dt>CURRENT_DATE-30
            ) t
        where t.rn = 1
)
, autofund_autopop_buid as(
select  
    idempotency_token,
    buid
from autofund_actions_history
union
select  
    idempotency_token,
    buid
from actions_history
)

-- Отбираем открытие и закрытие распоряжений из ftc на отчетную дату 

, autofund_1 as(
select  
    idempotency_token,
    (created_at + (3 * interval '1 hour')) as start_date,
    public_agreement_id,
    buid,
    get_json_object(current_state   ,'$.status') as current_status,
    get_json_object(current_state   ,'$.params.paymentMethod.paymentType') as current_paytype,
    LEAD(created_at + (3 * interval '1 hour')) over(partition by buid,public_agreement_id order by created_at) as end_date
from autofund_actions_history
), 
autopop_1 as(
select  
    idempotency_token,
    (created_at + (3 * interval '1 hour')) as start_date,
    public_agreement_id,
    buid,
    get_json_object(current_state   ,'$.status') as current_status,
    get_json_object(current_state   ,'$.paymentMethodInfo.paymentType') as current_paytype,
    LEAD(created_at + (3 * interval '1 hour')) over(partition by buid,public_agreement_id order by created_at) as end_date
from actions_history
),
union_auto as(
select  
    idempotency_token ,
    start_date,
    end_date
from autofund_1
where 1=1 
    and current_paytype = 'SAVINGS_ACCOUNT'
    and current_status  = 'ENABLED'
    and (cast(start_date  as date) = CURRENT_DATE - 1
        or cast(end_date  as date) = CURRENT_DATE - 1)
union all
select     
    idempotency_token ,
    start_date,
    end_date
from autopop_1
where 1=1 
    and current_paytype = 'SAVINGS_ACCOUNT'
    and current_status  = 'ENABLED'
    and (cast(start_date  as date) = CURRENT_DATE - 1 
        or cast(end_date  as date) = CURRENT_DATE - 1)
) 
, autofund_autopop_2 as(

select 
    idempotency_token,
    cast(start_date  as date) as start_date,
    cast('2100-12-31' as date) as  end_date
from union_auto
where cast(start_date  as date) = CURRENT_DATE - 1

union all

select 
    idempotency_token,
    cast('1900-01-01' as date) as  start_date,
    cast(end_date  as date) as end_date
from union_auto
where cast(end_date  as date) = CURRENT_DATE - 1
)

-- Отбираем открытие и закрытие распоряжений из abs на отчетную дату 

, main_docum_2 as (
select 
    substring(LEFT(C_NAZN , position('от' in C_NAZN)-2),position('№' in C_NAZN)+2) as number,
    cast(C_DATE_PROV as date) as start_date,
    cast('2100-12-31' as date) as end_date
from MAIN_DOCUM
where 1=1
and C_DATE_PROV > '2024-07-23'
and C_NUM_DT like   '90909%' 
and C_NUM_KT like  '99999%'
and cast(C_DATE_PROV as date)  = CURRENT_DATE - 1

union all

select 
    substring(LEFT(C_NAZN , position('от' in C_NAZN)-2),position('№' in C_NAZN)+2) as number,
    cast('1900-01-01' as date) as start_date,
    cast(C_DATE_PROV as date) as end_date
from MAIN_DOCUM
where 1=1
and C_DATE_PROV > '2024-07-23'
and C_NUM_DT like   '99999%'
and C_NUM_KT like  '90909%'
and cast(C_DATE_PROV as date)  = CURRENT_DATE - 1
)

-- выводим расхождения в распоряжениях между системами
, full_autofund_autopop as(
select 
    idempotency_token,
    number,
    afp.start_date as ftc_start_date,
    md.start_date as abs_start_date,
    afp.end_date as ftc_end_date,
    md.end_date as abs_end_date
from autofund_autopop_2 as afp
    full join main_docum_2 as md
    on 1=1
    and afp.idempotency_token = md.number
    and afp.start_date = md.start_date
    and  afp.end_date  =  md.end_date )

select 
    coalesce(af.idempotency_token, number) as number,
    bd.buid,
    nullif(ftc_start_date,'1900-01-01') as ftc_start_date,
    nullif(abs_start_date,'1900-01-01') as abs_start_date,
    nullif(ftc_end_date,'2100-12-31') as ftc_end_date,
    nullif(abs_end_date,'2100-12-31') as abs_end_date
from full_autofund_autopop af
    left join autofund_autopop_buid as bd
    on 1=1
        and coalesce(af.idempotency_token, af.number) = bd.idempotency_token
where 1=1
    and (af.idempotency_token is null or number is null)