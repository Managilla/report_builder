with account_core_ranged as(
select 
    source__ts_ms,
    activation_date,
    cast(modified_ts as date) as ftc_status_date,
    agreement,
    buid,
    closing_date,
    number,
    opening_date,
    status,
    type,
    dbz__op,
    ROW_NUMBER() OVER (PARTITION BY number ORDER BY source__ts_ms DESC) as rn
from account_core
where
    number like '42301%'
    and type = 'BALANCE'
    and ((status in ('OPEN','CLOSING') and cast('1970-01-01' as date) + (activation_date * interval '1 day')< CURRENT_DATE)
        or status = 'CLOSED'))

,account_core_2 as (
select
    *
from account_core_ranged
where 
    rn = 1
    and dbz__op != 'd'
    and status not in ('DRAFT', 'OPEN_FAIL'))

,operations_1 as (
select
    path_3,path_5,
    direction,
    is_stornare,
    case when  is_stornare = false then 1 else -1 end as is_stornare_cf,
    case when  direction = 'CREDIT'  then 1
        when  direction = 'DEBIT'  then -1 end as direction_cf,
    get_json_object(attributes  ,'$.op_type') as txntype,
    amount,
    cast(event_at/1000000 as timestamp) as tim
from operations
where 1=1
    and  path_0 = 'fintech'
    and path_1 = 'sa_op'
    and path_2 = 'buid'
    and path_4 = 'public_agreement'
and cast(event_at/1000000 as timestamp) <  (cast((CURRENT_DATE -1) as timestamp) + (21 * interval '1 hour'))
)

,operations_2 as(
select
    buid,
    agreement,
    sum_rub/1000 as sum_rub
from
    (select
        path_3 as buid,
        path_5 as agreement,
        sum(amount *  direction_cf) as sum_rub
    from operations_1
    group by path_3,path_5) 
)

,account_core_with_balance as (
select
    ac.*,
    op.sum_rub
from account_core_2 ac
left join operations_2 op
    on(ac.agreement = op.agreement)
)

,ac_fin_2 as (
select 
    pp.C_INTERNAL_CODE,
    af.C_MAIN_V_ID,
    af.id,
    sa.C_DATE_OP,
    st.C_CODE,
    cast(st.TXN_TS as date) as cft_status_date,
    sa.C_DATE_CLOSE
from ac_fin af
inner join account_cft sa 
    on (af.id = sa.id)
inner join depn s
    on(af.id = s.c_account)
inner join vid_deposit vd
    on(s.C_VID_DOG = vd.ID and vd.C_CODE in ('YA_NS', 'YA_NO_TERM_SAVE'))
inner join product pp
    on (pp.id = s.id)
left join com_status_prd st
    on (af.C_COM_STATUS = st.id)
where 
    (st.C_CODE is null and sa.C_DATE_OP < CURRENT_DATE)
    or st.C_CODE in ('CLOSE', 'TO_CLOSE')
)

,account_balance_groupped as(
select C_MAIN_V_ID, account_balance_out_amount from (
    select
        ac.C_MAIN_V_ID,
        ROW_NUMBER() over (partition by ac.C_MAIN_V_ID order by ab.account_balance_effective_from_date desc, ab.processed_dttm desc) as rownum,
        ab.account_balance_effective_from_date,
        ab.account_balance_out_amount
    from account_balance ab
    inner join ac_fin_2 ac
        on(ab.account_nk = ac.id)
    where ab.account_balance_effective_from_date < current_date
    group by 
        ac.C_MAIN_V_ID,
        ab.account_balance_effective_from_date,
        ab.processed_dttm,
        ab.account_balance_out_amount)
    where rownum=1
)

,ac_fin_with_balance as (
select 
    pr.*, 
    num.account_balance_out_amount
from ac_fin_2 as pr
left join account_balance_groupped as num
    on pr.C_MAIN_V_ID = num.C_MAIN_V_ID
)

,common_table_1 as (
select 
    *,
    case when af.C_CODE is null then 'OPEN'
        when af.C_CODE in ('CLOSE', 'TO_CLOSE') then 'CLOSED'
        else af.C_CODE
        end as cft2ftc_status,
    (cast('1970-01-01' as date) + coalesce(activation_date, opening_date) * interval '1 day') AS ftc_activation_date,
    cast (C_DATE_OP as date) as cft_activation_date,
    (cast('1970-01-01' as date) + closing_date * interval '1 day') as ftc_closing_date,
    cast(C_DATE_CLOSE as date) as cft_closing_date,
    cast(sum_rub as decimal (10,2)) as ftc_sum,
    account_balance_out_amount as cft_sum
from ac_fin_with_balance af
full join account_core_with_balance ac
    on(af.C_MAIN_V_ID = ac.number)
)

,res_table as(
select
    case 
        when number is null 
            then 'lost_ftc'
        when C_MAIN_V_ID is null 
            then 'lost_cft'
        when (number is not null and C_MAIN_V_ID is not null and cft2ftc_status != status
            and cft_status_date < current_date -1
            and ftc_status_date < current_date -1)
            then 'dif_status'
        when (number is not null and C_MAIN_V_ID is not null and ftc_activation_date != cft_activation_date)
            then 'dif_open_date'
        when (number is not null and C_MAIN_V_ID is not null and ftc_closing_date != cft_closing_date)
            then 'dif_close_date'
        when (number is not null and C_MAIN_V_ID is not null and activation_date is null and C_DATE_OP is null)
            then 'dif_open_null'
        when (number is not null and C_MAIN_V_ID is not null and ftc_sum != cft_sum)
            then 'diff_balance'
        else null
    end as dif_type,
    coalesce(C_MAIN_V_ID, number) as account,
    C_CODE as cft_status,
    status as ftc_status,
    cft_activation_date,
    ftc_activation_date,
    cft_closing_date,
    ftc_closing_date
from common_table_1)

select 
    *
from res_table
where dif_type is not null
