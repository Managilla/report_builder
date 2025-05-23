with t1 as (
select 
       t.banking_date,
       tt.name as ttype, 
       settl_amount, 
       ret_ref_number, 
       t.acq_ref_number,
       t.trace_ref_number,
       date(t.system_date) as system_date
  from bo_txn t
  join bo_txn_type tt 
    on t.txn_type_id = tt.id and tt.code not in ('pinSet','pinChange','balanceInquiry')
  left join bo_accessor acs 
    on acs.id = t.rcvr_accessor_id 
  join bo_accessor_type bat 
    on acs.accessor_type_id = bat.id and bat.code = 'CARD'
  inner join rt_system rs 
    on t.orig_rt_system_id = rs.id
  where t.txn_class  = 'F'
   and t.attributes not like '%FAKE_CLEAR%'
   and date(system_date) = '{DT}'--current_date-1
)

,t2 as (
    select 
       date(t.banking_date) as banking_date,
       tt.name as ttype,
       case 
          when t.txn_direction in ('R','A') then -1 * rcvr_billing_amount 
          else rcvr_billing_amount 
          end as rcvr_billing_amount,
       ret_ref_number,
       t.acq_ref_number,
       t.trace_ref_number,
       date(t.system_date) as system_date
   from bo_txn t
   join bo_txn_type tt on t.txn_type_id = tt.id
   where 1=1
        and t.attributes like '%FAKE_CLEAR%'
        and t.txn_direction in ('O','A')
        and not exists (select 1 from bo_txn t2 where t2.ret_ref_number = t.ret_ref_number and t2.rcvr_accessor_id = t.rcvr_accessor_id and t2.id <> t.id and t2.txn_class = 'F' and t2.txn_direction = t.txn_direction and t2.attributes not like '%FAKE_CLEAR%')
        and not exists (select 1 from bo_txn t2 where t2.ret_ref_number = t.ret_ref_number and t2.rcvr_accessor_id = t.rcvr_accessor_id and t2.id <> t.id and t2.txn_direction = 'R')
        and date(system_date) between date_add('{DT}', -1) and '{DT}' --current_date-2 and current_date-1
        and not exists (select null
                      from bo_txn txn_rrn, 
                           bo_txn txn_trn, 
                           bo_txn_type btt2 
                     where txn_rrn.ret_ref_number = t.ret_ref_number 
                       and txn_rrn.txn_class = 'A' 
                       and txn_rrn.trace_ref_number = txn_trn.trace_ref_number 
                       and txn_trn.txn_class = 'F' 
                       and txn_trn.txn_type_id = btt2.id 
                       and btt2.code = 'finNotifCredit')
        and t.status = 'FD'
)
,t3 as (
    select 
        to_timestamp(cast(left(created_at,10) as int)) as banking_date, 
        tx_type as ttype, 
        rrn as ret_ref_number, 
        billing_amount, 
        trace_reference_number as trace_ref_number
    from `authorization_transactions` 
    where status IN ('ACCEPTED', 'CLEARED', 'ADJUSTED', 'REVERSED', 'EXPIRED', 'DROPPED_MANUALLY')
        and date(to_timestamp(cast(left(created_at,10) as int))) = '{DT}'--current_date-1
        and group_id NOT IN (
            select group_id FROM `financial_transactions` WHERE date(created_at) = '{DT}'--current_date-1
            )
)
,t4 as (
    select 
        t.txn_date as banking_date, 
        tt.name as ttype, 
        t.settl_amount, 
        t.ret_ref_number, 
        t.trace_ref_number,
        date(t.system_date) as system_date
    from cards_bo_txn t
    join cards_bo_txn_type tt on t.txn_type_id = tt.id 
    where 1=1
        and date(t.system_date) between date_add('{DT}', -1) and '{DT}'  -- current_date-2 and current_date-1
        and t.rcvr_rt_system_id in (select id from cards_rt_system where code in ('atpiAdapter'))
        and t.txn_class = 'F'
        and t.txn_category = 'A'
        and tt.code not in ('pinChange','pinSet','balanceInquiry')
)


select 
    'fake clears in clearing cards' as error_name,
    t1.banking_date,
    t1.trace_ref_number,
    t1.ret_ref_number,
    t1.ttype,
    t1.settl_amount as amount
from t1
left join t2 on t1.trace_ref_number=t2.trace_ref_number and t2.banking_date = '{DT}'--current_date-1
    and t1.ret_ref_number=t2.ret_ref_number
    and t1.ttype=t2.ttype
    and t1.settl_amount=t2.rcvr_billing_amount
    and t1.banking_date=t2.banking_date
where 1=1
    and t2.ret_ref_number is not null
union all
select 
    'fake clears in clearing credit cards' as error_name,
    t4.banking_date,
    t4.trace_ref_number,
    t4.ret_ref_number,
    t4.ttype,
    t4.settl_amount as amount
from t4
left join t3 on t4.trace_ref_number=t3.trace_ref_number and t3.banking_date = '{DT}' --current_date-1
    and t4.ret_ref_number=t3.ret_ref_number
    and t4.ttype=t3.ttype
    and t4.settl_amount=t3.billing_amount
    and t4.banking_date=t3.banking_date
where 1=1
    and t3.ret_ref_number is not null
union all
select 
    'fake clears t-2 not in clearing cards' as error_name,
    t2.banking_date,
    t2.trace_ref_number,
    t2.ret_ref_number,
    t2.ttype,
    t2.rcvr_billing_amount as amount
from t2 
left join t1 on t1.trace_ref_number=t2.trace_ref_number 
    and t1.ret_ref_number=t2.ret_ref_number
    and t1.ttype=t2.ttype
    and t1.settl_amount=t2.rcvr_billing_amount
    and t1.banking_date=t2.banking_date
where 1=1
    and t1.ret_ref_number is null
    and t2.system_date = date_add('{DT}', -1) --current_date-2
union all
select 
    'fake clears t-2 not in clearing credit cards' as error_name,
    t3.banking_date,
    t3.trace_ref_number,
    t3.ret_ref_number,
    t3.ttype,
    t3.billing_amount as amount
from t3 
left join t4 on t4.trace_ref_number=t3.trace_ref_number 
    and t4.ret_ref_number=t3.ret_ref_number
    and t4.ttype=t3.ttype
    and t4.settl_amount=t3.billing_amount
    and t4.banking_date=t3.banking_date
where 1=1
    and t4.ret_ref_number is null
    and t3.banking_date = date_add('{DT}', -1) --current_date-2
union all
select 
    'fake clears t-2 in fake clears t-1 credit cards' as error_name,
    t3.banking_date,
    t3.trace_ref_number,
    t3.ret_ref_number,
    t3.ttype,
    t3.billing_amount as amount
from t3 
left join t4 on t4.trace_ref_number=t3.trace_ref_number 
    and t4.ret_ref_number=t3.ret_ref_number
    and t4.ttype=t3.ttype
    and t4.settl_amount=t3.billing_amount
left join t3 t3_2 on t3_2.trace_ref_number=t3.trace_ref_number 
    and t3_2.ret_ref_number=t3.ret_ref_number
    and t3_2.ttype=t3.ttype
    and t3_2.billing_amount=t3.billing_amount
    and t3_2.banking_date = '{DT}'--current_date-1
where 1=1
    and t4.ret_ref_number is not null
    and t3.banking_date = date_add('{DT}', -1) --current_date-2
    and t3_2.ret_ref_number is not null