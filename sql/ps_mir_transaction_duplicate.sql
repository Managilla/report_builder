with main_docum as(
    select * from (
            select *,
                row_number() over (partition by id order by txn_ts desc, txn_id desc) as rn
            from main_docum_inc
            where dt>CURRENT_DATE-30
            ) t
        where t.rn = 1
)
, t1 as (
select 
       t.banking_date,
       tt.name as ttype,
       t.settl_amount as amount, 
       t.ret_ref_number, 
       t.trace_ref_number,
       t.acq_ref_number,
       CONCAT_WS('|', collect_list(t.root_txn_id)) as root_txn_id --!!1126
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
   and date(system_date) = '{DT}' --current_date-1
  group by t.acq_ref_number,t.trace_ref_number,t.ret_ref_number,tt.name,t.settl_amount,t.banking_date
    having count(1) > 1
)

,t2 as (
    select 
       date(t.banking_date) as banking_date,
       tt.name as ttype,
       case 
          when t.txn_direction in ('R','A') then -1 * rcvr_billing_amount 
          else rcvr_billing_amount 
       end as amount,
       ret_ref_number,
       t.trace_ref_number
   from bo_txn t
   join bo_txn_type tt on t.txn_type_id = tt.id
   where t.attributes like '%FAKE_CLEAR%'
    and t.txn_direction in ('O','A')
    and not exists (select 1 from bo_txn t2 where t2.ret_ref_number = t.ret_ref_number and t2.rcvr_accessor_id = t.rcvr_accessor_id and t2.id <> t.id and t2.txn_class = 'F' and t2.txn_direction = t.txn_direction and t2.attributes not like '%FAKE_CLEAR%')
    and not exists (select 1 from bo_txn t2 where t2.ret_ref_number = t.ret_ref_number and t2.rcvr_accessor_id = t.rcvr_accessor_id and t2.id <> t.id and t2.txn_direction = 'R')
    and date(system_date) = '{DT}' --current_date - 1
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
    group by t.trace_ref_number,t.ret_ref_number,tt.name,t.rcvr_billing_amount,t.banking_date,t.txn_direction
    having count(1) > 1
)
,t3 as (
select 
    date(to_timestamp(cast(left(created_at,10) as int))) as banking_date, 
    tx_type as ttype, 
    billing_amount as amount, 
    rrn as ret_ref_number, 
    trace_reference_number as trace_ref_number
from `authorization_transactions` 
where status IN ('ACCEPTED', 'CLEARED', 'ADJUSTED', 'REVERSED', 'EXPIRED', 'DROPPED_MANUALLY')
    and date(to_timestamp(cast(left(created_at,10) as int))) = '{DT}' --current_date-1
    and group_id NOT IN (
        SELECT group_id FROM `financial_transactions` WHERE date(created_at) = '{DT}'--current_date-1
        )
group by trace_reference_number,rrn,tx_type,billing_amount,date(to_timestamp(cast(left(created_at,10) as int)))
having count(1) > 1
)
,t4 as (
select 
    t.txn_date as banking_date, 
    tt.name as ttype, 
    t.settl_amount as amount, 
    t.ret_ref_number, 
    t.trace_ref_number,
    t.acq_ref_number
from cards_bo_txn t
    join cards_bo_txn_type tt on t.txn_type_id = tt.id 
    where date(t.system_date) = '{DT}'--current_date - 1
    and t.rcvr_rt_system_id in (select id from cards_rt_system where code in ('atpiAdapter'))
    and t.txn_class = 'F'
    and t.txn_category = 'A'
    and tt.code not in ('pinChange','pinSet','balanceInquiry')
group by  t.acq_ref_number,t.trace_ref_number,t.ret_ref_number,tt.name,t.settl_amount,t.txn_date
having count(1) > 1
)
,t5 as (
select
    c_date_prov as banking_date,
    c_sum as amount,
    split(split(C_NAZN,'транзакции ')[1],' от')[0] as ret_ref_number,
    c_nazn as abs_nazn
from main_docum m
where 1=1
and date(c_date_prov)=current_date-1
and --1222 правки счетов
((c_num_kt in ('30232810500000000004') 
        and not (c_num_dt in('30102810545250000677','30232810100000000006') or c_num_dt like '455%' or c_num_dt like '407%' or c_num_dt like '40802%')) --3101 убрали 70606 
        or
        (c_num_dt in ('30233810800000000004')
        and not (c_num_kt in ('30102810545250000677') or c_num_kt like '70601%' or c_num_kt like '455%' or c_num_kt like '30232%' or c_num_kt like '407%' or c_num_kt like '40802%'))
        )
group by  c_date_prov,c_sum,c_nazn
having count(1) > 1
)

select 
    'clearing_cards' as report_name,
    t1.banking_date,
    t1.ttype,
    t1.amount,
    t1.ret_ref_number,
    t1.trace_ref_number,
    t1.acq_ref_number,
    NULL as abs_nazn,
    t1.root_txn_id --!!1126
from t1
union all
select 
    'fake_clears_cards' as report_name,
    t2.banking_date,
    t2.ttype,
    t2.amount,
    t2.ret_ref_number,
    t2.trace_ref_number,
    NULL as acq_ref_number,
    NULL as abs_nazn,
    NULL as root_txn_id --!!1126
from t2
union all
select 
    'fake_clears_credit_cards' as report_name,
    t3.banking_date,
    t3.ttype,
    t3.amount,
    t3.ret_ref_number,
    t3.trace_ref_number,
    NULL as acq_ref_number,
    NULL as abs_nazn,
    NULL as root_txn_id --!!1126
from t3
union all
select 
    'clearing_credit_cards' as report_name,
    t4.banking_date,
    t4.ttype,
    t4.amount,
    t4.ret_ref_number,
    t4.trace_ref_number,
    t4.acq_ref_number,
    NULL as abs_nazn,
    NULL as root_txn_id --!!1126
from t4
union all
select 
    'abs_main_docum' as report_name,
    t5.banking_date,
    NULL as ttype,
    t5.amount,
    t5.ret_ref_number,
    NULL as trace_ref_number,
    NULL as acq_ref_number,
    t5.abs_nazn,
    NULL as root_txn_id --!!1126
from t5