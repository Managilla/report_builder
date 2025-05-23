with main_docum_ (
    select md.*, "C_KL_DT#2#1" as C_KL_DT, "C_KL_KT#2#1" as C_KL_KT
    from main_docum md
    where C_DATE_PROV >= to_date(date_trunc('month', current_date - interval '1 month')) and C_DATE_PROV < to_date(date_trunc('month', current_date) - interval '1 day') + 1
),
rec_max_dates as (
select COLLECTION_ID, max(C_DATE) as max_dt
 from records
 where C_DATE < to_date(date_trunc('month', current_date)) -- to_date('2025-03-31')+1
group by COLLECTION_ID
),
rec_max_ids as (
select r.COLLECTION_ID, max(r.ID) as max_id
 from records r
 join rec_max_dates on rec_max_dates.COLLECTION_ID = r.COLLECTION_ID and rec_max_dates.max_dt = r.C_DATE
 where r.C_DATE < to_date(date_trunc('month', current_date)) -- to_date('2025-03-31')+1
group by r.COLLECTION_ID
),
rec_max_dates_start as (
select COLLECTION_ID, max(C_DATE) as max_dt
 from records
 where C_DATE < to_date(date_trunc('month', current_date - interval '1 month')) -- to_date('2025-03-01')
group by COLLECTION_ID
),
rec_max_ids_start as (
select r.COLLECTION_ID, max(r.ID) as max_id
 from records r
 join rec_max_dates_start on rec_max_dates_start.COLLECTION_ID = r.COLLECTION_ID and rec_max_dates_start.max_dt = r.C_DATE
 where r.C_DATE < to_date(date_trunc('month', current_date - interval '1 month')) -- to_date('2025-03-01')
group by r.COLLECTION_ID
)

select 
case when tt.code in ('purchase','c2b') then 'debits'
     when tt.code in ('refund','b2c') then 'credits'
     end ttype, 
agr.agr_number,
COALESCE(sum(case when t.txn_direction in ('R') then -1*t.txn_amount else t.txn_amount end),0) as txn_amount 
from bo_txn_cards t, bo_txn_type_cards tt, bo_agreement_cards agr
where orig_agreement_id = agr.id 
and t.txn_type_id = tt.id
and txn_class in ('F')
and t.id in (select txn_id from inv_invoice_entry_cards iie where iie.invoice_item_id  in 
             (select id from inv_invoice_item_cards iii where iii.invoice_id in 
              (select id from inv_invoice_cards ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01') 
              and to_date(date_trunc('month', current_date) - interval '1 day') -- to_date('2025-03-31')
            )))
and tt.code not in ('purchaseReimb','negativeReimb')
group by ttype, agr.agr_number
union all
select 'fee' as ttype, agr.agr_number, sum(case when f.txn_direction = 'R' then -1*f.fee_amount else f.fee_amount end) as fee_amount
from bo_txn_cards t  
left outer join bo_fee_txn_cards f on t.id = f.txn_id
inner join bo_txn_type_cards tt on t.txn_type_id = tt.id 
inner join bo_agreement_cards agr on orig_agreement_id = agr.id
where txn_class = 'F'
and tt.code not in ('purchaseReimb','negativeReimb')
and t.id in (select txn_id from inv_invoice_entry_cards iie where iie.invoice_item_id  in 
             (select id from inv_invoice_item_cards iii where iii.invoice_id in 
              (select id from inv_invoice_cards ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01') 
              and to_date(date_trunc('month', current_date) - interval '1 day') -- to_date('2025-03-31')
      )))
group by ttype, agr.agr_number
union all
select 'reimb' as ttype, agr.agr_number, sum(C_SUM) as txn_amount 
from MAIN_DOCUM_
join acc_account_cards a on a.account_number = MAIN_DOCUM_.C_NUM_DT and a.audit_state <> 'R'
join bo_agreement_cards agr on a.agreement_id = agr.id
where STATE_ID = 'PROV'
and C_NUM_DT like '30232810_1%'
and C_DATE_PROV >= to_date(date_trunc('month', current_date - interval '1 month')) -- to_date('2025-03-01') 
and C_DATE_PROV < to_date(date_trunc('month', current_date)) -- to_date('2025-03-31') + 1
and (   C_NAZN like 'Переводы физических лиц согласно Реестру%'                       
     OR C_NAZN like 'Переводы физических лиц с использованием СБП согласно  Реестру%' 
     OR C_NAZN like 'Возмещение ден.средств по операциям эквайринга по Реестру%' -- назначение платежа у мерчантов Расчётного банка
    )
group by ttype, agr.agr_number

union all

select 'disp' as ttype, agr.agr_number, sum(t.txn_amount) as txn_amount
from bo_txn_cards t  
inner join bo_txn_type_cards tt on t.txn_type_id = tt.id 
inner join bo_agreement_cards agr on rcvr_agreement_id = agr.id
where txn_class = 'F'
and tt.code  in ('adjDebit')
and t.id in (select txn_id from inv_invoice_entry_cards iie where iie.invoice_item_id  in 
             (select id from inv_invoice_item_cards iii where iii.invoice_id in 
              (select id from inv_invoice_cards ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
              and to_date(date_trunc('month', current_date) - interval '1 day') -- to_date('2025-03-31')
)))
group by ttype, agr.agr_number

UNION ALL

select 'disp_cr' as ttype, agr.agr_number, sum(t.txn_amount) as txn_amount
from bo_txn_cards t  
inner join bo_txn_type_cards tt on t.txn_type_id = tt.id 
inner join bo_agreement_cards agr on rcvr_agreement_id = agr.id
where txn_class = 'F'
and tt.code  in ('adjCredit')
and t.id in (select txn_id from inv_invoice_entry_cards iie where iie.invoice_item_id  in 
             (select id from inv_invoice_item_cards iii where iii.invoice_id in 
              (select id from inv_invoice_cards ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) -- to_date('2025-03-01')
              and to_date(date_trunc('month', current_date) - interval '1 day') -- to_date('2025-03-31')
)))
group by ttype, agr.agr_number

UNION ALL

select case when acc.C_MAIN_V_ID like '30232%' then 'debt_bank_end'
     when acc.C_MAIN_V_ID like '30233%' then 'debt_client_end' end as ttype,
agr.agr_number,
abs(sum(C_START_SUM + ( case when C_DT = 1 then -1*C_SUMMA else C_SUMMA end))) as txn_amount
 from records r
 join ac_fin acc on r.COLLECTION_ID = acc.C_ARC_MOVE
 join acc_account_cards a on a.account_number = acc.C_MAIN_V_ID and a.audit_state <> 'R'
 join bo_agreement_cards agr on a.agreement_id = agr.id
where 1=1
and r.ID in (select max_id from rec_max_ids)
and acc.C_MAIN_V_ID like '3023_810_1%'
group by acc.C_MAIN_V_ID, agr.agr_number
UNION ALL
select case when acc.C_MAIN_V_ID like '30232%' then 'debt_bank_start'
     when acc.C_MAIN_V_ID like '30233%' then 'debt_client_start' end as ttype,
agr.agr_number,
abs(sum(C_START_SUM + ( case when C_DT = 1 then -1*C_SUMMA else C_SUMMA end))) as txn_amount
 from records r
 join ac_fin acc on r.COLLECTION_ID = acc.C_ARC_MOVE
 join acc_account_cards a on a.account_number = acc.C_MAIN_V_ID and a.audit_state <> 'R'
 join bo_agreement_cards agr on a.agreement_id = agr.id
where 1=1
and r.ID in (select max_id from rec_max_ids_start)
and acc.C_MAIN_V_ID like '3023_810_1%'
group by acc.C_MAIN_V_ID, agr.agr_number

UNION ALL

select 'fixloan' as ttype, agr.agr_number, sum(C_SUM) as txn_amount 
from MAIN_DOCUM_
left join acc_account_cards a on a.account_number = MAIN_DOCUM_.C_NUM_KT and a.audit_state <> 'R'
left join bo_agreement_cards agr on a.agreement_id = agr.id
where STATE_ID = 'PROV'
and C_NUM_DT like '4070%'
and C_NUM_KT like '30233%'
and C_NAZN like 'Исполнение нетто-требований%'
group by ttype, agr.agr_number