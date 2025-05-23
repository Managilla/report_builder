with last_balance_end as (
    select distinct b.account_id, last_value(b.end_balance_amount) over(partition by b.account_id order by b.banking_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as debt_end 
    from acc_trial_balance b
    where b.banking_date <= to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
)
, last_balance_start as (
    select distinct b.account_id, last_value(b.end_balance_amount) over(partition by b.account_id order by b.banking_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as debt_end 
    from acc_trial_balance b
    where b.banking_date <= to_date(date_trunc('month', current_date - interval '1 month'))-1 -- to_date('2025-03-01')-1
)
, last_balance_end_settl as (
    select distinct b.account_id, last_value(b.end_balance_amount) over(partition by b.account_id order by b.banking_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as debt_end 
    from acc_trial_balance_settl b
    where b.banking_date <= to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
)
, last_balance_start_settl as (
    select distinct b.account_id, last_value(b.end_balance_amount) over(partition by b.account_id order by b.banking_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as debt_end 
    from acc_trial_balance_settl b
    where b.banking_date <= to_date(date_trunc('month', current_date - interval '1 month'))-1 --to_date('2025-03-01')-1
)
, last_balance_end_cards as (
    select distinct b.account_id, last_value(b.end_balance_amount) over(partition by b.account_id order by b.banking_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as debt_end 
    from acc_trial_balance_cards b
    where b.banking_date <= to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
)
, last_balance_start_cards as (
    select distinct b.account_id, last_value(b.end_balance_amount) over(partition by b.account_id order by b.banking_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as debt_end 
    from acc_trial_balance_cards b
    where b.banking_date <= to_date(date_trunc('month', current_date - interval '1 month')) -1 -- to_date('2025-03-01')-1
)
, solar_debt as (
    select  account_number as acc_s, agr_number, 
    case when account_number like '30232%' then 'Задолженность банка на конец'
        when account_number like '30233%' then 'Задолженность клиента на конец' end as ttype,
    coalesce(debt_end, 0) as solar_amt
    from acc_account a 
    join bo_agreement agr on agr.id = a.agreement_id
    left join last_balance_end b on a.id = b.account_id
    where a.audit_state = 'A'
    and a.category <> 'S'
    and a.agreement_id is not null
    and agr_number <> 'SBP_PVT'
    union all
    select  account_number as acc_s, agr_number, 
    case when account_number like '30232%' then 'Задолженность банка на начало'
        when account_number like '30233%' then 'Задолженность клиента на начало' end as ttype,
    coalesce(debt_end, 0) as solar_amt
    from acc_account a 
    join bo_agreement agr on agr.id = a.agreement_id
    left join last_balance_start b on a.id = b.account_id
    where a.audit_state = 'A'
    and a.category <> 'S'
    and a.agreement_id is not null
    and agr_number <> 'SBP_PVT'
    union all
    select  account_number as acc_s, agr_number, 
    case when account_number like '30232%' then 'Задолженность банка на конец'
        when account_number like '30233%' then 'Задолженность клиента на конец' end as ttype,
    coalesce(debt_end, 0) as solar_amt
    from acc_account_settl a 
    join bo_agreement_settl agr on agr.id = a.agreement_id
    left join last_balance_end_settl b on a.id = b.account_id
    where a.audit_state = 'A'
    and a.category <> 'S'
    and a.agreement_id is not null
    and agr_number <> 'SBP_PVT'
    union all
    select  account_number as acc_s, agr_number, 
    case when account_number like '30232%' then 'Задолженность банка на начало'
        when account_number like '30233%' then 'Задолженность клиента на начало' end as ttype,
    coalesce(debt_end, 0) as solar_amt
    from acc_account_settl a 
    join bo_agreement_settl agr on agr.id = a.agreement_id
    left join last_balance_start_settl b on a.id = b.account_id
    where a.audit_state = 'A'
    and a.category <> 'S'
    and a.agreement_id is not null
    and agr_number <> 'SBP_PVT'
    union all
    select  account_number as acc_s, agr_number, 
    case when account_number like '30232%' then 'Задолженность банка на конец'
        when account_number like '30233%' then 'Задолженность клиента на конец' end as ttype,
    coalesce(debt_end, 0) as solar_amt
    from acc_account_cards a 
    join bo_agreement_cards agr on agr.id = a.agreement_id
    left join last_balance_end_cards b on a.id = b.account_id
    where a.audit_state = 'A'
    and a.category <> 'S'
    and a.agreement_id is not null
    and agr_number <> 'SBP_PVT'
    union all
    select  account_number as acc_s, agr_number, 
    case when account_number like '30232%' then 'Задолженность банка на начало'
        when account_number like '30233%' then 'Задолженность клиента на начало' end as ttype,
    coalesce(debt_end, 0) as solar_amt
    from acc_account_cards a 
    join bo_agreement_cards agr on agr.id = a.agreement_id
    left join last_balance_start_cards b on a.id = b.account_id
    where a.audit_state = 'A'
    and a.category <> 'S'
    and a.agreement_id is not null
    and agr_number <> 'SBP_PVT'
)
, rec_max_dates as (
select COLLECTION_ID, max(C_DATE) as max_dt
    from records
    where C_DATE < to_date(date_trunc('month', current_date)) --to_date('2025-03-31')+1
    group by COLLECTION_ID
),
rec_max_ids as (
select r.COLLECTION_ID, max(r.ID) as max_id
 from records r
 join rec_max_dates on rec_max_dates.COLLECTION_ID = r.COLLECTION_ID and rec_max_dates.max_dt = r.C_DATE
 where r.C_DATE < to_date(date_trunc('month', current_date)) --to_date('2025-03-31')+1
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
, abs_debt as(
    select acc.C_MAIN_V_ID as acc_a, 
    case when acc.C_MAIN_V_ID like '30232%' then 'Задолженность банка на конец'
        when acc.C_MAIN_V_ID like '30233%' then 'Задолженность клиента на конец' end as ttype,
    sum(C_START_SUM + ( case when C_DT = 1 then -1*C_SUMMA else C_SUMMA end)) as abs_amt
    from records r
    join ac_fin acc on r.COLLECTION_ID = acc.C_ARC_MOVE
    where 1=1
    and r.ID in (select max_id from rec_max_ids)
    and acc.C_MAIN_V_ID like '3023_810_1%'
    group by acc.C_MAIN_V_ID
    UNION ALL
    select acc.C_MAIN_V_ID as acc_a, 
    case when acc.C_MAIN_V_ID like '30232%' then 'Задолженность банка на начало'
        when acc.C_MAIN_V_ID like '30233%' then 'Задолженность клиента на начало' end as ttype,
    sum(C_START_SUM + ( case when C_DT = 1 then -1*C_SUMMA else C_SUMMA end)) as abs_amt
    from records r
    join ac_fin acc on r.COLLECTION_ID = acc.C_ARC_MOVE
    where 1=1
    and r.ID in (select max_id from rec_max_ids_start)
    and acc.C_MAIN_V_ID like '3023_810_1%'
    group by acc.C_MAIN_V_ID
)
, act_data as(
    select 
    case when tt.code in ('purchase','c2b') then 'Сумма покупок'
        when tt.code in ('refund','b2c') then 'Сумма возвратов'
        end ttype, 
    agr.agr_number,
    COALESCE(sum(case when t.txn_direction in ('R') then -1*t.txn_amount else t.txn_amount end),0) as txn_amount 
    from bo_txn t, bo_txn_type tt, bo_agreement agr
    where orig_agreement_id = agr.id 
    and t.txn_type_id = tt.id
    and txn_class in ('F')
    and t.id in (select txn_id from inv_invoice_entry iie where iie.invoice_item_id  in 
                (select id from inv_invoice_item iii where iii.invoice_id in 
                (select id from inv_invoice ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) --('2025-03-01')
                and to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
                and ii.agreement_id in (select id from bo_agreement where (closing_date is null or closing_date >= to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
                ) and agreement_class = 'A')
            )))
    and tt.code not in ('purchaseReimb','negativeReimb')
    group by ttype, agr.agr_number
    union all
    select 'Комиссионное вознаграждение' as ttype, agr.agr_number, sum(case when f.txn_direction = 'R' then -1*f.fee_amount else f.fee_amount end) as fee_amount
    from bo_txn t  
    left outer join bo_fee_txn f on t.id = f.txn_id
    inner join bo_txn_type tt on t.txn_type_id = tt.id 
    inner join bo_agreement agr on orig_agreement_id = agr.id
    where txn_class = 'F'
    and tt.code not in ('purchaseReimb','negativeReimb')
    and t.id in (select txn_id from inv_invoice_entry iie where iie.invoice_item_id  in 
                (select id from inv_invoice_item iii where iii.invoice_id in 
                (select id from inv_invoice ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
                and to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
                and ii.agreement_id in (select id from bo_agreement where (closing_date is null or closing_date >= to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
                ) and agreement_class = 'A')
        )))
    group by ttype, agr.agr_number
    union all
    select 'Сумма опротестований (списания)' as ttype, agr.agr_number, sum(t.txn_amount) as txn_amount
    from bo_txn t  
    inner join bo_txn_type tt on t.txn_type_id = tt.id 
    inner join bo_agreement agr on rcvr_agreement_id = agr.id
    where txn_class = 'F'
    and tt.code  in ('C2BDISPUTE_DEBIT','adjDebit')
    and t.id in (select txn_id from inv_invoice_entry iie where iie.invoice_item_id  in 
                (select id from inv_invoice_item iii where iii.invoice_id in 
                (select id from inv_invoice ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) -- to_date('2025-03-01')
                and to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
            )))
    group by ttype, agr.agr_number
    union all
    select 'Сумма опротестований (зачисления)' as ttype, agr.agr_number, sum(t.txn_amount) as txn_amount
    from bo_txn t  
    inner join bo_txn_type tt on t.txn_type_id = tt.id 
    inner join bo_agreement agr on rcvr_agreement_id = agr.id
    where txn_class = 'F'
    and tt.code  in ('C2BDISPUTE_CREDIT','adjCredit')
    and t.id in (select txn_id from inv_invoice_entry iie where iie.invoice_item_id  in 
                (select id from inv_invoice_item iii where iii.invoice_id in 
                (select id from inv_invoice ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) -- to_date('2025-03-01')
                and to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
            )))
    group by ttype, agr.agr_number
    union all
    select 
    case when tt.code in ('purchase','c2b') then 'Сумма покупок'
        when tt.code in ('refund','b2c') then 'Сумма возвратов'
        end ttype, 
    agr.agr_number,
    COALESCE(sum(case when t.txn_direction in ('R') then -1*t.txn_amount else t.txn_amount end),0) as txn_amount 
    from bo_txn_settl t, bo_txn_type_settl tt, bo_agreement_settl agr
    where orig_agreement_id = agr.id 
    and t.txn_type_id = tt.id
    and txn_class in ('F')
    and t.id in (select txn_id from inv_invoice_entry_settl iie where iie.invoice_item_id  in 
                (select id from inv_invoice_item_settl iii where iii.invoice_id in 
                (select id from inv_invoice_settl ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) --('2025-03-01')
                and to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
                and ii.agreement_id in (select id from bo_agreement_settl where (closing_date is null or closing_date >= to_date(date_trunc('month', current_date - interval '1 month')) -- to_date('2025-03-01')
                ) and agreement_class = 'A')
            )))
    and tt.code not in ('purchaseReimb','negativeReimb')
    group by ttype, agr.agr_number
    union all
    select 'Комиссионное вознаграждение' as ttype, agr.agr_number, sum(case when f.txn_direction = 'R' then -1*f.fee_amount else f.fee_amount end) as fee_amount
    from bo_txn_settl t  
    left outer join bo_fee_txn_settl f on t.id = f.txn_id
    inner join bo_txn_type_settl tt on t.txn_type_id = tt.id 
    inner join bo_agreement_settl agr on orig_agreement_id = agr.id
    where txn_class = 'F'
    and tt.code not in ('purchaseReimb','negativeReimb')
    and t.id in (select txn_id from inv_invoice_entry_settl iie where iie.invoice_item_id  in 
                (select id from inv_invoice_item_settl iii where iii.invoice_id in 
                (select id from inv_invoice_settl ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) -- to_date('2025-03-01')
                and to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
                and ii.agreement_id in (select id from bo_agreement_settl where (closing_date is null or closing_date >= to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
                ) and agreement_class = 'A')
        )))
    group by ttype, agr.agr_number
    union all
    select 'Сумма опротестований (списания)' as ttype, agr.agr_number, sum(t.txn_amount) as txn_amount
    from bo_txn_settl t  
    inner join bo_txn_type_settl tt on t.txn_type_id = tt.id 
    inner join bo_agreement_settl agr on rcvr_agreement_id = agr.id
    where txn_class = 'F'
    and tt.code  in ('C2BDISPUTE_DEBIT','adjDebit')
    and t.id in (select txn_id from inv_invoice_entry_settl iie where iie.invoice_item_id  in 
                (select id from inv_invoice_item_settl iii where iii.invoice_id in 
                (select id from inv_invoice_settl ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
                and to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
        )))
    group by ttype, agr.agr_number
    union all
    select 'Сумма опротестований (зачисления)' as ttype, agr.agr_number, sum(t.txn_amount) as txn_amount
    from bo_txn_settl t  
    inner join bo_txn_type_settl tt on t.txn_type_id = tt.id 
    inner join bo_agreement_settl agr on rcvr_agreement_id = agr.id
    where txn_class = 'F'
    and tt.code  in ('C2BDISPUTE_CREDIT','adjCredit')
    and t.id in (select txn_id from inv_invoice_entry_settl iie where iie.invoice_item_id  in 
                (select id from inv_invoice_item_settl iii where iii.invoice_id in 
                (select id from inv_invoice_settl ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
                and to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
        )))
    group by ttype, agr.agr_number
    union all
    select 
    case when tt.code in ('purchase','c2b') then 'Сумма покупок'
        when tt.code in ('refund','b2c') then 'Сумма возвратов'
        end ttype, 
    agr.agr_number,
    COALESCE(sum(case when t.txn_direction in ('R') then -1*t.txn_amount else t.txn_amount end),0) as txn_amount 
    from bo_txn_cards t, bo_txn_type_cards tt, bo_agreement_cards agr
    where orig_agreement_id = agr.id 
    and t.txn_type_id = tt.id
    and txn_class in ('F')
    and t.id in (select txn_id from inv_invoice_entry_cards iie where iie.invoice_item_id  in 
                (select id from inv_invoice_item_cards iii where iii.invoice_id in 
                (select id from inv_invoice_cards ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) -- ('2025-03-01')
                and to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
                and ii.agreement_id in (select id from bo_agreement_cards where (closing_date is null or closing_date >= to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
                ) and agreement_class = 'A')
        )))
    and tt.code not in ('purchaseReimb','negativeReimb')
    group by ttype, agr.agr_number
    union all
    select 'Комиссионное вознаграждение' as ttype, agr.agr_number, sum(case when f.txn_direction = 'R' then -1*f.fee_amount else f.fee_amount end) as fee_amount
    from bo_txn_cards t  
    left outer join bo_fee_txn_cards f on t.id = f.txn_id
    inner join bo_txn_type_cards tt on t.txn_type_id = tt.id 
    inner join bo_agreement_cards agr on orig_agreement_id = agr.id
    where txn_class = 'F'
    and tt.code not in ('purchaseReimb','negativeReimb')
    and t.id in (select txn_id from inv_invoice_entry_cards iie where iie.invoice_item_id  in 
                (select id from inv_invoice_item_cards iii where iii.invoice_id in 
                (select id from inv_invoice_cards ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
                and to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
                and ii.agreement_id in (select id from bo_agreement_cards where (closing_date is null or closing_date >= to_date(date_trunc('month', current_date - interval '1 month')) -- to_date('2025-03-01')
                ) and agreement_class = 'A')
    )))
    group by ttype, agr.agr_number
    union all
    select 'Сумма опротестований (списания)' as ttype, agr.agr_number, sum(t.txn_amount) as txn_amount
    from bo_txn_cards t  
    inner join bo_txn_type_cards tt on t.txn_type_id = tt.id 
    inner join bo_agreement_cards agr on rcvr_agreement_id = agr.id
    where txn_class = 'F'
    and tt.code  in ('C2BDISPUTE_DEBIT','adjDebit')
    and t.id in (select txn_id from inv_invoice_entry_cards iie where iie.invoice_item_id  in 
                (select id from inv_invoice_item_cards iii where iii.invoice_id in 
                (select id from inv_invoice_cards ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) -- to_date('2025-03-01')
                and to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
    )))
    group by ttype, agr.agr_number
    union all
    select 'Сумма опротестований (зачисления)' as ttype, agr.agr_number, sum(t.txn_amount) as txn_amount
    from bo_txn_cards t  
    inner join bo_txn_type_cards tt on t.txn_type_id = tt.id 
    inner join bo_agreement_cards agr on rcvr_agreement_id = agr.id
    where txn_class = 'F'
    and tt.code  in ('C2BDISPUTE_CREDIT','adjCredit')
    and t.id in (select txn_id from inv_invoice_entry_cards iie where iie.invoice_item_id  in 
                (select id from inv_invoice_item_cards iii where iii.invoice_id in 
                (select id from inv_invoice_cards ii where opening_date between to_date(date_trunc('month', current_date - interval '1 month')) -- to_date('2025-03-01')
                and to_date(date_trunc('month', current_date) - interval '1 day') --to_date('2025-03-31')
    )))
    group by ttype, agr.agr_number
    )
, ttypes as (
        select 'Сумма возвратов' as ttype union all
        select 'Сумма покупок' as ttype union all 
        select 'Комиссионное вознаграждение' as ttype union all
        select 'Сумма опротестований (списания)' as ttype union all 
        select 'Сумма опротестований (зачисления)' as ttype union all 
        select 'Сумма Adjustment Credit' as ttype union all
        select 'Сумма Adjustment Debit' as ttype
    )
, agrs as (
        select distinct agr_number, agr.id, a_plus.account_number as acc_plus, a_minus.account_number as acc_minus 
        from bo_agreement agr
        left join acc_account a_plus on a_plus.agreement_id = agr.id and a_plus.account_number like '30232%' /*and a_plus.status = 'A'*/ and a_plus.audit_state <> 'R'
        left join acc_account a_minus on a_minus.agreement_id = agr.id and a_minus.account_number like '30233%' /*and a_minus.status = 'A'*/ and a_minus.audit_state <> 'R'
        where (agr.closing_date is null or agr.closing_date >= to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
        )
        and agreement_class = 'A'
        union all
        select distinct agr_number, agr.id, a_plus.account_number as acc_plus, a_minus.account_number as acc_minus 
        from bo_agreement_settl agr
        left join acc_account_settl a_plus on a_plus.agreement_id = agr.id and a_plus.account_number like '30232%' /*and a_plus.status = 'A'*/ and a_plus.audit_state <> 'R'
        left join acc_account_settl a_minus on a_minus.agreement_id = agr.id and a_minus.account_number like '30233%' /*and a_minus.status = 'A'*/ and a_minus.audit_state <> 'R'
        where (agr.closing_date is null or agr.closing_date >= to_date(date_trunc('month', current_date - interval '1 month'))--to_date('2025-03-01')
        )
        and agreement_class = 'A'
        union all
        select distinct agr_number, agr.id, a_plus.account_number as acc_plus, a_minus.account_number as acc_minus 
        from bo_agreement_cards agr
        left join acc_account_cards a_plus on a_plus.agreement_id = agr.id and a_plus.account_number like '30232%' /*and a_plus.status = 'A'*/ and a_plus.audit_state <> 'R'
        left join acc_account_cards a_minus on a_minus.agreement_id = agr.id and a_minus.account_number like '30233%' /*and a_minus.status = 'A'*/ and a_minus.audit_state <> 'R'
        where (agr.closing_date is null or agr.closing_date >= to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
        )
        and agreement_class = 'A'
    )
, act_data_solar as (
    select t.ttype,agrs.agr_number,coalesce(txn_amount, 0.00) as txn_amount, to_date(date_trunc('month', current_date)) as dt, 
    case when t.ttype in ('Сумма покупок','Сумма перечисленных средств','Сумма опротестований (зачисления)','Сумма Adjustment Credit') then agrs.acc_plus 
        when t.ttype in ('Сумма возвратов','Комиссионное вознаграждение','Сумма опротестований (списания)','Сумма Adjustment Debit') then agrs.acc_minus end as acc
    from ttypes t
    cross join agrs
    left join act_data a on t.ttype = a.ttype and a.agr_number = agrs.agr_number
)
, main_docum_ as (
    select md.*, "C_KL_DT#2#1" as C_KL_DT, "C_KL_KT#2#1" as C_KL_KT
    from main_docum md
    where C_DATE_PROV >= to_date(date_trunc('month', current_date - interval '1 month')) and C_DATE_PROV < to_date(date_trunc('month', current_date) - interval '1 day') + 1
)
, act_data_abs as (
    select 'Сумма покупок' as ttype, C_NUM_KT as acc, sum(C_SUM) as amt
    from MAIN_DOCUM_
    where STATE_ID = 'PROV'
    and C_NUM_KT like '30232810_1%'
    and (C_NAZN like 'Сформирован при обработке транзакции%' or C_NAZN like 'Перевод СБП%')
    and C_NAZN not like '%(Adjustment Credit)'
    and C_DATE_PROV >= to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
        and C_DATE_PROV < to_date(date_trunc('month', current_date)) --to_date('2025-03-31') + 1
    group by C_NUM_KT
    union all
    select 'Сумма возвратов' as txt, C_NUM_DT, sum(C_SUM)
    from MAIN_DOCUM_
    where STATE_ID = 'PROV'
    and C_NUM_DT like '30233810_1%'
    and (C_NAZN like 'Сформирован при обработке транзакции%' or C_NAZN like 'Перевод СБП%')
    and C_NAZN not like '%(Adjustment Debit)'
    and C_DATE_PROV >= to_date(date_trunc('month', current_date - interval '1 month')) -- to_date('2025-03-01')
        and C_DATE_PROV < to_date(date_trunc('month', current_date)) -- to_date('2025-03-31') + 1
    group by C_NUM_DT
    union all
    select 'Комиссионное вознаграждение' as txt, 
        case when C_NUM_DT like '30233810_1%'then C_NUM_DT when C_NUM_KT like '30233810_1%' then C_NUM_KT end acc, 
        sum(case when C_NUM_DT like '30233810_1%'then C_SUM when C_NUM_KT like '30233810_1%' then -1*C_SUM end)
    from MAIN_DOCUM_
    where STATE_ID = 'PROV'
    and ( ( C_NUM_DT like '30233810_1%' and C_NUM_KT like '70601%' ) OR ( C_NUM_DT like '70601%' and C_NUM_KT like '30233810_1%' ) )
    and C_DATE_PROV >= to_date(date_trunc('month', current_date - interval '1 month')) -- to_date('2025-03-01')
        and C_DATE_PROV < to_date(date_trunc('month', current_date)) --to_date('2025-03-31') + 1
    group by acc
    union all
    select 'Сумма опротестований (списания)' as txt, C_NUM_DT, sum(C_SUM)
    from MAIN_DOCUM_
    where STATE_ID = 'PROV'
    and C_NUM_DT like '30233810_1%'
    and (C_NAZN like 'Возмещение опротестованной операции%' or C_NAZN like '%(Adjustment Debit)')
    and C_DATE_PROV >= to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
    and C_DATE_PROV < to_date(date_trunc('month', current_date))-- to_date('2025-03-31') + 1
    group by C_NUM_DT
    union all
    select 'Сумма опротестований (зачисления)' as txt, C_NUM_KT, sum(C_SUM)
    from MAIN_DOCUM_
    where STATE_ID = 'PROV'
    and C_NUM_KT like '30232810_1%'
    and C_NAZN like '%(Adjustment Credit)'
    and C_DATE_PROV >= to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
        and C_DATE_PROV < to_date(date_trunc('month', current_date)) -- to_date('2025-03-31') + 1
    group by C_NUM_KT
)
, raw_data as (
select s.ttype as ttype_s, s.agr_number, coalesce(s.txn_amount,0) as solar_amt, s.dt, s.acc as acc_s, a.ttype as ttype_a, a.acc as acc_a, coalesce(a.amt,0) as abs_amt
    from act_data_solar s
    full join act_data_abs a on a.acc = s.acc and a.ttype = s.ttype
    )
, raw_debt as (
    select s.ttype as ttype_s, s.agr_number, coalesce(s.solar_amt,0) as solar_amt, s.acc_s as acc_s, a.ttype as ttype_a, a.acc_a as acc_a, coalesce(a.abs_amt,0) as abs_amt
    from solar_debt s
    full join abs_debt a on a.acc_a = s.acc_s and a.ttype = s.ttype
    )
, closed_agreements as (
    select agr_number from bo_agreement where closing_date < to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
        union all
    select agr_number from bo_agreement_settl where closing_date < to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01')
        union all
    select agr_number from bo_agreement_cards where closing_date < to_date(date_trunc('month', current_date - interval '1 month')) --to_date('2025-03-01') 
)
    select coalesce(ttype_s, ttype_a) as ttype, agr_number, solar_amt, coalesce(acc_s, acc_a) as acc, abs_amt, solar_amt - abs_amt as diff_amt
    from raw_data
    where agr_number not in (select agr_number from closed_agreements)
    union all
    select coalesce(ttype_s, ttype_a) as ttype, agr_number, solar_amt, coalesce(acc_s, acc_a) as acc, abs_amt, solar_amt - abs_amt as diff_amt
    from raw_debt
    where agr_number not in (select agr_number from closed_agreements)
