with ttypes as (
select 'credits' as ttype union all
select 'debits' as ttype union all 
select 'fee' as ttype union all
select 'reimb' as ttype union all
select 'disp' as ttype union all
select 'disp_cr' as ttype union all
select 'debt_bank_end' as ttype union all 
select 'debt_client_end' as ttype union all
select 'debt_bank_start' as ttype union all
select 'debt_client_start' as ttype union all
select 'fixloan' as ttype
),
agrs as (select distinct agr_number
        from bo_agreement 
         where (closing_date is null or closing_date >= to_date(date_trunc('month', current_date)) -- to_date('2025-03-31') + 1
         )
         and agreement_class = 'A'
         union all
         select distinct agr_number
        from bo_agreement_settl
         where (closing_date is null or closing_date >= to_date(date_trunc('month', current_date)) -- to_date('2025-03-31') + 1
         )
         and agreement_class = 'A'
         union all
         select distinct agr_number
        from bo_agreement_cards 
         where (closing_date is null or closing_date >= to_date(date_trunc('month', current_date)) -- to_date('2025-03-31') + 1
         )
         and agreement_class = 'A'
         ),
act_data_united as (
    select * from act_data
    union all
    select * from act_data_settl
    union all
    select * from act_data_cards
    )
, act_data_result as(
select t.ttype, agrs.agr_number, coalesce(txn_amount, 0.00) as txn_amount, to_date(date_trunc('month', current_date)) as dt
from ttypes t
cross join agrs
left join act_data_united a on t.ttype = a.ttype and a.agr_number = agrs.agr_number
)
, final as(

select f.agr_number,
    merch.agr_date,
    merch.director,
    merch.merchant,
    merch.director_short,
    round(sum(case when ttype = 'debt_bank_start' then txn_amount else 0 end), 2) as solar_start_debt_bank_amount,
    round(sum(case when ttype = 'debt_client_start' then txn_amount else 0 end), 2) as solar_start_debt_client_amount,
    round(sum(case when ttype = 'debits' then txn_amount else 0 end), 2) as solar_purchases_amount,
    round(sum(case when ttype = 'credits' then txn_amount else 0 end), 2) as solar_refunds_amount,
    round(sum(case when ttype = 'disp' then txn_amount else 0 end), 2) as solar_dispute_debit_amount,
    round(sum(case when ttype = 'disp_cr' then txn_amount else 0 end), 2) as solar_dispute_credit_amount,
    round(sum(case when ttype = 'fee' then txn_amount else 0 end), 2) as solar_fee_amount,
    round(sum(case when ttype = 'reimb' then txn_amount else 0 end), 2) as solar_transfer_amount,
    round(sum(case when ttype = 'fixloan' then txn_amount else 0 end), 2) as solar_fix_loan_amount,
    round(sum(case when ttype = 'debt_client_end' then txn_amount else 0 end), 2) as solar_end_debt_client_amount,
    round(sum(case when ttype = 'debt_bank_end' then txn_amount else 0 end), 2) as solar_end_debt_bank_amount
from merchant_agreement merch
    left join act_data_result f on f.agr_number = merch.agr_number
group by f.agr_number,
    merch.agr_date,
    merch.director,
    merch.merchant,
    merch.director_short
)
select f.*,
    solar_start_debt_bank_amount+solar_purchases_amount+solar_dispute_credit_amount+solar_fix_loan_amount+solar_end_debt_client_amount
    -solar_end_debt_bank_amount-solar_refunds_amount-solar_dispute_debit_amount-solar_fee_amount-solar_transfer_amount-solar_start_debt_client_amount as check_agr_amount,
    case when (solar_start_debt_bank_amount+solar_purchases_amount+solar_dispute_credit_amount+solar_fix_loan_amount+solar_end_debt_client_amount
    -solar_end_debt_bank_amount-solar_refunds_amount-solar_dispute_debit_amount-solar_fee_amount-solar_transfer_amount-solar_start_debt_client_amount) = 0 then 1 else 0 end as condition
from final f