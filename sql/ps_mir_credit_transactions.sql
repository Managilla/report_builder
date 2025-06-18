with main_docum as(
    select * from (
            select *,
                row_number() over (partition by id order by txn_ts desc, txn_id desc) as rn
            from main_docum_inc
            where dt>CURRENT_DATE-30
            ) t
        where t.rn = 1
)
, clearing as (
    select 
        t.txn_date, 
        tt.name as ttype, 
        tt.direction, 
        case 
           when txn_direction in ('A','R') then 'Reversal'
           when txn_direction in ('O') then 'Original'
           else 'Other'
        end as t_direction,
        t.settl_amount, 
        sum(settl_amount) over (partition by ret_ref_number,ba.agr_number,tt.name) as settl_amount_rrn, --3101 name
        t.ret_ref_number, 
        t.acq_ref_number,
        t.trace_ref_number,
        ba.agr_number,
        case when tt.name in ('Withdrawal', 'Debit', 'Purchase', 'Purchase With Cashback')
               and t.txn_direction = 'O'
           then 'case1'
           when (tt.name in ('Cash In', 'Credit', 'Refund', 'Financial Notification Credit') 
               and t.txn_direction = 'O')
               or (tt.name in ('Debit', 'Purchase') 
               and txn_direction in ('A','R'))
           then 'case2'
           end as check_case
    from cards_bo_txn t
    join cards_bo_txn_type tt on t.txn_type_id = tt.id 
    join cards_bo_agreement ba on t.rcvr_agreement_id = ba.id
    where 1=1
        and date(system_date) = '{DT}' --current_date-1
        and t.rcvr_rt_system_id in (select id from cards_rt_system where code in ('atpiAdapter'))
        and t.txn_class = 'F'
        and t.txn_category = 'A'
        and tt.code not in ('pinChange','pinSet','balanceInquiry')
)
, fake_clears as (
    select
        to_timestamp(cast(left(created_at,10) as int)) as txn_date, 
        tx_type as ttype,
        tx_sign, 
        rrn as ret_ref_number,
        billing_amount as amount, 
        sum(billing_amount) over 
        (partition by rrn,tx_type,get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(original_request,'$.body'),'$.model'),'$.solarData'),'$.customData'),'$.agreementNumber'))
        as amount_rrn, --3101 type
        trace_reference_number as trace_ref_number,
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(original_request,'$.body'),'$.model'),'$.solarData'),'$.customData'),'$.agreementNumber') as agr_number
    from `authorization_transactions` 
    where 1=1
        and status IN ('ACCEPTED', 'CLEARED', 'ADJUSTED', 'REVERSED', 'EXPIRED', 'DROPPED_MANUALLY')
        and date(to_timestamp(cast(left(created_at,10) as int))) = '{DT}'--current_date-1
        and group_id NOT IN (
            select group_id from `financial_transactions` where date(created_at) = '{DT}' --current_date-1
        )
        and lower(tx_type) in ('withdrawal', 'debit', 'purchase', 'purchase with cashback')
)
,main_doc as (
    select
        c_date_prov,
        c_num_dt,
        c_num_kt,
        c_sum,
        c_nazn,
        split(split(c_nazn,'транзакции ')[1],' от')[0] as rrn,
        cast(nullif(split(split(c_nazn,'Сумма ')[1],' валюта')[0],split(split(c_nazn,'Сумма ')[1],', валюта')[0])
            as numeric(17,2)) as sum_nazn,
        sum(cast(nullif(split(split(c_nazn,'Сумма ')[1],' валюта')[0],split(split(c_nazn,'Сумма ')[1],', валюта')[0])
            as numeric(17,2))) over (partition by split(split(c_nazn,'транзакции ')[1],' от')[0],c_acc_dt) as sum_nazn_rrn,
        sum(c_sum) over (partition by split(split(c_nazn,'транзакции ')[1],' от')[0],c_acc_dt) as sum_rrn,
        c_acc_dt,
        c_acc_kt,
        case when ((c_acc_kt in ('30232810500000000004') 
                and c_acc_dt like '455%') or (c_acc_dt in ('30233810800000000004') and (c_acc_kt like '455%' or c_acc_kt like '407%' or c_acc_kt like '40802%' )))
            then 'case1'
            else 'case2' 
            end as check_case
    from main_docum m
    where 1=1
        and date(c_date_prov)='{DT}'--current_date-1
        and (
        ((c_acc_kt in ('30232810500000000004') 
        and c_num_dt like '455%') or (c_num_dt in ('30233810800000000004') and (c_acc_kt like '455%' or c_acc_kt like '407%' or c_acc_kt like '40802%' )))
        or
        (c_num_dt in ('30233810800000000004')
        and (c_acc_kt like '30232%' or c_acc_kt like '407%' or c_acc_kt like '40802%' ))
        )
)

select 
    '{DT}' as report_date,
    'Сверка по кредитным транзакциям с документами АБС. Списания' as report_name,
    case when max(c.ret_ref_number) is null then 'Отсутствие записи в клиринге'
    when sum(sum_nazn_rrn)<>sum(settl_amount_rrn) and sum(c.settl_amount)<>sum(m.sum_rrn) then 'Несоответствие сумм'
    when max(m.rrn) is null then 'Отсутствие записи в АБС' 
    end as error_name,
    coalesce(m.c_date_prov,c.txn_date) as txn_date,
    m.c_num_dt as debit_account_number,
    m.c_num_kt as credit_account_number,
    c.ttype,
    c.direction,
    c.t_direction,
    sum(coalesce(c.settl_amount,m.c_sum)) as amount,
    sum(c.settl_amount) as sum_settl_amount,
    sum(m.c_sum) as sum_c_sum,
    sum(settl_amount_rrn) as sum_settl_amount_rrn,
    sum(sum_nazn_rrn) as sum_sum_nazn_rrn,
    sum(m.sum_rrn) as sum_sum_rrn,
    coalesce(coalesce(p.c_internal_code,p2.c_guid),c.agr_number) as agr_number,
    coalesce(c.ret_ref_number,m.rrn) as rrn,
    c.acq_ref_number,
    c.trace_ref_number
from main_doc m
left join hoz_op_acc h on h.`c_account_dog#1#2`=m.c_acc_dt and h.c_date_beg is not null
left join product p on p.c_array_dog_acc=h.collection_id
left join hoz_op_acc h2 on h2.`c_account_dog#1#2`=m.c_acc_dt  and h2.c_date_beg is not null
left join ya_product p2 on p2.c_account_dog=h2.collection_id
full outer join clearing c on c.ret_ref_number=m.rrn and (c.settl_amount=m.sum_nazn or c.settl_amount=m.sum_rrn) and c.agr_number=coalesce(p.c_internal_code,p2.c_guid)
where 1=1 
   and (m.check_case='case1' or m.check_case is null) --правки 3101
   and (c.check_case='case1' or c.check_case is null) --правки 3101
group by 
    c.acq_ref_number,
    c.trace_ref_number,
    coalesce(m.c_date_prov,c.txn_date),
    m.c_num_dt,
    m.c_num_kt,
    c.direction,
    c.t_direction,
    coalesce(coalesce(p.c_internal_code,p2.c_guid),c.agr_number),
    c.ttype,
    coalesce(c.ret_ref_number,m.rrn)
having sum(sum_nazn_rrn)<>sum(settl_amount_rrn) and sum(c.settl_amount)<>sum(m.sum_rrn)
or max(c.ret_ref_number) is null 
or max(m.rrn) is null 
or sum(sum_nazn_rrn)<>sum(settl_amount_rrn) and sum(c.settl_amount)<>sum(m.sum_rrn)
union all
select 
    '{DT}' as report_date,
    'Сверка по кредитным транзакциям с документами АБС. Зачисления' as report_name,
    case when max(fc.ret_ref_number) is null then 'Отсутствие записи в клиринге'
    when sum(sum_nazn_rrn)<>sum(settl_amount_rrn) and sum(c.settl_amount)<>sum(m.sum_rrn) then 'Несоответствие сумм'
    when max(m.rrn) is null then 'Отсутствие записи в АБС' 
    end as error_name,
    coalesce(m.c_date_prov,c.txn_date) as txn_date,
    m.c_num_dt as debit_account_number,
    m.c_num_kt as credit_account_number,
    c.ttype,
    c.direction,
    c.t_direction,
    sum(coalesce(c.settl_amount,m.c_sum)) as amount,
    sum(c.settl_amount),
    sum(m.c_sum),
    sum(amount_rrn),
    sum(sum_nazn_rrn),
    sum(m.sum_rrn),
    coalesce(coalesce(p.c_internal_code,p2.c_guid),c.agr_number) as agr_number,
    coalesce(c.ret_ref_number,m.rrn) as rrn,
    c.acq_ref_number,
    c.trace_ref_number
from main_doc m
left join hoz_op_acc h on h.`c_account_dog#1#2`=m.c_acc_kt and h.C_DATE_BEG is not null
left join product p on p.c_array_dog_acc=h.collection_id
left join hoz_op_acc h2 on h2.`c_account_dog#1#2`=m.c_acc_kt  and h2.C_DATE_BEG is not null
left join ya_product p2 on p2.c_account_dog=h2.collection_id
full outer join clearing c on c.ret_ref_number=m.rrn and (c.settl_amount=m.sum_nazn or c.settl_amount=m.sum_rrn) and c.agr_number=coalesce(p.c_internal_code,p2.c_guid)
left join fake_clears fc on fc.ret_ref_number=c.ret_ref_number and (fc.amount=c.settl_amount or fc.trace_ref_number=c.trace_ref_number) and c.agr_number=fc.agr_number
where 1=1 
   and (m.check_case='case2' or m.check_case is null) --правки 3101
   and (c.check_case='case2' or c.check_case is null) --правки 3101
   and fc.ret_ref_number is null
group by 
    c.acq_ref_number,
    c.trace_ref_number,
    coalesce(m.c_date_prov,c.txn_date),
    m.c_num_dt,
    m.c_num_kt,
    c.direction,
    c.t_direction,
    p.c_internal_code,
    p2.c_guid,
    c.agr_number,
    c.ttype,
    coalesce(c.ret_ref_number,m.rrn)
having sum(sum_nazn_rrn)<>sum(amount_rrn) and sum(c.settl_amount)<>sum(m.sum_rrn)
or max(c.ret_ref_number) is null 
or max(m.rrn) is null 
or sum(sum_nazn_rrn)<>sum(c.settl_amount_rrn) and sum(c.settl_amount)<>sum(m.sum_rrn)