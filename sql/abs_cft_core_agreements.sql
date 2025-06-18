with agreement_2 as(
select *
from (
    select *,
        row_number() over (partition by id order by source__ts_ms desc) as row_num
    from agreement
    where 
        product = 'SAVINGS_ACCOUNT'
        and (
            (status in ('OPEN', 'CLOSING', 'RESERVED') 
            and DATE '1970-01-01' + (opening_date * interval '1 day') < current_date)
            or status = 'CLOSED'
        )
) subquery
where row_num = 1
    and dbz__op != 'd'
    and status not in ('DRAFT', 'OPEN_FAIL')
)

, product_2 as (
select 
    pr.C_NUM_DOG, 
    pr.C_INTERNAL_CODE, 
    pr.C_DATE_BEGINING, 
    pr.C_DATE_CLOSE, 
    st.C_CODE,
    cast(st.TXN_TS as date) as cft_status_date
from 
    product AS pr
inner join depn sa 
    on sa.ID = pr.id
inner join vid_deposit vd 
    on sa.C_VID_DOG = vd.ID and vd.C_CODE in ('YA_NS', 'YA_NO_TERM_SAVE')
left join com_status_prd st 
    on pr.C_COM_STATUS = st.id
where 
    (st.C_CODE in ('WORK', 'OPEN') and pr.C_DATE_BEGINING < CURRENT_DATE)
    or 
    (st.C_CODE in ('CLOSE', 'TO_CLOSE'))
)

, common_table_1 as(
select 
    pr.*, 
    agg.*,
    case 
        when C_CODE = 'WORK' then 'OPEN'
        when C_CODE in ('CLOSE', 'TO_CLOSE') then 'CLOSED'
        else C_CODE
    end as cft_status_for_join,
    case 
        when status in ('CLOSING', 'RESERVED') then 'OPEN'
        else status
    end as ftc_status_for_join,
    DATE '1970-01-01' + (opening_date * INTERVAL '1 day') as ftc_opening_date,
    cast(C_DATE_BEGINING as date) as cft_opening_date,
    DATE '1970-01-01' + (closing_date * INTERVAL '1 day') as ftc_closing_date,
    cast(C_DATE_CLOSE as date) as cft_closing_date,
    cast(coalesce(modified_ts, created_ts) as date) as ftc_status_date
    --coalesce(cast(modified_ts) as date, cast(created_ts) as date) as ftc_status_date, 
from 
    agreement_2 agg
full join product_2 pr 
    on pr.C_INTERNAL_CODE = agg.id
)

, common_table_2 as(
select 
    *,
    case 
        when id is null 
            then 'lost_ftc'
        when C_INTERNAL_CODE is null 
            then 'lost_cft'
        when (id is not null and C_INTERNAL_CODE is not null and cft_status_for_join != ftc_status_for_join
                and cft_status_date < current_date -1
                and ftc_status_date < current_date -1)
            then 'dif_status'
        when (id is not null and C_INTERNAL_CODE is not null and ftc_opening_date != cft_opening_date)
            then 'dif_open_date'
        when (id is not null and C_INTERNAL_CODE is not null and ftc_closing_date != cft_closing_date)
            then 'dif_close_date'
        when (id is not null and C_INTERNAL_CODE is not null and opening_date is null and C_DATE_BEGINING is null)
            then 'dif_open_null'
        else 'OK'
        end as dif_type
from common_table_1
)

, common_table_3 as (
select 
    dif_type,
    coalesce (C_NUM_DOG, visible_number) as agreement,
    case when dif_type = 'dif_status' then cft_status_for_join 
        end as cft_status, 
    case when dif_type = 'dif_status' then ftc_status_for_join 
        end as ftc_status,
    case when dif_type = 'dif_open_date' then cft_opening_date
        end as cft_opening_date,
    case when dif_type = 'dif_open_date' then ftc_opening_date
        end as ftc_opening_date,
    case when dif_type = 'dif_close_date' then cft_closing_date
        end as cft_closing_date,
    case when dif_type = 'dif_close_date' then ftc_closing_date
        end as ftc_closing_date
from common_table_2
where dif_type != 'OK'
)

select * from common_table_3