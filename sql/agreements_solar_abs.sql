with abs_product as (
    select
        C_INTERNAL_CODE
        ,C_DATE_BEGIN
        ,C_DATE_CLOSE
    from product 
    where C_INTERNAL_CODE like 'ag%'
)

,abs as (
    select
      coalesce(yp.C_GUID,p.C_INTERNAL_CODE) as agr_number
      ,date(coalesce(yp.C_DATE_BEG,p.C_DATE_BEGIN))     as open_date
      ,date(coalesce(yp.C_DATE_CLOSE,p.C_DATE_CLOSE))   as close_date
    from ya_product yp
    full join abs_product p on yp.C_GUID=p.C_INTERNAL_CODE
)

,solar as (
select
  agr_number
  ,effective_date
  ,closing_date
  ,signature_date
from bo_agreement
)

select
    case when a.open_date="{DT}" and s.agr_number is null then 'нет договора в Солар'
         when s.signature_date="{DT}" and a.agr_number is null then 'нет договора в АБС' 
         when (s.signature_date="{DT}" or a.open_date="{DT}") and a.open_date<>s.signature_date
             then 'не соответствует дата открытия'
         when (s.signature_date="{DT}" or a.open_date="{DT}") and a.close_date<>s.closing_date
             then 'не соответствует дата закрытия'
         end                                                as error_name
    ,coalesce(a.agr_number,s.agr_number)                    as agr_number
    ,a.open_date                                            as abs_open_date
    ,s.signature_date                                       as solar_open_date
    ,a.close_date                                           as abs_close_date
    ,s.closing_date                                         as solar_close_date       
from abs a
full join solar s on a.agr_number=s.agr_number
where 1=1
      and (
        (a.open_date="{DT}" and s.agr_number is null)
        or
        (s.signature_date="{DT}" and a.agr_number is null)
        or
        ((s.signature_date="{DT}" or a.open_date="{DT}") and a.open_date<>s.signature_date)
        or
        ((s.signature_date="{DT}" or a.open_date="{DT}") and a.close_date<>s.closing_date)
      )