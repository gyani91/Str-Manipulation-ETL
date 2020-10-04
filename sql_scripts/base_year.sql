select {{ null_if ('id') }} as year_id
      ,{{ null_if ('name') }} as year_name
      ,{{ null_if ('c_2_g_status_c') }} as year_is_closed
      ,c_2_g_start_date_c as year_start_date
      ,c_2_g_end_date_c as year_end_date
      -- lookups
      ,{{ null_if ('c_2_g_owner_company_c') }} as year_owner_company
from {{ source('financialforce', 'c_2_g_coda_year_c') }}
where is_deleted = FALSE
