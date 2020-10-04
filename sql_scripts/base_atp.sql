select {{ null_if ('id') }} as id
      ,{{ null_if ('name') }} as name
      -- lookups
      ,{{ null_if ('item_c') }} as item_id
      ,{{ null_if ('location_c') }} as location_id
      ,{{ null_if ('inventory_c') }} as inventory_id
      -- values
      ,{{ if_null_integer('current_atp_c') }} as current_atp
      ,{{ if_null_integer('av_date_atp_c') }} as av_date_atp
      -- dates
      ,timestamp(current_date()) as snapshot_date
      ,availability_date_c
      ,current_atp_last_calculated_at_c
      ,av_date_atp_last_calculated_at_c
      -- system
      ,_fivetran_synced as fivetran_synced
from {{ source('salesforce', 'atp_c') }}
where is_deleted = FALSE
