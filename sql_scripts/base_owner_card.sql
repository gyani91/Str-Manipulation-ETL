select {{ null_if ('id') }} as id
      ,{{ null_if ('name') }} as name
      ,{{ null_if ('other_activities_c') }} as other_activities
      ,{{ null_if ('other_goals_c') }} as other_goals
      ,{{ null_if ('product_c') }} as product
      ,{{ null_if ('purchased_from_c') }} as purchased_from
      ,{{ null_if ('goals_for_the_year_c') }} as goals_for_the_year
      ,{{ null_if ('terrain_c') }} as terrain
      ,{{ null_if ('terrain_picklist_c') }} as terrain_picklist
      ,{{ null_if ('weekly_distance_c') }} as weekly_distance
      ,{{ null_if ('product_color_c') }} as product_color
      ,{{ null_if ('product_size_c') }} as product_size
      ,first_on_product_c as is_first_on_product
      -- lookups
      ,{{ null_if ('account_c') }} as account_id
      ,{{ null_if ('purchased_from_dealer_c') }} as purchased_from_dealer_id
      -- dates
      ,owner_card_registration_date_c as owner_card_registration_date
      ,created_date
      -- system
      ,_fivetran_synced as fivetran_synced
from {{ source('salesforce', 'owner_card_c') }}
where is_deleted = FALSE
