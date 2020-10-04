select {{ null_if ('id') }} as id
      ,{{ null_if ('account_number_new_c') }} as on_number
      ,{{ null_if ('name') }} as name
      ,{{ null_if ('first_name') }} as first_name
      ,{{ null_if ('last_name') }} as last_name
      ,{{ null_if ('person_email') }} as email
      ,{{ null_if ('agent_c') }} as on_agent
      ,{{ null_if ('account_quality_c') }} as quality
      ,{{ null_if ('status_c') }} as status
      ,{{ null_if ('association_c') }} as association
      ,{{ null_if ('partnership_c') }} as partnership
      ,{{ null_if ('ust_id_number_c') }} as ust_id_number
      ,{{ null_if ('e_shop_dealer_c') }} as pos_dealer_locator
      ,{{ null_if ('item_category_c') }} as pos_product_range
      ,{{ null_if ('pre_order_ss_16_c') }} as pre_order_status
      ,dealer_latitude_c as pos_dealer_latitude
      ,dealer_Longitude_c as pos_dealer_longitude
      ,{{ null_if ('billing_street') }} as billing_street
      ,{{ null_if ('billing_postal_code') }} as billing_postal_code
      ,{{ null_if ('billing_city') }} as billing_city
      ,{{ null_if ('billing_state') }} as billing_state
      ,{{ null_if ('shipping_name_c') }} as shipping_name
      ,{{ null_if ('shipping_street')}} as shipping_street
      , {{ null_if('shipping_postal_code') }} as shipping_postal_code
      ,{{ null_if ('shipping_city') }} as shipping_city
      ,{{ null_if ('shipping_state') }} as shipping_state
      ,{{ null_if ('customer_language_c') }} as customer_language
      ,{{ null_if ('gender_pc') }} as gender
      ,{{ null_if ('type') }} as type
      ,{{ null_if ('collection_status_c') }} as collection_status
      ,{{ null_if ('lead_collection_status_update_c') }} as lead_collection_status_update
      ,{{ null_if ('comment_collection_status_c') }} as comment_collection_status
      , currency_iso_code
      ,{{ null_if ('on_tech_rep_c') }} as on_tech_rep
      ,{{ null_if ('gln_c') }} as gln_identifier
      ,{{ null_if ('operating_model_c') }} as operating_model
      ,{{ null_if ('key_account_c') }} as key_account_name
      ,{{ null_if ('risk_profile_c') }} as risk_profile
      ,{{ null_if ('corona_action_proposal_c') }} as corona_action_proposal
      ,{{ null_if ('customer_request_c') }} as customer_request
      ,{{ null_if ('why_not_active_c') }} as prospect_reason
      ,{{ null_if ('c_2_g_codaaccounts_payable_control_c') }} as accounts_payable_control
      ,{{ null_if ('c_2_g_codadefault_expense_account_c') }} as default_expense_account
      ,{{ null_if ('c_2_g_codabase_date_1_c') }} as base_date_1
      ,{{ null_if ('c_2_g_codatax_calculation_method_c') }} as tax_calculation_method
      ,{{ null_if ('c_2_g_codainput_vatcode_c') }} as input_vat_code
      ,{{ null_if ('payment_system_c') }} as payment_system
      ,{{ null_if ('c_2_g_codapayment_method_c') }} as payment_method
      ,{{ null_if ('c_2_g_codabank_name_c') }} as bank_name
      ,{{ null_if ('c_2_g_codabank_sort_code_c') }} as bank_sort_code
      ,{{ null_if ('c_2_g_codabank_account_name_c') }} as bank_account_name
      ,{{ null_if ('c_2_g_codabank_account_number_c') }} as bank_account_number
      ,{{ null_if ('bank_clearing_number_c') }} as bank_clearing_number
      ,{{ null_if ('c_2_g_codabank_ibannumber_c') }} as bank_iban_number
      ,{{ null_if ('c_2_g_codabank_swiftnumber_c') }} as bank_swift_number
      ,{{ null_if ('account_number_new_c') }} as on_account_number
      --boolean
      ,is_newsletter_subscriber_c as is_newsletter_subscriber
      ,person_has_opted_out_of_email as has_opted_out
      ,referrer_c as is_referrer
      , referral_c as is_referral
      ,ffps_cust_rem_exclude_from_reminder_process_c as exclude_from_reminder_yn
      ,using_aot_system_c as is_using_aot_system

      -- lookups
      ,{{ null_if ('parent_id') }} as parent_id
      ,{{ null_if ('record_type_id') }} as record_type_id
      ,{{ null_if ('pbsi_account_group_c') }} as group_id
      ,{{ null_if ('owner_id') }} as owner_id
      ,{{ null_if ('on_payment_term_c') }} as on_payment_terms_id
      ,{{ null_if ('on_payment_terms_pre_order_c') }} as on_payment_terms_pre_order_id
      ,{{ null_if ('pw_ccpro_billing_country_lookup_c') }} as billing_country_id
      ,{{ null_if ('pw_ccpro_shipping_country_lookup_c') }} as shipping_country_id
      ,{{ null_if ( 'agent_lookup_c' ) }} as agent_lookup_id
      ,{{ null_if ('c_2_g_codaoutput_vatcode_c') }} as vat_code_id
      ,{{ null_if ('c_2_g_codacredit_manager_c') }} as credit_manager_id
      ,{{ null_if ('c_2_g_codafinance_contact_c') }} as finance_contact_id
      ,{{ null_if ('account_channel_lookup_c') }} as account_channel_id
      ,{{ null_if ('c_2_g_codadimension_1_c') }} as dimension_1_id
      ,{{ null_if ('c_2_g_codadimension_2_c') }} as dimension_2_id
      , b_24_integration_model_c

      -- dates
      ,created_date as created_date
      ,ffps_onagdl_dunning_date_c as dunning_date
      ,first_order_date_c as first_order_date
      ,ffps_cust_rem_last_reminder_generated_date_c as last_reminder_generated_date
      ,ffps_cust_rem_reminders_blocked_until_date_c as reminders_blocked_until_date
      ,ffps_cust_rem_last_statement_generated_date_c as last_statement_generated_date
      ,last_collection_status_update_c as last_collection_status_update
      -- values
      ,{{ if_null_integer('account_balance_new_c') }} as balance
      ,{{ if_null_integer('account_overdue_balance_c') }} as overdue_balance
      ,{{ if_null_integer('c_2_g_codacredit_limit_c') }} as credit_limit
      ,{{ if_null_integer('avg_days_overdue_c') }} as max_days_overdue
      ,{{ if_null_integer('Shoes_sold_p_a_all_brands_c') }} as potential
      ,{{ if_null_integer('pbsi_standard_discount_c') }} / 100 as standard_discount
      ,{{ if_null_integer('pre_order_discount_c') }} / 100 as pre_order_discount
      ,{{ if_null_integer('Budget_1_Qu_2015_c') }} as budget_ss18_shoes
      ,{{ if_null_integer('Budget_2_Qu_2015_c') }} as budget_ss18_prg
      ,{{ if_null_integer('bg_fw_17_c') }} as budget_fw18_shoes
      ,{{ if_null_integer('bg_fw_17_prg_c') }} as budget_fw18_prg
      ,{{ if_null_integer('ffps_cust_rem_last_reminder_severity_level_c') }} as last_reminder_severity
      ,nps_c as nps_score
      --,nps_c2 as nps_score2
      ,ran_customer_c as ran_customer
      ,_fivetran_synced as fivetran_synced
from {{ source('salesforce', 'account') }}
where is_deleted = FALSE
