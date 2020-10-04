select {{ null_if ('id') }} as id
      ,{{ null_if ('name') }} as pi_number
      ,{{ null_if ('c_2_g_invoice_status_c') }} as pi_invoice_status
      ,{{ null_if ('approval_status_c') }} as pi_approval_status
      ,{{ null_if ('approving_user_c') }} as pi_approving_user
      ,{{ null_if ('approved_from_c') }} as pi_approved_from
      ,{{ null_if ('c_2_g_owner_company_c') }} as pi_company_id
      ,{{ null_if ('c_2_g_account_c') }} as pi_account_id
      ,{{ null_if ('ascent_2_ff_purchase_order_c') }} as pi_sales_order_id
      ,{{ null_if ('c_2_g_invoice_currency_c') }} as pi_invoice_currency_id
      ,c_2_g_due_date_c as pi_due_date
      ,c_2_g_invoice_date_c as pi_invoice_date
      ,approval_date_c as pi_approval_date
      ,_fivetran_synced as fivetran_synced
from {{ source('financialforce', 'c_2_g_coda_purchase_invoice_c') }}
where is_deleted = FALSE
