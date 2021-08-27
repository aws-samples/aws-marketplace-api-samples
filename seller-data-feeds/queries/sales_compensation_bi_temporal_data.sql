    -- General note: When executing this query we are assuming that the data ingested in the database is using
    -- two time axes (the valid_from column and the update_date column).
    -- See documentation for more details: https://docs.aws.amazon.com/marketplace/latest/userguide/data-feed.html#data-feed-details

    -- Let's get all the products and keep the latest product_id, valid_from tuple
    -- We're transitioning from a bi-temporal data model to an uni-temporal data_model
    with products_with_uni_temporal_data as (
      select
       *
      from
      (
        select
         *,
         ROW_NUMBER() OVER (PARTITION BY product_id, valid_from ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
        from
         productfeed_v1
      )
      where
        -- a product_id can appear multiple times with the same valid_from date but with a different update_date column
        -- we are only interested in the most recent tuple
        row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
    ),

    -- Let's get the latest revision of a product
    -- A product can have multiple revisions where some of the columns like the title can change.
    -- For the purpose of the sales compensation report, we want to get the latest revision of a product
    products_with_latest_revision as (
     select
      *
     from
     (
      select
       *,
       ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY from_iso8601_timestamp(valid_from) desc) as row_num_latest_revision
      from
       products_with_uni_temporal_data
     )
     where
      row_num_latest_revision = 1
   ),

   -- Let's get all the addresses and keep the latest address_id and valid_from combination
	 -- We're transitioning from a bi-temporal data model to an uni-temporal data_model
   piifeed_with_uni_temporal_data as (
     select
      *
     from
     (
       select
        *,
        ROW_NUMBER() OVER (PARTITION BY address_id, valid_from ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
       from
        piifeed
     )
     where
       -- an address_id can appear multiple times with the same valid_from date but with a different update_date column
       -- we are only interested in the most recent
       row_num = 1
       -- ... and remove the soft-deleted one.
       and (delete_date is null or delete_date = '')
   ),

	 -- Let's get the latest revision of an address
	 -- An address_id can have multiple revisions where some of the columns can change.
	 -- For the purpose of the sales compensation report, we want to get the latest revision of an address
	 pii_with_latest_revision as (
      select
       *
      from
      (
       select
        *,
        ROW_NUMBER() OVER (PARTITION BY address_id ORDER BY from_iso8601_timestamp(valid_from) desc) as row_num_latest_revision
       from
        piifeed_with_uni_temporal_data
      )
      where
       row_num_latest_revision = 1
    ),

    -- Let's get all the accounts and keep the latest account_id, valid_from tuple
    -- We're transitioning from a bi-temporal data model to an uni-temporal data_model
    accounts_with_uni_temporal_data as (
      select
       *
      from
      (
        select
         *,
         ROW_NUMBER() OVER (PARTITION BY account_id, valid_from ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
        from
         accountfeed_v1
      )
      where
        -- an account_id can appear multiple times with the same valid_from date but with a different update_date column
        -- we are only interested in the most recent tuple
        row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
    ),

    -- Let's get all the historical dates for an account
    -- An account can have multiple revisions where some of the columns like the mailing_address_id can change.
    accounts_with_history as (
     select
      *,
      -- This interval's begin_date
      case
        when
        -- First record for a given account_id
          lag(valid_from, 1) over (partition by account_id order by from_iso8601_timestamp(valid_from) asc) is null
        then
          -- 'force' begin_date a bit earlier because of different data propagation times. We'll subtract one day as one
          -- hour is not sufficient
          from_iso8601_timestamp(valid_from) - INTERVAL '1' DAY
        else
          -- not the first line -> return the real date
          from_iso8601_timestamp(valid_from)
      end as begin_date,
      -- This interval's end date.
      COALESCE(
           LEAD(from_iso8601_timestamp(valid_from), 1) OVER (partition by account_id ORDER BY from_iso8601_timestamp(valid_from)),
           from_iso8601_timestamp('9999-01-01T00:00:00Z')
      ) as end_date
     from
       accounts_with_uni_temporal_data
   ),

    -- Let's get all the billing events and keep the latest billing_event_id, valid_from tuple
    -- We're transitioning from a bi-temporal data model to an uni-temporal data_model
    billing_events_with_uni_temporal_data as (
      select
       *
      from (
        select
          billing_event_id,
          from_iso8601_timestamp(valid_from) as valid_from,
          from_iso8601_timestamp(update_date) as update_date,
          delete_date,
          from_iso8601_timestamp(invoice_date) as invoice_date,
          transaction_type,
          transaction_reference_id,
          product_id,
          disbursement_billing_event_id,
          action,
          currency,
          from_account_id,
          to_account_id,
          end_user_account_id,
          -- convert an empty billing address to null. This will be later used in a COALESCE call
          case
           when billing_address_id <> '' then billing_address_id else null
          end as billing_address_id,
          CAST(amount as decimal(20, 10)) invoice_amount,
          ROW_NUMBER() OVER (PARTITION BY billing_event_id, valid_from ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
        from
          billingeventfeed_v1
        where
          -- The Sales Compensation Report does not contain BALANCE ADJUSTMENTS, so we filter them out here
          transaction_type <> 'BALANCE_ADJUSTMENT'
          -- Keep the transactions that will affect any future disbursed amounts
          and balance_impacting = '1'
        )
      where row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
    ),

    -- Let's get the billing address for all DISBURSED invoices. This will be the address of the payer when
    -- the invoice was paid.
    -- NOTE: For legal reasons, for CPPO transactions, the manufacturer will not see the payer's billing address id
    billing_addresses_for_disbursed_invoices as (
      select
        billing_events_raw.transaction_reference_id,
        billing_events_raw.billing_address_id,
        billing_events_raw.from_account_id
      from
        billing_events_with_uni_temporal_data billing_events_raw
      where
        -- the disbursed items will contain the billing address id
        billing_events_raw.action = 'DISBURSED'
        -- we only want to get the billing address id for the transaction line items where the seller is the receiver
        -- of the amount
        and billing_events_raw.transaction_type like 'SELLER_%'
      group by
        billing_events_raw.transaction_reference_id,
        billing_events_raw.billing_address_id,
        billing_events_raw.from_account_id
    ),

  -- An invoice can contain multiple line items
  -- We create a pivot table to calculate the different amounts that are part of an invoice
  -- The new row is aggregated at transaction_reference_id - end_user_account_id level
  invoiced_and_forgiven_transactions as (
    select
      transaction_reference_id,
      product_id,
      -- A transaction will have the same invoice date for all of its line items (transaction types)
      max(invoice_date) as invoice_date,
      -- A transaction will have the same billing_address_id for all of its line items. Remember that the billing event
      -- is uni temporal and we retrieved only the latest valid_from item
      max(billing_address_id) as billing_address_id,
      --  A transaction will have the same currency for all of its line items
      max(currency) as currency,
      -- We're building a pivot table in order to provide all the data related to a transaction in a single row
      sum(case when transaction_type = 'SELLER_REV_SHARE' then invoice_amount else 0 end) as seller_rev_share,
      sum(case when transaction_type = 'AWS_REV_SHARE' then invoice_amount else 0 end) as aws_rev_share,
      sum(case when transaction_type = 'SELLER_REV_SHARE_REFUND' then invoice_amount else 0 end) as seller_rev_refund,
      sum(case when transaction_type = 'AWS_REV_SHARE_REFUND' then invoice_amount else 0 end) as aws_rev_refund,
      sum(case when transaction_type = 'SELLER_REV_SHARE_CREDIT' then invoice_amount else 0 end) as seller_rev_credit,
      sum(case when transaction_type = 'AWS_REV_SHARE_CREDIT' then invoice_amount else 0 end) as aws_rev_credit,
      sum(case when transaction_type = 'SELLER_TAX_SHARE' then invoice_amount else 0 end) as seller_tax_share,
      sum(case when transaction_type = 'SELLER_TAX_SHARE_REFUND' then invoice_amount else 0 end) as seller_tax_refund,
      -- this is the account that pays the invoice.
      max(case
        -- We get the payer of the invoice from any transaction type that is not AWS and not BALANCE_ADJUSTMENT
        -- For AWS and BALANCE_ADJUSTMENT, the billing event feed will show the "AWS Marketplace" account as the
        -- receiver of the funds and the seller as the payer. We are not interested in this information here.
        when
         transaction_type not like '%AWS%' and transaction_type not like 'BALANCE_ADJUSTMENT' then from_account_id
       end) as payer_account_id,
      -- this is the account that used your product (can be same as the one who subscribed if the license was not distributed but used directly)
      end_user_account_id as customer_account_id
    from
      billing_events_with_uni_temporal_data
    where
      -- we only care for invoiced or forgiven items. disbursements are not part of the sales compensation report
      action in ('INVOICED', 'FORGIVEN')
    group by
      transaction_reference_id,
      product_id,
      -- there might be different end-users for the same transaction reference id. distributed licenses is an example
      end_user_account_id
),

invoiced_items_with_product_and_billing_address as (
  select
    invoice_amounts.*,
    products.product_code,
    products.title,
    payer_info.aws_account_id as payer_aws_account_id,
    payer_info.account_id as payer_reference_id,
    customer_info.aws_account_id as end_user_aws_account_id,
    (
        invoice_amounts.seller_rev_share +
        invoice_amounts.aws_rev_share +
        invoice_amounts.seller_rev_refund +
        invoice_amounts.aws_rev_refund +
        invoice_amounts.seller_rev_credit +
        invoice_amounts.aws_rev_credit +
        invoice_amounts.seller_tax_share +
        invoice_amounts.seller_tax_refund
    ) as seller_net_revenue,
    -- Try to get the billing address from the DISBURSED event (if any)
    -- If there is no DISBURSEMENT, get the billing address from the INVOICED item
    -- If still no billing address, then default to getting the mailing address of the payer
    coalesce(billing_add.billing_address_id, invoice_amounts.billing_address_id, payer_info.mailing_address_id) as final_billing_address_id
  from
    invoiced_and_forgiven_transactions invoice_amounts
    join products_with_latest_revision products on products.product_id = invoice_amounts.product_id
    left join accounts_with_history payer_info on payer_info.account_id = invoice_amounts.payer_account_id
        -- Get the Payer Information at the time of invoice creation
        and payer_info.begin_date <= invoice_amounts.invoice_date and invoice_amounts.invoice_date < payer_info.end_date
    left join accounts_with_history customer_info on customer_info.account_id = invoice_amounts.customer_account_id
        -- Get the End User Information at the time of invoice creation
        and customer_info.begin_date <= invoice_amounts.invoice_date and invoice_amounts.invoice_date < customer_info.end_date
    left join billing_addresses_for_disbursed_invoices billing_add on billing_add.transaction_reference_id = invoice_amounts.transaction_reference_id
        and billing_add.from_account_id = invoice_amounts.payer_account_id
),

invoices_with_full_address as (
  select
    payer_aws_account_id as "Payer AWS Account ID", -- "Customer AWS Account Number" in legacy report
    pii_data.country as "Country",
    pii_data.state_or_region as "State",
    pii_data.city as "City",
    pii_data.postal_code as "Zip Code",
    pii_data.email_domain as "Email Domain",
    product_code as "Product Code",
    title as "Product Title",
    seller_rev_share as "Gross Revenue",
    aws_rev_share as "AWS Revenue Share",
    seller_rev_refund as "Gross Refunds",
    aws_rev_refund as "AWS Refunds Share",
    seller_net_revenue as "Net Revenue",
    currency as "Currency",
    date_format(invoice_date, '%Y-%m')as "AR Period",
    transaction_reference_id as "Transaction Reference ID",
    payer_reference_id as "Payer Reference ID",
    end_user_aws_account_id as "End Customer AWS Account ID"
  from
    invoiced_items_with_product_and_billing_address invoice_amounts
    left join pii_with_latest_revision pii_data on pii_data.address_id = invoice_amounts.final_billing_address_id
    -- Filter out FORGIVEN and Field Demonstration Pricing transactions
    where seller_net_revenue <> 0
)

select * from invoices_with_full_address
-- To filter on a specific month, uncomment the following and replace the date:
-- where "AR Period" = '2021-04'