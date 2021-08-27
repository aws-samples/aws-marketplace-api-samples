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

    -- Let's get the latest version of a product
    -- A product can have multiple versions where some of the columns like the title can change.
    -- For the purpose of the disbursement reports, we want to get the latest version of a product
    products_with_latest_version as (
     select
      *
     from
     (
      select
       *,
       ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY from_iso8601_timestamp(valid_from) desc) as row_num_latest_version
       ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY from_iso8601_timestamp(valid_from) desc) as row_num_latest_version
      from
       products_with_uni_temporal_data
     )
     where
      row_num_latest_version = 1
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

    -- Let's get the latest version of an account
    -- An account can have multiple versions where some of the columns like the mailing_address_id can change.
    -- For the purpose of the disbursement reports, we want to get the latest version of an account
    accounts_with_latest_version as (
     select
      *
     from
     (
      select
       *,
       ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY from_iso8601_timestamp(valid_from) desc) as row_num_latest_version
      from
       accounts_with_uni_temporal_data
     )
     where
      row_num_latest_version = 1
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
          from_account_id,
          to_account_id,
          end_user_account_id,
          CAST(amount as decimal(20, 10)) invoice_amount,
          bank_trace_id,
          ROW_NUMBER() OVER (PARTITION BY billing_event_id, valid_from ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
        from
          billingeventfeed_v1
        )
      where row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
    ),

    -- Let's get all the disbursements
    -- The billing events data is immutable as per documentation: https://docs.aws.amazon.com/marketplace/latest/userguide/data-feed-billing-event.html
    -- We are not required to use time windows based on the valid_from column to get the most recent billing event
    disbursement_events as (
      select
        billing_events_raw.billing_event_id as disbursement_id,
        billing_events_raw.invoice_date as disbursement_date,
        billing_events_raw.bank_trace_id
      from
        billing_events_with_uni_temporal_data billing_events_raw
      where
        -- we're only interested in disbursements, so we filter non-disbursements by selecting transaction type to be DISBURSEMENT
        billing_events_raw.transaction_type = 'DISBURSEMENT'
        -- To select a time period, adjust the dates bellow if need be.
        -- For billing events we use the invoice date as the point in time of the disbursement being initiated
        and billing_events_raw.invoice_date >= from_iso8601_timestamp('2020-10-01T00:00:00Z')
        and billing_events_raw.invoice_date < from_iso8601_timestamp('2020-11-01T00:00:00Z')
    ),

    -- Get the invoices along with the line items that are part of the above filtered disbursements
    disbursed_line_items as (
      select
        line_items.transaction_reference_id,
        line_items.product_id,
        line_items.transaction_type,
        (case
           -- We get the payer of the invoice from any transaction type that is not AWS and not BALANCE_ADJUSTMENT
           -- For AWS and BALANCE_ADJUSTMENT, the billing event feed will show the "AWS Marketplace" account as the
           -- receiver of the funds and the seller as the payer. We are not interested in this information here.
           when line_items.transaction_type not like '%AWS%' and transaction_type not like 'BALANCE_ADJUSTMENT' then line_items.from_account_id
        end) as payer_account_id,
        line_items.end_user_account_id,
        invoice_amount,
        disbursements.disbursement_date,
        disbursements.disbursement_id,
        disbursements.bank_trace_id
      from
        billing_events_with_uni_temporal_data line_items
        -- Each disbursed line item is linked to the parent disbursement via the disbursement_billing_event_id
        join disbursement_events disbursements on disbursements.disbursement_id = line_items.disbursement_billing_event_id
      where
        -- we are interested only in the invoice line items that are DISBURSED
        line_items.action = 'DISBURSED'
    ),

  -- An invoice can contain multiple line items
  -- We create a pivot table to calculate the different amounts that are part of an invoice
  -- The new row is aggregated at transaction_reference_id - end_user_account_id level
  invoice_amounts_aggregated as (
    select
      transaction_reference_id,
      product_id,
      -- a given disbursement id should have the same disbursement_date
      max(disbursement_date) as disbursement_date,
      -- We're building a pivot table in order to provide all the data related to a transaction in a single row
      -- Note that the amounts are negated. This is because when an invoice is generated, we give you the positive amounts
      -- and the disbursement event negates the amounts
      sum(case when transaction_type = 'SELLER_REV_SHARE' then -invoice_amount else 0 end) as seller_rev_share,
      sum(case when transaction_type = 'AWS_REV_SHARE' then -invoice_amount else 0 end) as aws_rev_share,
      sum(case when transaction_type = 'SELLER_REV_SHARE_REFUND' then -invoice_amount else 0 end) as seller_rev_refund,
      sum(case when transaction_type = 'AWS_REV_SHARE_REFUND' then -invoice_amount else 0 end) as aws_rev_refund,
      sum(case when transaction_type = 'SELLER_REV_SHARE_CREDIT' then -invoice_amount else 0 end) as seller_rev_credit,
      sum(case when transaction_type = 'AWS_REV_SHARE_CREDIT' then -invoice_amount else 0 end) as aws_rev_credit,
      sum(case when transaction_type = 'SELLER_TAX_SHARE' then -invoice_amount else 0 end) as seller_tax_share,
      sum(case when transaction_type = 'SELLER_TAX_SHARE_REFUND' then -invoice_amount else 0 end) as seller_tax_refund,
      -- this is that account that pays the invoice.
      max(payer_account_id) as payer_account_id,
      -- this is the account that used your product (can be same as the one who subscribed if the license was not distributed but used directly)
      end_user_account_id as customer_account_id,
      bank_trace_id
    from
      disbursed_line_items
    group by
      transaction_reference_id,
      product_id,
      disbursement_id,
      -- there might be different end-users for the same transaction reference id. distributed licenses is an example
      end_user_account_id,
      -- there is one and only one bank_trace_id per disbursement
      --   -> this is redundant with "group by disbursement_id" but it is "cleaner" to set it in here than using a min or max in the select part
      bank_trace_id
),

disbursed_amount_by_product as (
  select
    products.title as "Product Title",
    products.product_code as "Product Code",
    -- We are rounding the sums using 2 decimal precision
    -- Note that the rounding method might differ between SQL implementations.
    -- The old disbursement report is using RoundingMode.HALF_UP. This might create discrepancies between this SQL output
    -- and the old disbursement report
    round(invoice_amounts.seller_rev_share, 2) as "Seller Rev",
    round(invoice_amounts.aws_rev_share, 2) as "AWS Ref Fee",
    round(invoice_amounts.seller_rev_refund, 2) as "Seller Rev Refund",
    round(invoice_amounts.aws_rev_refund, 2) as "AWS Ref Fee Refund",
    round(invoice_amounts.seller_rev_credit, 2) as "Seller Rev Credit",
    round(invoice_amounts.aws_rev_credit, 2) as "AWS Ref Fee Credit",
    (
        round(invoice_amounts.seller_rev_share, 2) +
        round(invoice_amounts.aws_rev_share, 2) +
        round(invoice_amounts.seller_rev_refund, 2) +
        round(invoice_amounts.aws_rev_refund, 2) +
        round(invoice_amounts.seller_rev_credit, 2) +
        round(invoice_amounts.aws_rev_credit, 2) +
        round(invoice_amounts.seller_tax_share, 2) +
        round(invoice_amounts.seller_tax_refund, 2)
    ) as "Net Amount",
    invoice_amounts.transaction_reference_id as "Transaction Reference ID",
    round(invoice_amounts.seller_tax_share, 2) as "Seller Sales Tax",
    round(invoice_amounts.seller_tax_refund, 2) as "Seller Sales Tax Refund",
    payer_info.aws_account_id as "Payer AWS Account ID", -- "Customer AWS Account Number" in legacy report
    customer_info.aws_account_id as "End Customer AWS Account ID",
    invoice_amounts.disbursement_date as "Disbursement Date",
    invoice_amounts.bank_trace_id as "Bank Trace ID"
  from
    invoice_amounts_aggregated invoice_amounts
    join products_with_latest_version products on products.product_id = invoice_amounts.product_id
    left join accounts_with_latest_version payer_info on payer_info.account_id = invoice_amounts.payer_account_id
    left join accounts_with_latest_version customer_info on customer_info.account_id = invoice_amounts.customer_account_id
)

select * from disbursed_amount_by_product;
-- To filter on a specific month, update the date range in the where clause of the "disbursement_events" subquery above
