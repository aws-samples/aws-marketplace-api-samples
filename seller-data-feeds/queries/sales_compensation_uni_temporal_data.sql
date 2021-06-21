
-- General note: When executing this query we are assuming that the data ingested in the database is using just
-- one time axis (the valid_from column).
-- See documentation for more details: https://docs.aws.amazon.com/marketplace/latest/userguide/data-feed.html#data-feed-details



-- Let's get the latest revision of a product
-- A product can have multiple revisions where some of the columns like the title can change.
-- For the purpose of the Sales Compensation reports, we want to get the latest revision of a product
products_with_latest_revision as (
     select
      *
     from
     (
      select
       *,
       ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY from_iso8601_timestamp(valid_from) desc) as row_num_latest_revision
      from
       productfeed_v1
     )
     where
      row_num_latest_revision = 1
   ),

-- Let's get the latest revision of an address
-- An address_id can have multiple revisions where some of the columns can change.
-- For the purpose of the sales compensation report, we want to get the latest revision of an account
pii_with_latest_revision as (
     select
      *
     from
     (
      select
       *,
       ROW_NUMBER() OVER (PARTITION BY address_id,aws_account_id ORDER BY from_iso8601_timestamp(valid_from) desc) as row_num_latest_revision
      from
       piifeed
     )
     where
      row_num_latest_revision = 1
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
          from_iso8601_timestamp(invoice_date) as invoice_date,
          transaction_type,
          transaction_reference_id,
          product_id,
          disbursement_billing_event_id,
          action,
          from_account_id,
          to_account_id,
          end_user_account_id,
          billing_address_id,
          CAST(amount as decimal(20, 10)) as amount,
          currency,
          ROW_NUMBER() OVER (PARTITION BY billing_event_id, valid_from ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
        from
          billingeventfeed_v1
        where -- We only want to show amounts that are taken into account for seller disbursements.
              balance_impacting = 1
        )
      where row_num = 1
    ),

-- In order to get the mailing_address_id at the time of invoice, we need to get a historical date range for each account_id - mailing_address_id tuple.
-- This will allow us to identify which mailing_address_id is used for any given invoice date and account_id.
mailing_address_history as (
    Select
        account_id,
        mailing_address_id,
        valid_from,
        coalesce(lead(valid_from) over (partition by account_id order by valid_from asc),  timestamp '9999-01-01 00:00:00') as valid_to
    from
    ( -- Here we are getting the last date a account_id - mailing_address_id was valid.
      -- This will allow us to get the total date range an account_id - mailing_address_id combination was active.
            select
                max(from_iso8601_timestamp(valid_from)) as valid_from,
                account_id,
                mailing_address_id
            from accountfeed_v1
            group by account_id, mailing_address_id) q
),


-- There are some records that have no billing_address_id, in these cases we need to map the Account ID to its mailing address.
-- We will get the mailing address at the time of invoice because  customers can move after the invoice date and we want to keep the results consistent.
mailing_address_id as (
    Select
          transaction_reference_id,
          from_account_id,
          acc.mailing_address_id
    from billing_events_with_uni_temporal_data bill
    inner join mailing_address_history acc on acc.account_id = bill.from_account_id and (bill.invoice_date >= acc.valid_from  and (bill.invoice_date < valid_to OR valid_to =  timestamp '9999-01-01 00:00:00'))
    and transaction_type like 'SELLER_%' and billing_address_id = '' and action in ('INVOICED', 'FORGIVEN')
    group by transaction_reference_id, from_account_id, acc.mailing_address_id
),

-- Now we get a One to One Relationship for the transaction_reference_id, aws_account_id to join in relevant PII data. (Address,City,Country,etc)
-- We always want to have an address, in this case we default to mailing address if billing address is not known.
-- Billing addresses are not always known at time of invoice creation, but is always known at time of disbursement (because we disburse collected invoices)
-- Because of this, we will first look for billing_address from disbursed transactions and then fall back on invoiced billing_address

-- First, we get all transactions that have been disubrsed as this is the source of truth for billing_address_id.
-- If a transaction hasn't yet been disbursed, we fall back to the billing_address_id at time of invoice.

disubrsed_transactions as (
    Select
        bill.transaction_reference_id,
        bill.billing_address_id
    from billingeventfeed_v1 bill
    where action = 'DISBURSED' and  bill.transaction_type like 'SELLER_%'
    group by bill.transaction_reference_id, bill.billing_address_id
),

-- Now we get a One to One Relationship for the transaction_reference_id, aws_account_id to join in relevant PII data. (Address,City,Country,etc)
-- We always want to have an address, in this case we default to mailing address if billing address is not known.
-- Billing addresses are not always known at time of invoice creation, but is always known at time of disbursement (because we disburse collected invoices)
-- Because of this, we will first look for billing_address from disbursed transactions and then fall back on invoiced billing_address

pii_mapping as (

  Select
    bill.transaction_reference_id,
    case when coalesce(dis.billing_address_id, bill.billing_address_id) = '' then add.mailing_address_id else coalesce(dis.billing_address_id, bill.billing_address_id) end as address_id, -- In order to get the most accurate address_id, we start with looking for the address_id at time of disbursement. If there is no disbursement, we get the address_id at time of invoice. If there is no address_id in the billing table, then we use the mailing_address_id at time of invoice.
    aws_account_id
  from billing_events_with_uni_temporal_data bill
  inner join accounts_with_latest_revision acc on acc.account_id = bill.from_account_id and bill.transaction_type like 'SELLER_%'
  left join disubrsed_transactions dis on dis.transaction_reference_id = bill.transaction_reference_id
  left join mailing_address_id add on add.transaction_reference_id = bill.transaction_reference_id
  where (case when coalesce(dis.billing_address_id, bill.billing_address_id) = '' then add.mailing_address_id else coalesce(dis.billing_address_id, bill.billing_address_id) end) != '' and action in ('INVOICED', 'FORGIVEN')
  group by
    bill.transaction_reference_id,
    case when coalesce(dis.billing_address_id, bill.billing_address_id) = '' then add.mailing_address_id else coalesce(dis.billing_address_id, bill.billing_address_id) end,
    aws_account_id
),

-- An invoice can contain multiple line items
-- We create a pivot table to calculate the different amounts that are part of an invoice
-- The new row is aggregated at transaction_reference_id - payer_account level
transactions as (
  Select
  coalesce(sum(case when transaction_type = 'SELLER_REV_SHARE' then amount end),0) as "Gross Revenue",
  coalesce(sum(case when transaction_type = 'AWS_REV_SHARE' then amount end),0) as "AWS Revenue Share",
  coalesce(sum(case when transaction_type = 'SELLER_REV_SHARE_REFUND' then amount end),0) as "Gross Refunds",
  coalesce(sum(case when transaction_type = 'AWS_REV_SHARE_REFUND' then amount end),0) as "AWS Refunds Share",
  (
    coalesce(sum(case when transaction_type = 'SELLER_REV_SHARE' then amount end),0)
    + coalesce(sum(case when transaction_type = 'AWS_REV_SHARE' then amount end),0)
    + coalesce(sum(case when transaction_type = 'SELLER_REV_SHARE_REFUND' then amount end),0)
    + coalesce(sum(case when transaction_type = 'AWS_REV_SHARE_REFUND' then amount end),0)
    + coalesce(sum(case when transaction_type = 'SELLER_TAX_SHARE' then amount end),0)
    + coalesce(sum(case when transaction_type = 'AWS_TAX_SHARE' then amount end),0)
    + coalesce(sum(case when transaction_type = 'SELLER_TAX_SHARE_REFUND' then amount end),0)
    + coalesce(sum(case when transaction_type = 'AWS_TAX_SHARE_REFUND' then amount end),0)
  )  as "Net Revenue",
  bill.transaction_reference_id,
  bill.product_id,
  date_trunc('month',bill.invoice_date) as "Invoice Month",
  currency
  from billing_events_with_uni_temporal_data bill
  where action in ('INVOICED', 'FORGIVEN')
  group by
    bill.transaction_reference_id,
    bill.product_id,
    date_trunc('month',bill.invoice_date),
    currency
  )

  Select
  pii.aws_account_id as "Customer AWS Account Number",
  acc.encrypted_account_id as "Payer Reference ID",
  pii.country as "Country",
  pii.state_or_region as "State or Region",
  pii.city as "City",
  pii.postal_code as "Zip Code",
  pii.email_domain as "Email Domain",
  p.product_code as "Product Code",
  p.title as "Product Title",
  "Gross Revenue" as "Gross Revenue" ,
  "AWS Revenue Share" as "AWS Revenue Share",
  "Gross Refunds" as "Gross Refunds",
  "AWS Refunds Share" as "AWS Refunds Share",
  "Net Revenue" as "Net Revenue",
  txn.currency as "Currency",
  "Invoice Month",
  txn.transaction_reference_id as "Transaction Reference ID",
  map.address_id as "Payer Address ID"
  from transactions txn
  inner join products_with_latest_revision p on p.product_id = txn.product_id
  left join pii_mapping map on map.transaction_reference_id = txn.transaction_reference_id
  left join pii_with_latest_revision pii on pii.address_id = map.address_id and pii.aws_account_id = map.aws_account_id
  left join accounts_with_latest_revision acc on acc.aws_account_id = pii.aws_account_id
  where "Invoice Month" between date_parse('01-01-2020','%m-%d-%Y') and date_parse('12-31-2020','%m-%d-%Y')
  and "Net Revenue" != 0-- Filter out Forgiven and FDP transactions
  order by "Net Revenue" desc






