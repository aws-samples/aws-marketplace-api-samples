    -- General note: When executing this query we are assuming that the data ingested in the database is using just
    -- one time axis (the valid_from column).
    -- See documentation for more details: https://docs.aws.amazon.com/marketplace/latest/userguide/data-feed.html#data-feed-details

    -- Let's try to create a historical view of all the products. We can use this view to get product information
    -- at a certain point in time (disbursement date, invoice creation, etc...)
    -- A product can have multiple versions where some of the columns like the title can change.
    with products_with_history as (
      select
       *,
       COALESCE(
           LEAD(from_iso8601_timestamp(valid_from), 1) OVER (partition by product_id ORDER BY from_iso8601_timestamp(valid_from)),
           from_iso8601_timestamp('2999-01-01T00:00:00Z')
       ) as valid_to
      from
       productfeed_v1
   ),

    -- Let's try to create a historical view of all the accounts. We can use this view to get account information
    -- at a certain point in time (disbursement date, invoice creation, etc...)
    -- An account can have multiple versions where some of the columns like the mailing_address_id can change.
    accounts_with_history as (
      select
       *,
       COALESCE(
           LEAD(from_iso8601_timestamp(valid_from), 1) OVER (partition by account_id ORDER BY from_iso8601_timestamp(valid_from)),
           from_iso8601_timestamp('2999-01-01T00:00:00Z')
       ) as valid_to
      from
       accountfeed_v1
   ),

    -- Let's get all the disbursements
    -- The billing events data is immutable as per documentation: https://docs.aws.amazon.com/marketplace/latest/userguide/data-feed-billing-event.html
    -- We are not required to use time windows based on the valid_from column to get the most recent billing event
    disbursement_events as (
      select
        billing_events_raw.billing_event_id as disbursement_id,
        from_iso8601_timestamp(billing_events_raw.invoice_date) as disbursement_date,
        billing_events_raw.bank_trace_id
      from
        billingeventfeed_v1 billing_events_raw
      where
        -- we're only interested in disbursements, so we filter non-disbursements by selecting transaction type to be DISBURSEMENT
        billing_events_raw.transaction_type = 'DISBURSEMENT'
        -- Select a time period, adjust the dates bellow if need be. For billing events we use the invoice date as the point in time of the disbursement being initiated
        and from_iso8601_timestamp(billing_events_raw.invoice_date) >= from_iso8601_timestamp('2020-10-01T00:00:00Z')
        and from_iso8601_timestamp(billing_events_raw.invoice_date) < from_iso8601_timestamp('2020-11-01T00:00:00Z')
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
        CAST(line_items.amount as decimal(20, 10)) invoice_amount,
        disbursements.disbursement_date,
        disbursements.disbursement_id,
        disbursements.bank_trace_id
      from
        billingeventfeed_v1 line_items
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
      -- this is the account that subscribed to your product
      end_user_account_id as customer_account_id,
      bank_trace_id
    from
      disbursed_line_items
    group by
      transaction_reference_id,
      product_id,
      disbursement_id,
      -- there might be a different end-user for the same transaction reference id. distributed licenses is an example
      end_user_account_id,
      bank_trace_id
),

disbursed_amount_by_product as (
  select
    products.title as ProductTitle,
    products.product_code as ProductCode,
    -- We are rounding the sums using 2 decimal precision
    -- Note that the rounding method might differ between SQL implementations.
    -- The old disbursement report is using RoundingMode.HALF_UP. This might create discrepancies between this SQL output
    -- and the old disbursement report
    round(invoice_amounts.seller_rev_share, 2) as SellerRev,
    round(invoice_amounts.aws_rev_share, 2) as AWSRefFee,
    round(invoice_amounts.seller_rev_refund, 2) as SellerRevRefund,
    round(invoice_amounts.aws_rev_refund, 2) as AWSRefFeeRefund,
    round(invoice_amounts.seller_rev_credit, 2) as SellerRevCredit,
    round(invoice_amounts.aws_rev_credit, 2) as AWSRefFeeCredit,
    (
        round(invoice_amounts.seller_rev_share, 2) +
        round(invoice_amounts.aws_rev_share, 2) +
        round(invoice_amounts.seller_rev_refund, 2) +
        round(invoice_amounts.aws_rev_refund, 2) +
        round(invoice_amounts.seller_rev_credit, 2) +
        round(invoice_amounts.aws_rev_credit, 2)
    ) as NetAmount,
    invoice_amounts.transaction_reference_id as TransactionReferenceID,
    round(invoice_amounts.seller_tax_share, 2) as SellerSalesTax,
    round(invoice_amounts.seller_tax_refund, 2) as SellerSalesTaxRefund,
    payer_info.aws_account_id as PayerAwsAccountId,
    customer_info.aws_account_id as EndCustomerAwsAccountId,
    invoice_amounts.disbursement_date as DisbursementDate,
    invoice_amounts.bank_trace_id as BankTraceId
  from
    invoice_amounts_aggregated invoice_amounts
    join products_with_history products on products.product_id = invoice_amounts.product_id
    left join accounts_with_history payer_info on payer_info.account_id = invoice_amounts.payer_account_id
     -- For the purpose of the disbursement report we'll get the latest payer account view from history.
     AND from_iso8601_timestamp(payer_info.valid_from) <= current_timestamp and current_timestamp < payer_info.valid_to
    left join accounts_with_history customer_info on customer_info.account_id = invoice_amounts.customer_account_id
     -- For the purpose of the disbursement report we'll get the latest customer account view from history.
     AND from_iso8601_timestamp(customer_info.valid_from) <= current_timestamp and current_timestamp < customer_info.valid_to
  where
  -- For the purpose of the disbursement report we'll get the latest product view from history.
  from_iso8601_timestamp(products.valid_from) <= current_timestamp and current_timestamp < products.valid_to
  -- Change the "current_timestamp" to any historical date (i.e. disbursement_date) to view how the product or account
  -- appeared at that point in time. For example:
  -- from_iso8601_timestamp(products.valid_from) <= current_timestamp and current_timestamp < products.valid_to
  -- can be changed (to get the product information at the time of disbursement) to:
  -- from_iso8601_timestamp(products.valid_from) <= invoice_amounts.disbursement_date and invoice_amounts.disbursement_date < products.valid_to
)

select * from disbursed_amount_by_product;
