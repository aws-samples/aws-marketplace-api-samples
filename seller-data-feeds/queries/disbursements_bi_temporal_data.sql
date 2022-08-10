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
         -- An offer_id has several valid_from dates (each representing an offer revision)
--   but because of bi-temporality, an offer_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
offers_with_uni_temporal_data as (
    select
        *
    from
    (
        select
            *,
            ROW_NUMBER() OVER (PARTITION BY offer_id, valid_from ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
        from
            offerfeed_v1
    )
    where
        -- keep latest ...
        row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
),
-- Here, we build the validity time range (adding valid_to on top of valid_from) of each offer revision.
-- We will use it to get Offer name at invoice time.
-- NB: If you'd rather get "current" offer name, un-comment "offers_with_latest_revision"
offers_with_history as (
    select
        offer_id,
        offer_revision,
        name,
        opportunity_name,
        opportunity_description,
        opportunity_id,
        from_iso8601_timestamp(valid_from) as valid_from,
        coalesce(
            lead(from_iso8601_timestamp(valid_from)) over (partition by offer_id order by from_iso8601_timestamp(valid_from) asc),
            timestamp '2999-01-01 00:00:00'
        ) as valid_to
    from offers_with_uni_temporal_data
),
-- provided for reference only if you are interested into get "current" offer name
--  (ie. not used afterwards)
offers_with_latest_revision as (
    select
        *
    from
    (
        select
            offer_id,
            offer_revision,
            name,
            opportunity_name,
            opportunity_description,
            opportunity_id,
            valid_from,
            null valid_to,
            ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY from_iso8601_timestamp(valid_from) desc) as row_num_latest_revision
        from
         offers_with_uni_temporal_data
    )
    where
        row_num_latest_revision = 1
),


-- An agreement_id has several valid_from dates (each representing an agreement revision)
--   but because of bi-temporality, an agreement_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
agreements_with_uni_temporal_data as (
    select
        *
    from
    (
        select
            *,
            ROW_NUMBER() OVER (PARTITION BY agreement_id, valid_from ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
        from
            -- TODO change to agreementfeed_v1 when Agreement Feed is GA'ed
            agreementfeed
    )
    where
        -- keep latest ...
        row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
),
-- Here, we build the validity time range (adding valid_to on top of valid_from) of each agreement revision.
-- We will use it to get agreement metadata at invoice time.
agreements_with_history as (
    select
        agreement_id,
        origin_offer_id as offer_id,
        proposer_account_id,
        acceptor_account_id,
        agreement_revision,
        start_date,
        end_date,
        acceptance_date,
        from_iso8601_timestamp(valid_from) as valid_from,
        coalesce(
            lead(from_iso8601_timestamp(valid_from)) over (partition by agreement_id order by from_iso8601_timestamp(valid_from) asc),
            timestamp '2999-01-01 00:00:00'
        ) as valid_to
    from agreements_with_uni_temporal_data
),
-- enrich each account history record with company name from mailing_address
accounts_with_latest_version_with_company_name as (
    select
        acc.account_id,
        acc.aws_account_id,
        acc.encrypted_account_id,
        acc.mailing_address_id,
        acc.tax_address_id,
        pii_mailing.company_name as mailing_company_name,
        pii_mailing.email_domain
    from accounts_with_latest_version acc
    -- left join because mailing_address_id can be null but when exists
    left join pii_with_latest_revision pii_mailing on acc.mailing_address_id=pii_mailing.address_id
),
    -- Let's get all the billing events and keep the latest billing_event_id, valid_from tuple
    -- We're transitioning from a bi-temporal data model to an uni-temporal data_model
    billing_events_with_uni_temporal_data as (
      select
       *
      from (
        select
          billing_event_id,
          parent_billing_event_id,
          from_iso8601_timestamp(valid_from) as valid_from,
          from_iso8601_timestamp(update_date) as update_date,
          delete_date,
          from_iso8601_timestamp(invoice_date) as invoice_date,
          invoice_id,
          case when payment_due_date = '' then null else from_iso8601_timestamp(payment_due_date) end as payment_due_date,
          transaction_type,
          transaction_reference_id,
          agreement_id,
          product_id,
          disbursement_billing_event_id,
          action,
          from_account_id,
          to_account_id,
          end_user_account_id,
          billing_address_id,
          CAST(amount as decimal(20, 10)) invoice_amount,
          bank_trace_id,
          --As broker_id has not been backfilled, manually fill up this column
  		  case when broker_id is null or broker_id = '' then 'AWS_INC' else broker_id end as broker_id,
          ROW_NUMBER() OVER (PARTITION BY billing_event_id, valid_from ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
        from
          billingeventfeed_v1
        )
      where row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
    ),

invoiced_billing_events_with_uni_temporal_data as (
    select *
    from billing_events_with_uni_temporal_data
    where action in ('FORGIVEN', 'INVOICED')
),

-- Here we select the account_id of the current seller (We identify this by looking for the to_account_id related to revenue transactions).
-- We will use it later to distinguish own agreements from agreements generated by channel partners.
seller_account as (
     select
         from_account_id as seller_account_id
     from
         invoiced_billing_events_with_uni_temporal_data bill
     where
        -- Assumption here is only seller will pay listing fee. As of 12/21/2021, there are cases that Channel partner have 0 listing fee for CPPO, so the amount could be 0.
         bill.transaction_type like 'AWS_REV_SHARE' and invoice_amount <= 0 and action = 'INVOICED'
     group by
         -- from_account_id is always the same for all those "listing fee" transactions == the seller of record himself.
         -- If this view returns more than 1 record, the overall query will fail (on purpose). Please contact AWS Marketplace if this happens.
         from_account_id
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
    ),

    -- Get the invoices along with the line items that are part of the above filtered disbursements
    disbursed_line_items as (
      select
        line_items.transaction_reference_id,
        line_items.product_id,
        line_items.transaction_type,
        line_items.from_account_id,
        invoice_date,
        invoice_id,
        line_items.billing_event_id,
        payment_due_date,
        line_items.end_user_account_id,
        line_items.broker_id,
        line_items.agreement_id,
        line_items.billing_address_id,
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

       --When broker_id is 'AWS_EUROPE',invoice_id of listing fee is different from invoice_id of seller revenue, these 2 invoice_ids are linked through parent_billing_event_id and billing_event_id.
--To map all amounts to the seller invoices, parent_billing_event_id is used when the transaction type is not SELLER_REV_SHARE
--Below CTE maps the listing fee invoices to the related buyer invoice.
--It's also used to override listing fee invoice values with the buyer invoice values for the fields that those values are different
--(e.x. invoice_date, billing_address_id, transaction_reference_id, from_account_id, etc.)
seller_invoice_id_mapping as(
    select distinct
        inv.invoice_id,
        inv.billing_event_id, inv.invoice_date,
        inv.transaction_reference_id,inv.transaction_type, inv.from_account_id,
        inv.billing_address_id, inv.payment_due_date, inv.agreement_id,
        listing.invoice_id as listing_fee_invoice_id,
        listing.transaction_reference_id as listing_fee_transaction_reference_id
    from (
        select
        case
            when transaction_type like 'SELLER_REV_SHARE%'
            then billing_event_id
            else parent_billing_event_id
        end as billing_event_id_seller_rev,
        billing_event_id, invoice_id, invoice_date, transaction_reference_id, transaction_type,
        from_account_id, billing_address_id, payment_due_date, agreement_id
        from billing_events_with_uni_temporal_data where broker_id != 'AWS_INC') inv
    left join (
        select distinct parent_billing_event_id, invoice_id, transaction_reference_id
        from billing_events_with_uni_temporal_data
        where broker_id != 'AWS_INC' and action = 'INVOICED' and transaction_type like 'AWS_REV_SHARE%' ) listing on inv.billing_event_id_seller_rev = listing.parent_billing_event_id
),

--unify disburse_flag across invoice_id
--for those fields where listing fee invoice values are different from the customer invoices, listing fee values are replaced with parent customer invoice values.
invoiced_disbursed_line_items_unified as(
    select invoice_item.billing_event_id,
           coalesce(map_to_buyer_invoice.invoice_date, invoice_item.invoice_date) invoice_date,
           invoice_item.transaction_type,
           coalesce(map_to_buyer_invoice.transaction_reference_id, invoice_item.transaction_reference_id) transaction_reference_id,
           invoice_item.disbursement_date,
           invoice_item.disbursement_id,
           invoice_item.bank_trace_id,
           invoice_item.broker_id,
           invoice_item.product_id,
           coalesce(map_to_buyer_invoice.from_account_id, invoice_item.from_account_id) from_account_id,
           invoice_item.end_user_account_id,
           coalesce(map_to_buyer_invoice.billing_address_id, invoice_item.billing_address_id) billing_address_id,
           invoice_item.invoice_amount,
           coalesce(map_to_buyer_invoice.agreement_id,invoice_item.agreement_id) agreement_id,
           coalesce(map_to_buyer_invoice.invoice_id,invoice_item.invoice_id) invoice_id,
           map_to_listing_fee_invoice.listing_fee_invoice_id,
           coalesce(map_to_buyer_invoice.payment_due_date, invoice_item.payment_due_date) payment_due_date
    from
        disbursed_line_items invoice_item
    left join seller_invoice_id_mapping map_to_buyer_invoice on invoice_item.invoice_id = map_to_buyer_invoice.listing_fee_invoice_id and map_to_buyer_invoice.transaction_type like 'SELLER_REV_SHARE%'
    left join seller_invoice_id_mapping map_to_listing_fee_invoice on invoice_item.invoice_id = map_to_listing_fee_invoice.invoice_id and invoice_item.billing_event_id = map_to_listing_fee_invoice.billing_event_id
),

  -- An invoice can contain multiple line items
  -- We create a pivot table to calculate the different amounts that are part of an invoice
  -- The new row is aggregated at transaction_reference_id
  invoice_amounts_aggregated as (
    select
      transaction_reference_id,
      product_id,
      agreement_id,
      max(billing_address_id) as billing_address_id,
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
      max(case
      -- We get the payer of the invoice from any transaction type that is not AWS and not BALANCE_ADJUSTMENT
      -- For AWS and BALANCE_ADJUSTMENT, the billing event feed will show the "AWS Marketplace" account as the
      -- receiver of the funds and the seller as the payer. We are not interested in this information here.
            when transaction_type not like '%AWS%' and transaction_type not like 'BALANCE_ADJUSTMENT' then from_account_id
          end) as payer_account_id,
      broker_id,
      -- this is the account that used your product (can be same as the one who subscribed if the license was not distributed but used directly)
      end_user_account_id,
      bank_trace_id,
      invoice_date,
      payment_due_date
    from
        invoiced_disbursed_line_items_unified
    group by
      transaction_reference_id,
      product_id,
      agreement_id,
      broker_id,
      -- there might be different end-users for the same transaction reference id. distributed licenses is an example
      end_user_account_id,
      -- there is one and only one bank_trace_id per disbursement
      --   -> this is redundant with "group by disbursement_id" but it is "cleaner" to set it in here than using a min or max in the select part
      bank_trace_id,
      disbursement_id,
      invoice_date,
      payment_due_date
),
-- Payer and End user address preference order: tax address > billing address > mailing address,
-- subscriber address preference order: tax address >  mailing address
invoice_amounts_aggregated_with_sub_address as (
    select inv.*, agg.acceptor_account_id subscriber_account_id,
        offer.opportunity_id,
        coalesce (
        case when acc_pay.tax_address_id = '' then null else acc_pay.tax_address_id end,
        case when inv.billing_address_id = '' then null else inv.billing_address_id end,
        case when acc_pay.mailing_address_id = '' then null else acc_pay.mailing_address_id end) as payer_address_id,
        coalesce (
        case when acc_end.tax_address_id = '' then null else acc_end.tax_address_id end,
        case when inv.billing_address_id = '' then null else inv.billing_address_id end,
        case when acc_end.mailing_address_id = '' then null else acc_end.mailing_address_id end) as end_user_address_id,
        coalesce (
        --empty value in Athena shows as '', change all '' value to null in order to follow the preference order logic above
        case when acc_sub.tax_address_id ='' then null else acc_sub.tax_address_id end,
        case when acc_sub.mailing_address_id = '' then null else acc_sub.mailing_address_id end) as subscriber_address_id
    from invoice_amounts_aggregated inv
    left join agreements_with_history agg on inv.agreement_id = agg.agreement_id
        and (inv.invoice_date >= agg.valid_from and inv.invoice_date < agg.valid_to)
    left join offers_with_history offer on agg.offer_id = offer.offer_id and (inv.invoice_date >= offer.valid_from  and inv.invoice_date < offer.valid_to)
    left join accounts_with_latest_version acc_sub on agg.acceptor_account_id = acc_sub.account_id
    left join accounts_with_latest_version acc_pay on inv.payer_account_id = acc_pay.account_id
    left join accounts_with_latest_version acc_end on inv.end_user_account_id = acc_end.account_id
),

disbursed_amount_by_product as (
  select
  --Payer Information
    payer_info.aws_account_id as "Payer AWS Account ID", -- "Customer AWS Account Number" in legacy report
    pii_payer.company_name as "Payer Company Name",
    pii_payer.email_domain as "Payer Email Domain",
    pii_payer.city as "Payer City",
    pii_payer.state_or_region as "Payer State",
    pii_payer.country as "Payer Country",
    pii_payer.postal_code as "Payer Postal Code",

   --End Customer Information
    customer_info.aws_account_id as "End Customer AWS Account ID",
    pii_customer.company_name as "End Customer Company Name",
    pii_customer.email_domain as "End Customer Email Domain",
    pii_customer.city as "End Customer City",
    pii_customer.state_or_region as "End Customer State",
    pii_customer.country as "End Customer Country",
    pii_customer.postal_code as "End Customer Postal Code",

    --Subscriber Information
    subscriber_info.aws_account_id as "Subscriber AWS Account ID",
    pii_subscriber.company_name as "Subscriber Company Name",
    pii_subscriber.email_domain as "Subscriber Email Domain",
    pii_subscriber.city as "Subscriber City",
    pii_subscriber.state_or_region as "Subscriber State",
    pii_subscriber.country as "Subscriber Country",
    pii_subscriber.postal_code as "Subscriber Postal Code",

    products.title as "Product Title",
    products.product_code as "Product Code",

    -- We used a sub-select to fail the query if it returns more than 1 record (to be on the safe side)
    case when proposer_account_id <> (select seller_account_id from seller_account) then acc_reseller.aws_account_id else null end as "Reseller AWS Account ID",
    -- reseller company name at time of invoice
    case when proposer_account_id <> (select seller_account_id from seller_account ) then acc_reseller.mailing_company_name else null end "Reseller Company Name",

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
    round(invoice_amounts.seller_tax_share, 2) as "Seller Sales Tax",
    round(invoice_amounts.seller_tax_refund, 2) as "Seller Sales Tax Refund",
    invoice_amounts.transaction_reference_id as "Transaction Reference ID",
    offer.opportunity_name as "Opportunity Name",
    offer.opportunity_description as "Opportunity Description",
    invoice_amounts.disbursement_date as "Disbursement Date",
    invoice_amounts.payment_due_date as "Payment Due Date",
    invoice_amounts.bank_trace_id as "Bank Trace ID",
    broker_id as "Broker ID"

  from
    invoice_amounts_aggregated_with_sub_address invoice_amounts
    join products_with_latest_version products on products.product_id = invoice_amounts.product_id
    left join accounts_with_latest_version payer_info on payer_info.account_id = invoice_amounts.payer_account_id
    left join pii_with_latest_revision pii_payer on invoice_amounts.payer_address_id = pii_payer.address_id
    left join accounts_with_latest_version customer_info on customer_info.account_id = invoice_amounts.end_user_account_id
    left join pii_with_latest_revision pii_customer on invoice_amounts.end_user_address_id = pii_customer.address_id
    left join accounts_with_latest_version subscriber_info on subscriber_info.account_id = invoice_amounts.subscriber_account_id
    left join pii_with_latest_revision pii_subscriber on pii_subscriber.address_id = invoice_amounts.subscriber_address_id
    left join (select distinct opportunity_id,opportunity_name,opportunity_description from offers_with_history) offer on invoice_amounts.opportunity_id = offer.opportunity_id
    -- TODO left join because agreement feed is not GA yet and not 100% backfilled yet -> will be moved to INNER join after backfill
    left join agreements_with_history agg on agg.agreement_id = invoice_amounts.agreement_id
        and (invoice_amounts.invoice_date >= agg.valid_from and invoice_amounts.invoice_date < agg.valid_to)
    -- TODO left join because of the account history bug -> will move to inner join when fixed (and agreements are backfilled)
    left join accounts_with_latest_version_with_company_name acc_reseller on agg.proposer_account_id = acc_reseller.account_id
)

select * from disbursed_amount_by_product
-- Filter results to within a 90 trailing period
where "Disbursement Date" > date_add('DAY', -90, current_date)
