-- Revenue Recognition Reporting at Disbursement Time

-- Comparison with servimos_rev_recognition_invoice_bi_temporal_data.sql:
--  * Similarities:
--      1. Both reports provide information for c, subscribers, products, procurement, and revenue
--      2. Both reports show customer details at time of invoice (mainly for tax purposes)
--  * Differences:
--      1. servimos_rev_recognition_invoice_bi_temporal_data.sql provide above information for both disbursed and undisbursed invoices while this report only includes disbursed invoices.
--      2. servimos_rev_recognition_invoice_bi_temporal_data.sql date range is based on invoice dates while this report's date range is based on disbursement date.

-- Comparison with servimos_rev_recognition_subcription_bi_temporal_data.sql:
--  * Similarities: Both reports provide information for subscribers, products and procurement
--  * Differences:
--      1. This report has additional info for payers, end customers, and revenue for disbursed invoices. servimos_rev_recognition_subcription_bi_temporal_data
--      2. servimos_rev_recognition_subcription_bi_temporal_data date range is based on subscription dates while this report's date range is based on disbursement date.


-- General note: When executing this query we are assuming that the data ingested in the database is using
-- two time axes (the valid_from column and the update_date column).
-- See documentation for more details: https://docs.aws.amazon.com/marketplace/latest/userguide/data-feed.html#data-feed-details

-- A product_id has several valid_from dates (each representing a product revision),
--   but because of bi-temporality, each product_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
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
        -- keep latest ...
        row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
),
-- Here, we build the validity time range (adding valid_to on top of valid_from) of each product revision.
-- We will use it to get the product title at invoice time.
-- NB: If you'd rather get "current" product title, un-comment "products_with_latest_revision"
products_with_history as (
    select
        product_id,
        title,
        from_iso8601_timestamp(valid_from) as valid_from,
        coalesce(
            lead(from_iso8601_timestamp(valid_from)) over (partition by product_id order by from_iso8601_timestamp(valid_from) asc),
            timestamp '2999-01-01 00:00:00'
        ) as valid_to
    from products_with_uni_temporal_data
),
-- provided for reference only if you are interested into get "current" product title
--  (ie. not used afterwards)
products_with_latest_revision as (
    select
        *
    from
    (
        select
            product_id,
            title,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY from_iso8601_timestamp(valid_from) desc) as row_num_latest_revision
        from
            products_with_uni_temporal_data
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
            valid_from,
            null valid_to,
            ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY from_iso8601_timestamp(valid_from) desc) as row_num_latest_revision
        from
         offers_with_uni_temporal_data
    )
    where
        row_num_latest_revision = 1
),

-- An offer_target_id has several valid_from dates (each representing an offer revision)
--   but because of bi-temporality, an offer_target_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
offer_targets_with_uni_temporal_data as (
    select
        *
    from
    (
        select
            *,
            ROW_NUMBER() OVER (PARTITION BY offer_target_id, valid_from ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
        from
            offertargetfeed_v1
    )
    where
        -- keep latest ...
        row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
),


offers_with_history_with_target_type as (
    select
        offer.offer_id,
        offer.offer_revision,
        -- even though today it is not possible to combine several types of targeting in a single offer, let's ensure the query is still predictable if this gets possible in the future
        max(
            distinct
            case
                when off_tgt.target_type is null then 'Public'
                when off_tgt.target_type='BuyerAccounts' then 'Private'
                when off_tgt.target_type='ParticipatingPrograms' then 'Program:'||cast(off_tgt.value as varchar)
                when off_tgt.target_type='CountryCodes' then 'GeoTargeted'
                -- well, there is no other case today, but rather be safe...
                else 'Other Targeting'
            end
        ) as offer_target,
        min(offer.name) as name,
        min(offer.opportunity_name) as opportunity_name,
        min(offer.opportunity_description) as opportunity_description,
        offer.valid_from,
        offer.valid_to
    from
        offers_with_history offer
        -- left joining because public offers don't have targets
        left join offer_targets_with_uni_temporal_data off_tgt on offer.offer_id=off_tgt.offer_id and offer.offer_revision=off_tgt.offer_revision
    group by
        offer.offer_id,
        offer.offer_revision,
        --redundant with offer_revision, as each revision has a dedicated valid_from/valid_to (but cleaner in the group by)
        offer.valid_from,
        offer.valid_to
),
-- provided for reference only if you are interested into get "current" offer targets
--  (ie. not used afterwards)
offers_with_latest_revision_with_target_type as (
    select
        offer.offer_id,
        offer.offer_revision,
        -- even though today it is not possible to combine several types of targeting in a single offer, let's ensure the query is still predictable if this gets possible in the future
        max(
            distinct
            case
                when off_tgt.target_type is null then 'Public'
                when off_tgt.target_type='BuyerAccounts' then 'Private'
                when off_tgt.target_type='ParticipatingPrograms' then 'Program:'||cast(off_tgt.value as varchar)
                when off_tgt.target_type='CountryCodes' then 'GeoTargeted'
                -- well, there is no other case today, but rather be safe...
                else 'Other Targeting'
            end
        ) as offer_target,
        min(offer.name) as name,
        min(offer.opportunity_name) as opportunity_name,
        min(offer.opportunity_description) as opportunity_description,
        offer.valid_from,
        offer.valid_to
    from
        offers_with_latest_revision offer
        -- left joining because public offers don't have targets
        left join offer_targets_with_uni_temporal_data off_tgt on offer.offer_id=off_tgt.offer_id and offer.offer_revision=off_tgt.offer_revision
    group by
        offer.offer_id,
        offer.offer_revision,
        -- redundant with offer_revision, as each revision has a dedicated valid_from (but cleaner in the group by)
        offer.valid_from,
        offer.valid_to
),


-- An account_id has several valid_from dates (each representing a separate revision of the data)
--   but because of bi-temporality, an account_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
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
        -- keep latest ...
        row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
),

-- Here, we build the validity time range (adding valid_to on top of valid_from) of each account revision.
-- We will use it to get account metadata at invoice time.
accounts_with_history as (
    select
        account_id,
        -- sometimes, this columns gets imported as a "bigint" and loses heading 0s -> casting to a char and re-adding heading 0s (if need be)
        substring('000000000000'||cast(aws_account_id as varchar),-12) aws_account_id,
        encrypted_account_id,
        mailing_address_id,
        tax_address_id,
        from_iso8601_timestamp(valid_from) as valid_from,
        coalesce(
            lead(from_iso8601_timestamp(valid_from)) over (partition by account_id order by from_iso8601_timestamp(valid_from) asc),
            timestamp '2999-01-01 00:00:00'
        ) as valid_to
    from
        accounts_with_uni_temporal_data
),


-- An address_id has several valid_from dates (each representing a separate revision of the data)
--   but because of bi-temporality, an account_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
pii_with_uni_temporal_data as (
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
        -- keep latest ...
        row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
),

-- We are only interested in the most recent tuple (BTW: a given address is not supposed to chaneg over time but when bugs ;-) so this query mainly does nothing)
pii_with_latest_revision as (
    select
        *
    from
    (
        select
            *,
            ROW_NUMBER() OVER (PARTITION BY address_id ORDER BY from_iso8601_timestamp(valid_from) desc) as row_num_latest_revision
        from
            pii_with_uni_temporal_data
    )
    where
        row_num_latest_revision = 1
),

-- enrich each account history record with company name from mailing_address
accounts_with_history_with_company_name as (
    select
        acc.account_id,
        acc.aws_account_id,
        acc.encrypted_account_id,
        acc.mailing_address_id,
        acc.tax_address_id,
        pii_mailing.company_name as mailing_company_name,
        pii_mailing.email_domain,
        acc.valid_from,
        acc.valid_to
    from accounts_with_history acc
    -- left join because mailing_address_id can be null but when exists
    left join pii_with_latest_revision pii_mailing on acc.mailing_address_id=pii_mailing.address_id
),


-- A given billing_event_id represents an accounting event and thus has only one valid_from date,
--   but because of bi-temporality, a billing_event_id (+ its valid_from) can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
billing_events_with_uni_temporal_data as (
    select
        *
    from
    (
        select
            billing_event_id,
            from_iso8601_timestamp(valid_from) as valid_from,
            from_iso8601_timestamp(update_date) as update_date,
            delete_date,
            from_iso8601_timestamp(invoice_date) as invoice_date_as_date,
            invoice_date,
            transaction_type,
            transaction_reference_id,
            parent_billing_event_id,
            bank_trace_id,
            --As broker_id has not been backfilled, manually fill up this column
            case when broker_id is null or broker_id = '' then 'AWS_INC' else broker_id end as broker_id,
            product_id,
            disbursement_billing_event_id,
            action,
            from_account_id,
            to_account_id,
            end_user_account_id,
            billing_address_id,
            -- casting in case data was imported as varchar
            CAST(amount as decimal(20, 10)) as amount,
            currency,
            agreement_id,
            invoice_id,
            payment_due_date,
            usage_period_start_date,
            usage_period_end_date,
            ROW_NUMBER() OVER (PARTITION BY billing_event_id, valid_from ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
        from
            billingeventfeed_v1
    )
    where
        -- keep latest ...
        row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
),

-- for revenue recognition at disbursement time, we are only interested at invoiced and forgiven records
--  (ie, disbursements are skipped)
invoiced_billing_events_with_uni_temporal_data as (
    select *
    from billing_events_with_uni_temporal_data
    where action in ('FORGIVEN', 'INVOICED')
),
-- Get billing event id for 'DISBURSEMENT'
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
    line_items.billing_event_id,
    line_items.transaction_reference_id,
    line_items.parent_billing_event_id,
    line_items.product_id,
    line_items.transaction_type,
    line_items.amount,
    --Currently, partial payment is not allowed, but will be available in future, this is to identify per billing_event_id, how much has been disbursed in total
    sum(line_items.amount) over (partition by line_items.parent_billing_event_id) disbursed_amount_per_parent,
    --Similarly, when partial payment is implemented, this will catch the most recent date of disbursement
    --Note: format is subject to change when partial payment is implemented as we are also planning to show all previous disbursed dates if a billing_event_id is disbursed for more than 1 time
    max(disbursements.disbursement_date) over (partition by line_items.parent_billing_event_id) last_disbursement_date,
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

 -- Here we select the account_id of the current seller (We identify this by looking for the to_account_id related to revenue transactions).
 -- We will use it later to distinguish own agreements from agreements generated by channel partners.
 seller_account as (
     select
         from_account_id as seller_account_id
     from
         invoiced_billing_events_with_uni_temporal_data bill
     where
        -- Assumption here is only seller will pay listing fee. As of 12/21/2021, there are cases that Channel partner have 0 listing fee for CPPO, so the amount could be 0.
         bill.transaction_type like 'AWS_REV_SHARE' and amount <= 0 and action = 'INVOICED'
     group by
         -- from_account_id is always the same for all those "listing fee" transactions == the seller of record himself.
         -- If this view returns more than 1 record, the overall query will fail (on purpose). Please contact AWS Marketplace if this happens.
         from_account_id
 ),
--Join invoiced_item with disbursement_item to find out if the invoice has been disbursed
--'DISBURSED' action does not have 'AWS_TAX_SHARE' AND 'AWS_TAX_SHARE_REFUND' transaction_type, therefore, for billing_event_id with 'AWS_TAX_SHARE' AND 'AWS_TAX_SHARE_REFUND' transaction_type, we can't identify if it has been disbursed.
--Will unify disburse_flag based on invoice_id next step.
invoiced_line_items_with_disbursement_info  as(
    select invoice.*,
    case
        when disburse.parent_billing_event_id is not null and - disburse.disbursed_amount_per_parent = invoice.amount then 'Yes'
        when disburse.parent_billing_event_id is not null and - disburse.disbursed_amount_per_parent <> invoice.amount then 'Partial'
        else 'No'
    end as disburse_flag_temp,
    disburse.last_disbursement_date,
    - disburse.disbursed_amount_per_parent disburse_amount,
    disburse.bank_trace_id disburse_bank_trace_id
    from
        invoiced_billing_events_with_uni_temporal_data invoice
    left join
        (select distinct parent_billing_event_id, disbursed_amount_per_parent, last_disbursement_date, bank_trace_id from disbursed_line_items )disburse on invoice.billing_event_id = disburse.parent_billing_event_id),

--When broker_id is 'AWS_EUROPE',invoice_id of listing fee is different from invoice_id of seller revenue, these 2 invoice_ids are linked through parent_billing_event_id and billing_event_id. Adding column 'Listing Fee invoice ID' to provide the mapping between 2 invoice_ids
--To map all amounts to the seller invoices, parent_billing_event_id is used when the transaction type is not SELLER_REV_SHARE
--Below CTE maps the listing fee invoices to the related buyer invoice.
--It's also used to override listing fee invoice values with the buyer invoice values for the fields that those values are different
--(e.x. invoice_date, billing_address_id, transaction_reference_id, from_account_id, etc.)
seller_invoice_id_mapping as(
    select distinct
        inv.invoice_id,
        inv.billing_event_id, inv.invoice_date_as_date, inv.invoice_date,
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
        billing_event_id, invoice_id, invoice_date_as_date, invoice_date, transaction_reference_id, transaction_type,
        from_account_id, billing_address_id, payment_due_date, agreement_id
        from billing_events_with_uni_temporal_data where broker_id != 'AWS_INC') inv
    left join (
        select distinct parent_billing_event_id, invoice_id, transaction_reference_id
        from billing_events_with_uni_temporal_data
        where broker_id != 'AWS_INC' and action = 'INVOICED' and transaction_type like 'AWS_REV_SHARE%' ) listing on inv.billing_event_id_seller_rev = listing.parent_billing_event_id
     ),
--unify disburse_flag across invoice_id
--for those fields where listing fee invoice values are different from the customer invoices, listing fee values are replaced with parent customer invoice values.
invoiced_disbursed_disburse_flag_invoice_unified as(
    select invoice_item.billing_event_id,
           invoice_item.valid_from,
           invoice_item.update_date,
           invoice_item.delete_date,
           coalesce(map_to_buyer_invoice.invoice_date_as_date,invoice_item.invoice_date_as_date) invoice_date_as_date,
           coalesce(map_to_buyer_invoice.invoice_date, invoice_item.invoice_date) invoice_date,
           invoice_item.transaction_type,
           coalesce(map_to_buyer_invoice.transaction_reference_id, invoice_item.transaction_reference_id) transaction_reference_id,
           invoice_item.parent_billing_event_id,
           invoice_item.bank_trace_id,
           invoice_item.broker_id,
           invoice_item.product_id,
           invoice_item.disbursement_billing_event_id,
           invoice_item.action,
           coalesce(map_to_buyer_invoice.from_account_id, invoice_item.from_account_id) from_account_id,
           invoice_item.to_account_id,
           invoice_item.end_user_account_id,
           coalesce(map_to_buyer_invoice.billing_address_id, invoice_item.billing_address_id) billing_address_id,
           invoice_item.amount,
           invoice_item.currency,
           coalesce(map_to_buyer_invoice.agreement_id,invoice_item.agreement_id) agreement_id,
           coalesce(map_to_buyer_invoice.invoice_id,invoice_item.invoice_id) invoice_id,
           map_to_listing_fee_invoice.listing_fee_invoice_id,
           coalesce(map_to_buyer_invoice.payment_due_date, invoice_item.payment_due_date) payment_due_date,
           invoice_item.usage_period_start_date,
           invoice_item.usage_period_end_date,
           case
               when disbursed_item.invoice_id is null then 'No'
               when disbursed_item.disburse_flag_temp = 'Partial' then 'Partial'
               else 'Yes'
           end as disburse_flag,
           disbursed_item.last_disbursement_date,
           disbursed_item.disburse_bank_trace_id
    from
        invoiced_line_items_with_disbursement_info invoice_item
    left join
        (select distinct invoice_id,
                disburse_flag_temp,
                disburse_bank_trace_id,
                last_disbursement_date
            from invoiced_line_items_with_disbursement_info
            where disburse_flag_temp in ('Yes','Partial') ) disbursed_item
        on invoice_item.invoice_id = disbursed_item.invoice_id
    left join seller_invoice_id_mapping map_to_buyer_invoice on invoice_item.invoice_id = map_to_buyer_invoice.listing_fee_invoice_id and map_to_buyer_invoice.transaction_type like 'SELLER_REV_SHARE%'
    left join seller_invoice_id_mapping map_to_listing_fee_invoice on invoice_item.invoice_id = map_to_listing_fee_invoice.invoice_id and invoice_item.billing_event_id = map_to_listing_fee_invoice.billing_event_id
),
invoiced_transactions_disbursed_disburse_flag_invoice_unified as (
    select
        currency,
        -- We are separating Revenue and Cost of Goods Sold below:
        --  customer invoiced seller_rev_share records expose from_account_id=<customer> to_account_id=Seller
        --  while COGS records expose from_account_id=Seller to_account_id=<manufacturer>
        sum(case when transaction_type = 'SELLER_REV_SHARE' and  to_account_id =(select seller_account_id from seller_account) then amount else 0 end) as gross_revenue,
        sum(case when transaction_type = 'AWS_REV_SHARE' then amount else 0 end) as aws_rev_share,
        -- _CREDIT is a form of refunds, hence we club those 2 together
        sum(case when transaction_type in ('SELLER_REV_SHARE_REFUND','SELLER_REV_SHARE_CREDIT') and to_account_id =(select seller_account_id from seller_account) then amount else 0 end) as gross_refunds,
        sum(case when transaction_type in ('AWS_REV_SHARE_REFUND','AWS_REV_SHARE_CREDIT') then amount else 0 end) as aws_refund_share,
        sum(case when transaction_type = 'AWS_TAX_SHARE' then amount else 0 end) as aws_tax_share,
        sum(case when transaction_type = 'AWS_TAX_SHARE_REFUND' then amount else 0 end) as aws_tax_share_refund,
        sum(case when transaction_type = 'SELLER_TAX_SHARE' then amount else 0 end) as seller_tax_share,
        sum(case when transaction_type = 'SELLER_TAX_SHARE_REFUND' then amount else 0 end) as seller_tax_share_refund,
        -- NB: the 2 following cost of goods records will be 0 if the seller is not a channel partner
        --set COGS/COGS refund =0 the when buyer is the seller itself
        sum(case when transaction_type = 'SELLER_REV_SHARE' and from_account_id = (select seller_account_id from seller_account) and to_account_id <> (select seller_account_id from seller_account) then amount else 0 end) as cogs,
        sum(case when transaction_type in ('SELLER_REV_SHARE_REFUND','SELLER_REV_SHARE_CREDIT') and from_account_id = (select seller_account_id from seller_account) and to_account_id <> (select seller_account_id from seller_account)  then amount else 0 end) as cogs_refund,
        agreement_id,
        product_id,
        invoice_date_as_date,
        invoice_date,
        listing_fee_invoice_id,
        invoice_id,
        broker_id,
        transaction_reference_id,
        disburse_flag,
        last_disbursement_date,
        disburse_bank_trace_id,
        max(
            case
                -- For AWS and BALANCE_ADJUSTMENT, the billing event feed will show the "AWS Marketplace" account as the
                -- receiver of the funds and the seller as the payer. We are not interested in this information here.
                -- Null values will be ignored by the `max` aggregation function.
                when transaction_type like 'AWS%' or transaction_type like 'BALANCE_ADJUSTMENT'
                then null
                -- We get the payer of the invoice from *any* transaction type that is not AWS and not BALANCE_ADJUSTMENT (because they are the same for a given end user + agreement + product).
                else from_account_id
            end
            ) as payer_account_id,
        -- A transaction will have the same billing_address_id for all of its line items.
         --Note: Currently, only transaction_type like 'SELLER_%' have billing_address_id exposed. Make adjustment on the max() selection when billing_address_id of other transaction_types exposed.
        max(billing_address_id) as billing_address_id,
        end_user_account_id,
        usage_period_start_date,
        usage_period_end_date,
        payment_due_date
    from
    invoiced_disbursed_disburse_flag_invoice_unified
    -- filter added to only include disbursed invoices
    where disburse_flag in ('Yes', 'Partial')
    group by
        product_id,
        invoice_date,
        invoice_date_as_date,
        listing_fee_invoice_id,
        invoice_id,
        broker_id,
        currency,
        agreement_id,
        end_user_account_id,
        usage_period_start_date,
        usage_period_end_date,
        -- will always have same value given above grouping fields -> cleaner to group by in here rather than using a max or min in the select
        payment_due_date,
        transaction_reference_id,
        disburse_flag,
        last_disbursement_date,
        disburse_bank_trace_id
),

--add subscriber and payer addressID, payer address preference order: tax address > billing address > mailing address,  subscriber address preference order: tax address >  mailing address
invoiced_disbursed_disburse_flag_invoice_unified_with_sub_address as (
  select inv.*, agg.acceptor_account_id subscriber_account_id,
         coalesce (
             --empty value in Athena shows as '', change all '' value to null in order to follow the preference order logic above
             case when acc_acp.tax_address_id ='' then null else acc_acp.tax_address_id end,
             case when acc_acp.mailing_address_id = '' then null else acc_acp.mailing_address_id end) as subscriber_address_id,
         coalesce (
             case when acc_pay.tax_address_id = '' then null else acc_pay.tax_address_id end,
             case when inv.billing_address_id = '' then null else inv.billing_address_id end,
             case when acc_pay.mailing_address_id = '' then null else acc_pay.mailing_address_id end) as payer_address_id,
         coalesce (
             case when acc_end.tax_address_id = '' then null else acc_end.tax_address_id end,
             case when inv.billing_address_id = '' then null else inv.billing_address_id end,
             case when acc_end.mailing_address_id = '' then null else acc_end.mailing_address_id end) as end_user_address_id
  from invoiced_transactions_disbursed_disburse_flag_invoice_unified inv
  left join agreements_with_history agg on inv.agreement_id = agg.agreement_id
                                               and (inv.invoice_date_as_date >= agg.valid_from and inv.invoice_date_as_date < agg.valid_to)
  left join accounts_with_history_with_company_name acc_acp on agg.acceptor_account_id = acc_acp.account_id
                                               and (inv.invoice_date_as_date >= acc_acp.valid_from  and inv.invoice_date_as_date < acc_acp.valid_to)
  left join accounts_with_history_with_company_name acc_pay on inv.payer_account_id = acc_pay.account_id
                                               and (inv.invoice_date_as_date >= acc_pay.valid_from  and inv.invoice_date_as_date < acc_pay.valid_to)
  left join accounts_with_history_with_company_name acc_end on inv.end_user_account_id = acc_end.account_id
                                               and (inv.invoice_date_as_date >= acc_end.valid_from  and inv.invoice_date_as_date < acc_end.valid_to)
),

-- Channel partner offers do not exist in offertargetfeed_v1 table (as per legal requirement), causing cppo offer be defined as 'Public' in previous step, we will convert them back to 'Private' in next step
cppo_offer_id as(
  select distinct offer_id
  from offers_with_uni_temporal_data
  where
    -- seller_account_id is null means the ISV owns the offer
    seller_account_id is not null
    and seller_account_id !=(select seller_account_id from seller_account)
),

revenue_recognition_at_disbursement_time as (

select
    --Payer Information
    acc_payer.aws_account_id as "Payer AWS Account ID", -- "Customer AWS Account Number" in legacy report
    acc_payer.encrypted_account_id as "Payer Encrypted Account ID",
    pii_payer.company_name as "Payer Company Name",
    pii_payer.email_domain as "Payer Email Domain",
    pii_payer.city as "Payer City",
    pii_payer.state_or_region as "Payer State",
    pii_payer.country as "Payer Country",
    pii_payer.postal_code as "Payer Postal Code",

   --End Customer Information
    acc_enduser.aws_account_id as "End Customer AWS Account ID",
    acc_enduser.encrypted_account_id as "End User Encrypted Account ID",
    pii_end_user.company_name as "End Customer Company Name",
    pii_end_user.email_domain as "End Customer Email Domain",
    pii_end_user.city as "End Customer City",
    pii_end_user.state_or_region as "End Customer State",
    pii_end_user.country as "End Customer Country",
    pii_end_user.postal_code as "End Customer Postal Code",

    --Subscriber Information
    acc_subscriber.aws_account_id as "Subscriber AWS Account ID",
    acc_subscriber.encrypted_account_id as "Subscriber Encrypted Account ID",
    pii_subscriber.company_name as "Subscriber Company Name",
    pii_subscriber.email_domain as "Subscriber Email Domain",
    pii_subscriber.city as "Subscriber City",
    pii_subscriber.state_or_region as "Subscriber State",
    pii_subscriber.country as "Subscriber Country",
    pii_subscriber.postal_code as "Subscriber Postal Code",

   ------------------
   -- Product Info --
   ------------------
   inv.product_id as "Product ID",
   -- product title at time of invoice. It is possible that the title changes over time and therefore there may be multiple product titles mapped to a single product id.
   p.title as "Product Title",

   ----------------------
   -- Procurement Info --
   ----------------------
   offer.offer_id as "Offer ID",
   -- offer name at time of invoice. It is possible that the name changes over time therefore there may be multiple offer names mapped to a single offer id.
   offer.name as "Offer Name",
   -- offer target at time of invoice.
   case when offer.offer_id in (select distinct offer_id from cppo_offer_id) then 'Private' else offer.offer_target end as "Offer Target",
   offer.opportunity_name as "Offer opportunity Name",
   offer.opportunity_description as "Offer opportunity Description",
   -- We used a sub-select to fail the query if it returns more than 1 record (to be on the safe side)
   case when agg.proposer_account_id <> (select seller_account_id from seller_account) then acc_reseller.aws_account_id else null end as "Reseller AWS Account ID",
   -- reseller company name at time of invoice
   case when agg.proposer_account_id <> (select seller_account_id from seller_account ) then  acc_reseller.mailing_company_name else null end "Reseller Company Name",
   agg.agreement_id as "Agreement ID",
   -- all agreement related data are surfaced as they were at time of invoice.
   agg.agreement_revision as "Agreement Revision",
   agg.start_date as "Agreement Start Date",
   agg.end_date as "Agreement End Date",
   agg.acceptance_date as "Agreement Acceptance Date",

   --------------
   -- Revenues --
   --------------
   inv.invoice_id as "Invoice ID",
   inv.listing_fee_invoice_id as "Listing Fee Invoice ID",
   inv.transaction_reference_id as "Transaction Reference ID",
   inv.invoice_date as "Invoice Date",
   inv.usage_period_start_date as "Usage Period Start Date",
   inv.usage_period_end_date as "Usage Period End Date",
   -- We are rounding the sums using 2 decimal precision
   -- Note that the rounding method might differ between SQL implementations.
   -- The monthly revenue report is using RoundingMode.HALF_UP. This might create tiny discrepancies between this SQL output
   -- and the legacy report
   round(inv.gross_revenue,2) as "Gross Revenue",
   round(inv.gross_refunds,2) as "Gross Refund",
   inv.payment_due_date as "Payment Due Date",
   round(inv.aws_rev_share,2) as "Listing Fee",
   round(inv.aws_refund_share,2) as "Listing Fee Refund",
   round(inv.aws_tax_share,2) as "AWS Tax Share",
   round(inv.aws_tax_share_refund,2) as "AWS Tax Share Refund",
   round(inv.seller_tax_share,2) as "Seller Tax Share",
   round(inv.seller_tax_share_refund,2) as "Seller Tax Share Refund",
   round(inv.cogs,2) as "COGS",
   round(inv.cogs_refund,2) as "COGS Refund",
   -- summing rounded amounts to ensure that the calculation is consistent between above figures and this one
   -- net revenue = gross revenue - listing fee - tax - cogs
   (    round(inv.gross_revenue,2) +
        round(inv.aws_rev_share,2) +
        round(inv.gross_refunds,2) +
        round(inv.aws_refund_share,2) +
        round(inv.seller_tax_share,2) +
        round(inv.seller_tax_share_refund,2) +
        round(inv.cogs,2) +
        round(inv.cogs_refund,2)
   ) as "Seller Net Revenue",
   currency as "Currency",

   ------------------
   -- Disbursement --
   ------------------
    -- 'no value' cells in all data feeds show as 'empty string' in Athena, change the possible NULL values generated from previous left join to unify those columns with no value as ''
    case when last_disbursement_date is null then '' else last_disbursement_date end as "Last Disbursement Date",
    case when disburse_bank_trace_id is null then '' else disburse_bank_trace_id end as "Disburse Bank Trace Id",
    broker_id as "Broker ID"

from invoiced_disbursed_disburse_flag_invoice_unified_with_sub_address inv

    -- if you want to get current product title, replace the next join with: left join products_with_latest_revision p on p.product_id = inv.product_id
    join products_with_history p on p.product_id = inv.product_id and (inv.invoice_date_as_date >= p.valid_from  and inv.invoice_date_as_date < p.valid_to)
    left join accounts_with_history_with_company_name acc_payer on inv.payer_account_id = acc_payer.account_id
                                                                    and (inv.invoice_date_as_date >= acc_payer.valid_from  and inv.invoice_date_as_date < acc_payer.valid_to)
    -- left join because end_user_account_id is nullable (eg if the invoice is originated from a reseller)
    left join accounts_with_history_with_company_name acc_enduser on inv.end_user_account_id = acc_enduser.account_id
                                                                    and (inv.invoice_date_as_date >= acc_enduser.valid_from  and inv.invoice_date_as_date < acc_enduser.valid_to)
    left join accounts_with_history_with_company_name acc_subscriber on inv.subscriber_account_id = acc_subscriber.account_id
                                                                    and (inv.invoice_date_as_date >= acc_subscriber.valid_from  and inv.invoice_date_as_date < acc_subscriber.valid_to)
    left join pii_with_latest_revision pii_payer on inv.payer_address_id = pii_payer.address_id
    left join pii_with_latest_revision pii_subscriber on inv.subscriber_address_id = pii_subscriber.address_id
    left join pii_with_latest_revision pii_end_user on inv.end_user_address_id = pii_end_user.address_id
    -- TODO left join because agreement feed is not GA yet and not 100% backfilled yet -> will be moved to INNER join after backfill
    left join agreements_with_history agg on agg.agreement_id = inv.agreement_id and (inv.invoice_date_as_date >= agg.valid_from  and inv.invoice_date_as_date < agg.valid_to)
    -- TODO left join because of the account history bug -> will move to inner join when fixed (and agreements are backfilled)
    left join accounts_with_history_with_company_name acc_reseller on agg.proposer_account_id = acc_reseller.account_id
                                                                        and (inv.invoice_date_as_date >= acc_reseller.valid_from  and inv.invoice_date_as_date < acc_reseller.valid_to)
    -- if you want to get current offer name, replace the next join with: left join offer_targets_with_latest_revision_with_target_type off on agg.offer_id = off.offer_id
    left join offers_with_history_with_target_type offer on agg.offer_id = offer.offer_id and (inv.invoice_date_as_date >= offer.valid_from  and inv.invoice_date_as_date < offer.valid_to)
)
select *
from revenue_recognition_at_disbursement_time
-- Filter results to within a 90 trailing period
where cast(date_parse("Last Disbursement Date",'%Y-%m-%dT%H:%i:%SZ') as date) > date_add('DAY', -90, current_date)
