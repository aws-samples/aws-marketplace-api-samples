-- Revenue Recognition Reporting at Invoice Time

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
        pii.company_name,
        pii.email_domain,
        acc.valid_from,
        acc.valid_to
    from accounts_with_history acc
    -- left join because mailing_address_id can be null but when exists
    left join pii_with_latest_revision pii on acc.mailing_address_id=pii.address_id
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
-- for revenue recognition at invoice time, we are only interested at invoiced and forgiven records
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
    amount,
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
        when disburse.parent_billing_event_id is not null then 'Yes'
        else 'No'
    end as disburse_flag_temp,
    disburse.disbursement_date as disbursement_date,
    - disburse.amount disburse_amount,
    disburse.bank_trace_id disburse_bank_trace_id
    from
        invoiced_billing_events_with_uni_temporal_data invoice
    left join
        disbursed_line_items disburse on invoice.billing_event_id = disburse.parent_billing_event_id),
--unify disburse_flag across invoice_id
invoiced_disbursed_disburse_flag_invoice_unified as(
    select  invoice_item.billing_event_id,
            invoice_item.valid_from,
            invoice_item.update_date,
            invoice_item.delete_date,
            invoice_item.invoice_date_as_date,
            invoice_item.invoice_date,
            invoice_item.transaction_type,
            invoice_item.transaction_reference_id,
            invoice_item.parent_billing_event_id,
            invoice_item.bank_trace_id,
            invoice_item.product_id,
            invoice_item.disbursement_billing_event_id,
            invoice_item.action,
            invoice_item.from_account_id,
            invoice_item.to_account_id,
            invoice_item.end_user_account_id,
            invoice_item.billing_address_id,
            invoice_item.amount,
            invoice_item.currency,
            invoice_item.agreement_id,
            invoice_item.invoice_id,
            invoice_item.payment_due_date,
            invoice_item.usage_period_start_date,
            invoice_item.usage_period_end_date,
           case
               when disbursed_item.invoice_id is null then 'No'
               else 'Yes'
           end as disburse_flag,
           disbursed_item.disbursement_date as disbursement_date,
           disbursed_item.disburse_bank_trace_id as disburse_bank_trace_id
    from
        invoiced_line_items_with_disbursement_info invoice_item
    left join
        (select distinct invoice_id,
                disbursement_date,
                disburse_bank_trace_id
            from invoiced_line_items_with_disbursement_info
            where disburse_flag_temp = 'Yes' ) disbursed_item
        on invoice_item.invoice_id = disbursed_item.invoice_id
) ,
invoiced_transactions as (
  select
        currency,
        -- We are separating Revenue and Cost of Goods Sold below:
        --  customer invoiced seller_rev_share records expose from_account_id=<customer> to_account_id=Seller
        --  while COGS records expose from_account_id=Seller to_account_id=<manufacturer>
        sum(case when transaction_type = 'SELLER_REV_SHARE' and  to_account_id =(select seller_account_id from seller_account)   then amount else 0 end) as gross_revenue,
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
        invoice_id,
        transaction_reference_id,
        disburse_flag,
        disbursement_date,
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
        end_user_account_id,
        usage_period_start_date,
        usage_period_end_date,
        payment_due_date
    from
        invoiced_disbursed_disburse_flag_invoice_unified
    group by
        product_id,
        invoice_date,
        invoice_date_as_date,
        invoice_id,
        currency,
        agreement_id,
        end_user_account_id,
        usage_period_start_date,
        usage_period_end_date,
        -- will always have same value given above grouping fields -> cleaner to group by in here rather than using a max or min in the select
        payment_due_date,
        transaction_reference_id,
        disburse_flag,
        disbursement_date,
        disburse_bank_trace_id
),

revenue_recognition_at_invoice_time as (

select
   -------------------
   -- Customer Info --
   -------------------
   acc_payer.aws_account_id as "Payer AWS Account ID",
   -- payer company name at time of invoice
   acc_payer.company_name as "Payer Company Name",
   acc_payer.email_domain as "Payer Email Domain",
   acc_enduser.aws_account_id as "End User AWS Account ID",
   -- end user company name at time of invoice
   acc_enduser.company_name as "End User Company Name",
   acc_enduser.email_domain as "End User Email Domain",
   acc_enduser.encrypted_account_id as "End User Encrypted Account ID",

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
   offer.offer_target as "Offer Target",
   -- We used a sub-select to fail the query if it returns more than 1 record (to be on the safe side)
   case when agg.proposer_account_id <> (select seller_account_id from seller_account) then acc_reseller.aws_account_id else null end as "Reseller AWS Account ID",
   -- reseller company name at time of invoice
   case when agg.proposer_account_id <> (select seller_account_id from seller_account ) then  acc_reseller.company_name else null end "Reseller Company Name",
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
    disburse_flag as "Disbursement Flag",
    disbursement_date as "Disbursement Date",
    disburse_bank_trace_id as "Disburse Bank Trace Id"

from invoiced_transactions inv

    -- if you want to get current product title, replace the next join with: left join products_with_latest_revision p on p.product_id = inv.product_id
    join products_with_history p on p.product_id = inv.product_id and (inv.invoice_date_as_date >= p.valid_from  and inv.invoice_date_as_date < p.valid_to)


    -- TODO /!\ problem between invoice_date which is < account.valid_from!! -> cannot make it an inner join now
    --   -> change this when https://code.amazon.com/reviews/CR-52453819 is pushed
    left join accounts_with_history_with_company_name acc_payer on inv.payer_account_id = acc_payer.account_id
                                                                    and (inv.invoice_date_as_date >= acc_payer.valid_from  and inv.invoice_date_as_date < acc_payer.valid_to)
    -- left join because end_user_account_id is nullable (eg if the invoice is originated from a reseller)
    left join accounts_with_history_with_company_name acc_enduser on inv.end_user_account_id = acc_enduser.account_id
                                                                    and (inv.invoice_date_as_date >= acc_enduser.valid_from  and inv.invoice_date_as_date < acc_enduser.valid_to)

    -- TODO left join because agreement feed is not GA yet and not 100% backfilled yet -> will be moved to INNER join after backfill
    left join agreements_with_history agg on agg.agreement_id = inv.agreement_id and (inv.invoice_date_as_date >= agg.valid_from  and inv.invoice_date_as_date < agg.valid_to)
    -- TODO left join because of the account history bug -> will move to inner join when fixed (and agreements are backfilled)
    left join accounts_with_history_with_company_name acc_reseller on agg.proposer_account_id = acc_reseller.account_id
                                                                        and (inv.invoice_date_as_date >= acc_reseller.valid_from  and inv.invoice_date_as_date < acc_reseller.valid_to)
    -- if you want to get current offer name, replace the next join with: left join offer_targets_with_latest_revision_with_target_type off on agg.offer_id = off.offer_id
    -- TODO left join because reseller's agreements show offer IDs not exposed in manufacturer's Offer Feed (yet, Nimish seeking Legal Approval)
    left join offers_with_history_with_target_type offer on agg.offer_id = offer.offer_id and (inv.invoice_date_as_date >= offer.valid_from  and inv.invoice_date_as_date < offer.valid_to)

)
select *
from revenue_recognition_at_invoice_time
-- Filter results to within a 90 trailing period
where cast(date_parse("Invoice Date",'%Y-%m-%dT%H:%i:%SZ') as date) > date_add('DAY', -90, current_date)
-- where "Invoice Date" >= '2021-08-01' and "Invoice Date"< '2021-10-15'
/* **** TEST CASES ****
We don;t have a good framework to test those queries and I thought it is important for others to understand how to test the above if some changes are needed.
So instead of relying on CRs description, I added in here the list of things to test and how it can be tested.



With Cisco's data

1.1 "legacy forgiven" = transaction with just SELLER_REV_SHARE + AWS_REV_SHARE (+optional tax) being forgiven:
    invoice_id='136087252', all amounts = 0. OK

1.2. "modern forgiven" = transaction with SELLER_REV_SHARE + AWS_REV_SHARE (+optional tax) AND *_REFUNDS, all being forgiven
    invoice_id='656377197', all amounts =0. OK

2. refund
    invoice_id='132623096'
    amounts ok (in good columns)

3.1. Public Offer (with seevral version)
    offer_id='offer-rvsarcu6pv2wg' 2 versions
    invoice_id='728428589'
    amounts are OK

3.2. Private Offer targeted to several accounts
    offer_id='offer-dgvvhrmzgydsy', 2 versions, 2 accounts being targeted
    invoice_id='742804817'
    amounts are OK

3.3. geo targeted offer to several countries
    offer_id='offer-v2bc5u3w3u7ns', 12 versions, 34 countries
    agmt-atl9ftw4dqiis6vjnv1jaav1h
    invoice_id='748711081'
    amounts are OK

3.4. Program Targeted offer
    offer_id='offer-dsb3hdebcy6fc', 5 versions
    agmt-eew8hozdaqiqh0z8ha6xz843o
    invoice_id='712081605'
    amounts are OK

3.5. FDP
    oops, there are agreements but no invoice with a backfilled agreement id for FDP:
       select * from billingeventfeed_v1
       where
       agreement_id in (select distinct agg.agreement_id from agreementfeed agg
            join offertargetfeed_v1 ot on agg.origin_offer_id=ot.offer_id
            where
            target_type='ParticipatingPrograms' and value='FieldDemonstration')
    --> I cannot test
    (when agreements are backfilled, invoice_id=287090957 should be an FDP)

4.1 product title changed between invoice time and now
    product_id='prod-63prromhvvjqu' changed 3 times name in Feb, but only sales as of May.
    I could not find a product who changed title while being on sale...
    but anyways, amounts are correct (ie not duplciated/multiplied...)

4.2. offer name changed between invoice time and now << NOT possible today via the UI (will be with CAPI by eoy)! Will rely on 4.1 test case as the logic is 100% symmetrical
4.3. offer target changed between invoice time and now << NOT possible today via the UI (will be with CAPI by eoy)! Will rely on 4.1 test case as the logic is 100% symmetrical
4.4. one company name changed between invoice time and now; payer or enduser or reseller are all symmetrical -> just tested 1 of those.

4.5. agreement is amended between invoice time and now (e.g. end_date changes)
    identified by
    select agreement_id, count(1) from agreements_with_history
    where agreement_id in (select agreement_id from billingeventfeed_v1 where action='INVOICED' and transaction_type='SELLER_REV_SHARE' group by agreement_id having count(1) > 1)
    group by agreement_id
    having count(1) > 1


    agmt-bm1ryibnmqht041iqcqognvp8 : 2 revisions, rev2 valid_from = 2021-04-28, rev1 had an end_date of  2999-01-01 while rev2 end_date = 2021-04-28 (this generally means agreement is cancelled)
    2 invoices : invoice_id=721442365 2021-04-03
    invoice_id=740138937 2021-05-03


    rev rec sees 2 records, with correct amounts, and invoice_id=740138937 correctly shows agreemnt_end_date=2021-04-28T17:42:48Z while previous invoice shows end_date=2999

5.1 SoR is Manufacturer with a sale done by Channel Partner
    invoice_id=743263457, amount are OK (especially COGS=0)
    amounts double checked in subledgers

5.2 SoR is Channel Partner with a invoice for a product owned by a Mnufacturer -> COGS >0
    /!\ Cisco is NOT a channel partner-> tested with Eplus
    invoice_id=603499941
    amounts double checked in subledgers

6. invoice with and without agreement link (they are not backfilled yet)
    ok

7.1. usage-based invoice (with usage period spanning previous month)
7.2 OOCB (with usage period being a single point in time)

8. constraint on the outputs of the query:
select * from revenue_recognition_at_invoice_time where not (
1=1
-- # checking payer account
and "Payer AWS Account ID" is not null and length("Payer AWS Account ID") =12 -- 0 lines
-- and "Payer Company Name" is nullable (e.g. B2C)


-- # checking product
and product_id is not null and product_title is not null -- 0 lines

-- # checking end user + offer + reseller

and (
-- so far, they are exclusive: if reseller, then no end user and no offer, and vice versa
    ("End User AWS Account ID" is not null and length("End User AWS Account ID") = 12
    -- "End User Company Name" is nullable (e.g. B2C customer)
    and "End User Encrypted Account ID" is not null
    and offer_id is not null
    -- offer_name can be null for public offers or legacy private offers
    and offer_target is not null
    and "Reseller AWS Account No" is null and "Reseller Company Name" is null
        )
  OR
    ( agreement_id is null -- damn backfill not done yet! TODO remove after backfill
    OR
    ("Reseller AWS Account No" is not null and length("Reseller AWS Account No") =12 and "Reseller Company Name" is not null)
    )
) -- 0 lines

-- # checking agreement metadata
and (agreement_id is null -- damn backfill not done yet! TODO remove after backfill
OR
    (agreement_id is not null
    -- and agreement_revision is not null -- no revision yet
    and agreement_start_date is not null and agreement_end_date is not null and agreement_acceptance_date is not null
    )
)-- 0 lines


-- # checking invoicing metadata
and invoice_id is not null and invoice_date is not null and usage_period_start_date is not null and usage_period_end_date is not null
and payment_due_date is not null
-- 0 lines


-- # checking amounts
and "Seller Gross Rev Share" is not null and "Seller Gross Refund" is not null
and "Listing Fee" is not null and "Listing Fee Refund" is not null
and "AWS Tax Share" is not null and "AWS Tax Share Refund" is not null
and "Seller Tax Share" is not null and "Seller Tax Share Refund" is not null
and "COGS" is not null and "COGS Refund" is not null
and "Seller Net Revenue" is not null
-- 0 lines

)
*/