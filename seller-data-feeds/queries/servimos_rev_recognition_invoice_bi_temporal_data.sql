-- Revenue Recognition Reporting at Invoice Time

-- DISCLAIMER: This query uses the Agreement Feed, which is currently in private beta.
-- This query will be changed before General Availability (GA).

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
        off.offer_id,
        off.offer_revision,
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
        min(off.name) as name,
        off.valid_from,
        off.valid_to
    from
        offers_with_history off
        -- left joining because public offers don't have targets
        left join offer_targets_with_uni_temporal_data off_tgt on off.offer_id=off_tgt.offer_id and off.offer_revision=off_tgt.offer_revision
    group by
        off.offer_id,
        off.offer_revision,
        --redundant with offer_revision, as each revision has a dedicated valid_from/valid_to (but cleaner in the group by)
        off.valid_from,
        off.valid_to
),
-- provided for reference only if you are interested into get "current" offer targets
--  (ie. not used afterwards)
offers_with_latest_revision_with_target_type as (
    select
        off.offer_id,
        off.offer_revision,
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
        min(off.name) as name,
        off.valid_from,
        off.valid_to
    from
        offers_with_latest_revision off
        -- left joining because public offers don't have targets
        left join offer_targets_with_uni_temporal_data off_tgt on off.offer_id=off_tgt.offer_id and off.offer_revision=off_tgt.offer_revision
    group by
        off.offer_id,
        off.offer_revision,
        -- redundant with offer_revision, as each revision has a dedicated valid_from (but cleaner in the group by)
        off.valid_from,
        off.valid_to
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
        acc.valid_from,
        acc.valid_to
    from accounts_with_history acc
    -- left join because mailing_address_id can be null but when exists
    left join pii_with_latest_revision pii on acc.mailing_address_id=pii.address_id
),


-- A given billing_event_id represents an accounting event and thus has only one valid_from date,
--   but because of bi-temporality, a billing_event_id (+ its valid_from) can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
invoiced_billing_events_with_uni_temporal_data as (
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
        where
            -- for revenue recognition at invoice time, we are only interested at invoiced and forgiven records
            --  (ie, disbursements are skipped)
            action in ('FORGIVEN', 'INVOICED')
    )
    where
        -- keep latest ...
        row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
),
 -- Here we select the account_id of the current seller (We identify this by looking for the to_account_id related to revenue transactions).
 -- We will use it later to distinguish own agreements from agreements generated by channel partners.
 seller_account as (
     select
         from_account_id as seller_account_id
     from
         invoiced_billing_events_with_uni_temporal_data bill
     where
         bill.transaction_type like 'AWS_REV_SHARE' and amount < 0 and action = 'INVOICED'
     group by
         -- from_account_id is always the same for all those "listing fee" transactions == the seller of record himself.
         -- If this view returns more than 1 record, the overall query will fail (on purpose). Please contact AWS Marketplace if this happens.
         from_account_id
 ),
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
        sum(case when transaction_type = 'SELLER_REV_SHARE' and from_account_id = (select seller_account_id from seller_account) then amount else 0 end) as cogs,
        sum(case when transaction_type in ('SELLER_REV_SHARE_REFUND','SELLER_REV_SHARE_CREDIT') and from_account_id = (select seller_account_id from seller_account)  then amount else 0 end) as cogs_refund,
        agreement_id,
        product_id,
        invoice_date_as_date,
        invoice_date,
        invoice_id,
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
        invoiced_billing_events_with_uni_temporal_data
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
        payment_due_date
),

revenue_recognition_at_invoice_time as (

select
   -------------------
   -- Customer Info --
   -------------------

   acc_payer.aws_account_id as "Payer AWS Account ID",
   -- payer company name at time of invoice
   acc_payer.company_name as "Payer Company Name",
   acc_enduser.aws_account_id as "End User AWS Account ID",
   -- end user company name at time of invoice
   acc_enduser.company_name as "End User Company Name",
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
   off.offer_id as "Offer ID",
   -- offer name at time of invoice. It is possible that the name changes over time therefore there may be multiple offer names mapped to a single offer id.
   off.name as "Offer Name",
   -- offer target at time of invoice.
   off.offer_target as "Offer Target",
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
   (    round(inv.gross_revenue,2) +
        round(inv.aws_rev_share,2) +
        round(inv.gross_refunds,2) +
        round(inv.aws_refund_share,2) +
        round(inv.seller_tax_share,2) +
        round(inv.seller_tax_share_refund,2)
   ) as "Seller Net Revenue",
   currency as "Currency"
from invoiced_transactions inv

    -- if you want to get current product title, replace the next join with: left join products_with_latest_revision p on p.product_id = inv.product_id
    join products_with_history p on p.product_id = inv.product_id and (inv.invoice_date_as_date >= p.valid_from  and inv.invoice_date_as_date < p.valid_to)

    left join accounts_with_history_with_company_name acc_payer on inv.payer_account_id = acc_payer.account_id
                                                                    and (inv.invoice_date_as_date >= acc_payer.valid_from  and inv.invoice_date_as_date < acc_payer.valid_to)
    -- left join because end_user_account_id is nullable (eg if the invoice is originated from a reseller)
    left join accounts_with_history_with_company_name acc_enduser on inv.end_user_account_id = acc_enduser.account_id
                                                                    and (inv.invoice_date_as_date >= acc_enduser.valid_from  and inv.invoice_date_as_date < acc_enduser.valid_to)

    -- TODO left join because agreement feed is not GA yet and not 100% backfilled yet -> will be moved to INNER join after backfill
    left join agreements_with_history agg on agg.agreement_id = inv.agreement_id and (inv.invoice_date_as_date >= agg.valid_from  and inv.invoice_date_as_date < agg.valid_to)

    left join accounts_with_history_with_company_name acc_reseller on agg.proposer_account_id = acc_reseller.account_id
                                                                        and (inv.invoice_date_as_date >= acc_reseller.valid_from  and inv.invoice_date_as_date < acc_reseller.valid_to)
    -- if you want to get current offer name, replace the next join with: left join offer_targets_with_latest_revision_with_target_type off on agg.offer_id = off.offer_id
    -- TODO left join because reseller's agreements show offer IDs not exposed in manufacturer's Offer Feed (yet, Nimish seeking Legal Approval)
    left join offers_with_history_with_target_type off on agg.offer_id = off.offer_id and (inv.invoice_date_as_date >= off.valid_from  and inv.invoice_date_as_date < off.valid_to)

)
select * from revenue_recognition_at_invoice_time
-- if you want to filter on a specific month, simply do: where "Invoice Date" between '2021-04-01' and '2021-04-30'
