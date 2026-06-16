-- Usage report

-- General note: When executing this query we are assuming that the data ingested in the database  
-- is using two time axes (the valid_from column and the update_date column).
-- See documentation for more details: https://docs.aws.amazon.com/marketplace/latest/userguide/data-feed.html#data-feed-details
--
-- Each SDDS feed is resolved from bi-temporal to uni-temporal by keeping only the latest
-- update_date per primary key + valid_from, and excluding soft-deleted records.

-- ============================================================================
-- Layer 1: Bi-Temporal Resolution CTEs
-- Resolve each SDDS feed from bi-temporal to uni-temporal model
-- ============================================================================

-- An account_id has several valid_from dates (each representing a separate revision of the data)
-- but because of bi-temporality, an account_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
with accounts_with_uni_temporal_data as (
select
    account_id,
    aws_account_id,
    encrypted_account_id,
    mailing_address_id,
    tax_address_id,
    tax_legal_name,
    from_iso8601_timestamp(valid_from) as valid_from,
    tax_registration_number,
    catalog
from
    (
    select
        -- empty value in Athena shows as '', change all '' value to null
        case when account_id = '' then null else account_id end as account_id,
        case when cast(aws_account_id as varchar) = '' then null else aws_account_id end as aws_account_id,
        case when encrypted_account_id = '' then null else encrypted_account_id end as encrypted_account_id,
        case when mailing_address_id = '' then null else mailing_address_id end as mailing_address_id,
        case when tax_address_id = '' then null else tax_address_id end as tax_address_id,
        case when tax_legal_name = '' then null else tax_legal_name end as tax_legal_name,
        valid_from,
        delete_date,
        case when tax_registration_number = '' then null else tax_registration_number end as tax_registration_number,
        catalog,
        row_number() over (partition by account_id, valid_from order by from_iso8601_timestamp(update_date) desc) as row_num
    from
        accountfeed_v1
    )
where
    -- keep latest ...
    row_num = 1
    -- ... and remove the soft-deleted one.
    and (delete_date is null or delete_date = '')
),

-- An agreement_id has several valid_from dates (each representing an agreement revision)
-- but because of bi-temporality, an agreement_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
agreements_with_uni_temporal_data as (
select
    agreement_id,
    offer_id,
    proposer_account_id,
    acceptor_account_id,
    from_iso8601_timestamp(valid_from) as valid_from,
    case when start_time is null or start_time = '' then null else from_iso8601_timestamp(start_time) end as start_date,
    case when end_time is null or end_time = '' then null else from_iso8601_timestamp(end_time) end as end_date,
    case when acceptance_time is null or acceptance_time = '' then null else from_iso8601_timestamp(acceptance_time) end as acceptance_date,
    intent,
    preceding_agreement_id,
    status,
    status_reason_code,
    currency_code as estimated_charges_currency_code,
    estimated_agreement_value as estimated_charges_net_amount,
    offer_set_id,
    '' as agreement_revision,
    catalog
from
    (
    select
        -- empty value in Athena shows as '', change all '' value to null
        case when agreement_id = '' then null else agreement_id end as agreement_id,
        offer_id,
        proposer_account_id,
        acceptor_account_id,
        valid_from,
        start_time,
        end_time,
        acceptance_time,
        delete_date,
        case when intent = '' then null else intent end as intent,
        case when status = '' then null else status end as status,
        case when status_reason_code = '' then null else status_reason_code end as status_reason_code,
        case when preceding_agreement_id = '' then null else preceding_agreement_id end as preceding_agreement_id,
        case when offer_set_id = '' then null else offer_set_id end as offer_set_id,
        case when currency_code = '' then null else currency_code end as currency_code,
        cast(estimated_agreement_value as decimal(38,6)) as estimated_agreement_value,
        catalog,
        row_number() over (partition by agreement_id, valid_from order by from_iso8601_timestamp(update_date) desc) as row_num
    from
        agreementfeed_v1
    )
where
    -- keep latest ...
    row_num = 1
    -- ... and remove the soft-deleted one.
    and (delete_date is null or delete_date = '')
),

-- An agreement_id + term_id has several valid_from dates (each representing an agreement term)
-- but because of bi-temporality, an agreement_id + term_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
agreement_terms_with_uni_temporal_data as (
select
    agreement_id,
    term_id,
    term_type,
    term_configuration,
    from_iso8601_timestamp(valid_from) as valid_from
from
    (
    select
        -- empty value in Athena shows as '', change all '' value to null
        case when agreement_id = '' then null else agreement_id end as agreement_id,
        term_id,
        case when term_type = '' then null else term_type end as term_type,
        case when term_configuration = '' then null else term_configuration end as term_configuration,
        valid_from,
        delete_date,
        row_number() over (partition by agreement_id, term_id, valid_from order by from_iso8601_timestamp(update_date) desc) as row_num
    from
        agreementtermfeed_v1
    )
where
    -- keep latest ...
    row_num = 1
    -- ... and remove the soft-deleted one.
    and (delete_date is null or delete_date = '')
),

-- An offer_id has several valid_from dates (each representing an offer revision)
-- but because of bi-temporality, an offer_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
offers_with_uni_temporal_data as (
select
    from_iso8601_timestamp(valid_from) as valid_from,
    offer_id,
    offer_revision,
    name,
    expiration_date,
    opportunity_id,
    opportunity_name,
    opportunity_description,
    seller_account_id,
    recipient_account_id,
    catalog
from
    (
    select
        valid_from,
        update_date,
        delete_date,
        offer_id,
        offer_revision,
        case when name = '' then null else name end as name,
        expiration_date,
        case when opportunity_id = '' then null else opportunity_id end as opportunity_id,
        case when opportunity_name = '' then null else opportunity_name end as opportunity_name,
        case when opportunity_description = '' then null else opportunity_description end as opportunity_description,
        case when seller_account_id = '' then null else seller_account_id end as seller_account_id,
        case when recipient_account_id = '' then null else recipient_account_id end as recipient_account_id,
        catalog,
        row_number() over (partition by offer_id, valid_from order by from_iso8601_timestamp(update_date) desc) as row_num
    from
        offerfeed_v1
    )
where
    -- keep latest ...
    row_num = 1
    -- ... and remove the soft-deleted one.
    and (delete_date is null or delete_date = '')
),

-- An offer_target_id has several valid_from dates (each representing an offer revision)
-- but because of bi-temporality, an offer_target_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
offer_targets_with_uni_temporal_data as (
select
    from_iso8601_timestamp(valid_from) as valid_from,
    offer_target_id,
    offer_id,
    offer_revision,
    target_type,
    polarity,
    value,
    catalog
from
    (
    select
        valid_from,
        update_date,
        delete_date,
        offer_target_id,
        offer_id,
        offer_revision,
        case when target_type = '' then null else target_type end as target_type,
        polarity,
        value,
        catalog,
        row_number() over (partition by offer_target_id, valid_from order by from_iso8601_timestamp(update_date) desc) as row_num
    from
        offertargetfeed_v1
    )
where
    -- keep latest ...
    row_num = 1
    -- ... and remove the soft-deleted one.
    and (delete_date is null or delete_date = '')
),

-- A product_id has several valid_from dates (each representing a product revision),
-- but because of bi-temporality, each product_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
products_with_uni_temporal_data as (
select
    from_iso8601_timestamp(valid_from) as valid_from,
    product_id,
    manufacturer_account_id,
    product_code,
    title,
    catalog
from
    (
    select
        valid_from,
        update_date,
        delete_date,
        product_id,
        case when manufacturer_account_id = '' then null else manufacturer_account_id end as manufacturer_account_id,
        case when product_code = '' then null else product_code end as product_code,
        case when title = '' then null else title end as title,
        catalog,
        row_number() over (partition by product_id, valid_from order by from_iso8601_timestamp(update_date) desc) as row_num
    from
        productfeed_v1
    )
where
    -- keep latest ...
    row_num = 1
    -- ... and remove the soft-deleted one.
    and (delete_date is null or delete_date = '')
),

-- Legacy product ID mapping: resolves old product IDs to new ones
-- No bi-temporal resolution needed — just filter by mapping_type and deduplicate
legacy_products as (
select
    legacy_id,
    new_id,
    min(catalog) as min_data_catalog,
    max(catalog) as max_data_catalog
from
    legacyidmappingfeed_v1
where
    mapping_type = 'PRODUCT'
group by
    legacy_id,
    new_id
),

-- A usage_feed_id has several valid_from dates (each representing a usage revision),
-- but because of bi-temporality, each usage_feed_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
usage_with_uni_temporal_data as (
select
    cast(valid_from as timestamp) as valid_from,
    from_iso8601_timestamp(update_date) as usage_reported_date,
    usage_feed_id,
    from_iso8601_timestamp(usage_date) as usage_date,
    product_id,
    case when agreement_id = '' then null else agreement_id end as agreement_id,
    end_user_account_id,
    payer_account_id,
    region,
    dimension_key as resource_type,
    usage_unit as usage_unit_types,
    usage_quantity as usage_units,
    pricing_currency as currency,
    cast(estimated_revenue_in_pricing_currency as double) as estimated_revenue,
    catalog,
    recipient_account_id,
    offer_id,
    usage_rate_per_unit_in_pricing_currency as usage_rate_per_unit,
    charge_item_description
from
    (
    select
        valid_from,
        update_date,
        delete_date,
        usage_feed_id,
        usage_date,
        product_id,
        agreement_id,
        end_user_account_id,
        payer_account_id,
        region,
        dimension_key,
        usage_unit,
        usage_quantity,
        pricing_currency,
        estimated_revenue_in_pricing_currency,
        catalog,
        recipient_account_id,
        offer_id,
        usage_rate_per_unit_in_pricing_currency,
        charge_item_description,
        row_number() over (partition by usage_feed_id, valid_from order by from_iso8601_timestamp(update_date) desc) as row_num
    from
        dailyusagefeed_v1
    )
where
    -- keep latest ...
    row_num = 1
    -- ... and remove the soft-deleted one.
    and (delete_date is null or delete_date = '')
),

-- ============================================================================
-- Layer 2: Historical Validity Ranges
-- Build valid_from/valid_to ranges with backward date extension;
-- join accounts to addresses for company names; resolve offer targets;
-- identify seller account
-- ============================================================================

-- Build validity time ranges for accounts.
-- The first revision is extended to 1970-01-01 to handle backdated agreements.
-- valid_to is computed using LEAD() with a 2999-01-01 terminus.
accounts_with_history as (
with accounts_with_history_with_extended_valid_from as (
    select
        account_id,
        -- sometimes, this column gets imported as a "bigint" and loses heading 0s -> casting to a char and re-adding heading 0s (if need be)
        substring('000000000000'||cast(aws_account_id as varchar),-12) as aws_account_id,
        encrypted_account_id,
        mailing_address_id,
        tax_address_id,
        tax_legal_name,
        -- The start time of account valid_from is extended to '1970-01-01 00:00:00', because:
        -- ... in tax report transformations, some tax line items with invoice_date cannot
        -- ... fall into the default valid time range of the associated account
        case
            when lag(valid_from) over (partition by account_id order by valid_from asc) is null
                then cast('1970-01-01 00:00:00' as TIMESTAMP WITH TIME ZONE)
            else valid_from
        end as valid_from,
        catalog as min_data_catalog,
        catalog as max_data_catalog
    from accounts_with_uni_temporal_data
)
select
    account_id,
    aws_account_id,
    encrypted_account_id,
    mailing_address_id,
    tax_address_id,
    tax_legal_name,
    valid_from,
    coalesce(
        lead(valid_from) over (partition by account_id order by valid_from asc),
        cast('2999-01-01 00:00:00' as TIMESTAMP WITH TIME ZONE)
    ) as valid_to,
    min_data_catalog,
    max_data_catalog
from
    accounts_with_history_with_extended_valid_from
),

-- Keep only the latest revision per address_id.
-- A given address is not supposed to change over time but when bugs happen, this picks the most recent.
address_with_latest_revision as (
select
    from_iso8601_timestamp(valid_from) as valid_from,
    address_id,
    company_name,
    email_domain,
    country_code,
    state_or_region,
    city,
    -- Postal codes are sometimes imported as numbers, make sure here they are all stored as string
    cast(postal_code as varchar) as postal_code,
    cast(null as varchar) as min_data_catalog,
    cast(null as varchar) as max_data_catalog
from
    (
    select
        valid_from,
        update_date,
        delete_date,
        case when address_id = '' then null else address_id end as address_id,
        case when company_name = '' then null else company_name end as company_name,
        email_domain,
        case when country_code = '' then null else country_code end as country_code,
        case when state_or_region = '' then null else state_or_region end as state_or_region,
        case when city = '' then null else city end as city,
        case when postal_code = '' then null else postal_code end as postal_code,
        row_number() over (partition by address_id order by update_date desc) as row_num_latest_revision
    from
        addressfeed_v1
    )
where
    row_num_latest_revision = 1
    and (delete_date is null or delete_date = '')
),

-- Join accounts to addresses for company name resolution.
-- Apply 212-day backward extension for first revision to handle backdated BYOL agreements.
accounts_with_history_with_company_name as (
select
    awh.account_id,
    awh.aws_account_id,
    awh.encrypted_account_id,
    awh.mailing_address_id,
    awh.tax_address_id,
    coalesce(
        -- empty value in Athena shows as '', change all '' value to null
        case when address.company_name = '' then null else address.company_name end,
        awh.tax_legal_name) as mailing_company_name,
    awh.tax_legal_name,
    address.email_domain,
    awh.valid_from,
    -- For BYOL, the agreement might be accepted (using some external non-AWS system or manual process) days before
    -- that BYOL agreement is entered into AWS Marketplace by the buyer. Therefore, the buyer is permitted to manually
    -- enter a backdated acceptance date, which might predate the point in time when the account was created.
    -- To work around this, we need to adjust the valid_from of the account to be
    -- earlier than the earliest possible backdated BYOL agreement acceptance date.
    case
        when lag(awh.valid_from) over (partition by aws_account_id order by awh.valid_from asc) is null
        then date_add('Day', -212, awh.valid_from)
        -- 212 is the longest delay between acceptance_date of the agreement and the account start_date
        else awh.valid_from
    end as valid_from_adjusted,
    awh.valid_to,
    coalesce(least(awh.min_data_catalog, address.min_data_catalog), awh.min_data_catalog, address.min_data_catalog) as min_data_catalog,
    coalesce(greatest(awh.max_data_catalog, address.max_data_catalog), awh.max_data_catalog, address.max_data_catalog) as max_data_catalog
from accounts_with_history as awh
left join address_with_latest_revision as address on
    awh.mailing_address_id = address.address_id and awh.mailing_address_id is not null
),

-- Build validity time ranges for agreements.
-- The first revision is extended to 1970-01-01 to handle usage line items with dates
-- that cannot fall into the default valid time range.
-- A 60-minute adjustment is applied unconditionally for version=1 to handle the renewal edge case.
agreements_revisions_with_history as (
with agreements_with_window_functions as (
    select
        agreement_id,
        offer_id,
        offer_set_id,
        proposer_account_id,
        acceptor_account_id,
        start_date,
        end_date,
        acceptance_date,
        valid_from as agreement_valid_from,
        -- The start time of agreement valid_from is extended to '1970-01-01 00:00:00', because:
        -- ... in usage report transformations, some usage line items with usage_date cannot
        -- ... fall into the default valid time range of the associated agreement
        case
            when lag(valid_from) over (partition by agreement_id order by valid_from asc) is null
            then timestamp '1970-01-01 00:00:00 UTC'
            else valid_from
        end as valid_from,
        coalesce(
            lead(valid_from) over (partition by agreement_id order by valid_from asc),
            timestamp '2999-01-01 00:00:00 UTC'
        ) as valid_to,
        rank() over (partition by agreement_id order by valid_from asc) as version,
        preceding_agreement_id as origin_agreement_id,
        intent as origin_intent,
        estimated_charges_currency_code,
        estimated_charges_net_amount,
        status,
        status_reason_code,
        agreement_revision,
        catalog as min_data_catalog,
        catalog as max_data_catalog,
        catalog as data_catalog
    from
        agreements_with_uni_temporal_data
    )
select
    agreement_valid_from,
    agreement_id,
    offer_id,
    offer_set_id,
    proposer_account_id,
    acceptor_account_id,
    start_date,
    end_date,
    acceptance_date,
    valid_from,
    -- The 60-minute adjustment handles the special case where renewal happens for a contract.
    -- Applied unconditionally for version=1 (unlike the Anthropic report which has a date threshold).
    case
        when version = 1 then date_add('minute', -60, valid_from)
        else valid_from
    end as valid_from_adjusted,
    valid_to,
    origin_agreement_id,
    origin_intent,
    estimated_charges_currency_code,
    estimated_charges_net_amount,
    status,
    status_reason_code,
    agreement_revision,
    min_data_catalog,
    max_data_catalog,
    data_catalog
from
    agreements_with_window_functions
),

-- Aggregate term_type values into a comma-separated list per agreement and valid_from.
-- Used to enrich the final output with accepted term types.
agreement_with_all_accepted_term_types as (
select
    agreement_id,
    valid_from as revision_creation_time,
    listagg(terms.term_type, ',') within group (order by terms.term_type) as accepted_term_types,
    cast(null as varchar) as min_data_catalog,
    cast(null as varchar) as max_data_catalog
from
    agreement_terms_with_uni_temporal_data as terms
group by
    agreement_id,
    valid_from
),

-- Build validity time ranges for offers.
-- The first revision is extended backward to handle backdated BYOL agreements:
--   Before 2021-04-01: 3857 days
--   On or after 2021-04-01: 1700 days (per design spec)
-- CPPO detection: seller_account_id differs from the seller account.
offers_with_history as (
select
    offer_id,
    offer_revision,
    seller_account_id,
    recipient_account_id,

    (
        -- The seller_account_id column is never null except for some test data:
        seller_account_id = recipient_account_id
    ) as is_legal_seller_the_feed_recipient,

    case
      when NOT (seller_account_id = recipient_account_id) then 'Y'
      -- TODO: Replace the above one line with the below commented code block before adding support for distributors
      --when opportunity_id is not null then 'Y'
      -- After we add support for distributors, we can no longer assume that new offers are CPPO,
      -- just because the ISV does not match the legal seller of record because the distributor
      -- could sell directly to the end buyer:
      --[for historical CPPO data that is missing opportunity_id] when isv_aws_account_id (from product) != seller_account_id (from offer) and valid_from < 'YYYY-MM-DD 00:00:00'::timstamp then 'Y'
      else 'N'
    end as cppo_flag,

    name,
    opportunity_id,
    opportunity_name,
    opportunity_description,
    valid_from,
    case
        when lag(valid_from) over (partition by offer_id order by valid_from asc) is null and valid_from < cast('2021-04-01' as TIMESTAMP WITH TIME ZONE)
            then date_add('Day', -3857, valid_from)
        -- 3857 is the longest delay between acceptance_date of an agreement and the first revision of the offer
        when lag(valid_from) over (partition by offer_id order by valid_from asc) is null and valid_from >= cast('2021-04-01' as TIMESTAMP WITH TIME ZONE)
            then date_add('Day', -1700, valid_from)
        -- 1700 days for offers created on or after 2021-04-01 (per design spec)
        else valid_from
    end as valid_from_adjusted,
    coalesce(
        lead(valid_from) over (partition by offer_id order by valid_from asc),
        cast('2999-01-01 00:00:00' as TIMESTAMP WITH TIME ZONE)
    ) as valid_to,
    catalog as min_data_catalog,
    catalog as max_data_catalog
from offers_with_uni_temporal_data
),

-- Resolve offer target type from offertargetfeed_v1 with priority:
--   BuyerAccounts → Private
--   ParticipatingPrograms → Program:{value}
--   CountryCodes → GeoTargeted
--   else → Other Targeting
offer_target_type as (
select
    offer_id,
    offer_revision,
    substring(
        -- The first character indicates the priority (lower value means higher precedence):
        min(
            case
            when offer_target.target_type = 'BuyerAccounts' then '1Private'
            when offer_target.target_type = 'ParticipatingPrograms' then '2Program:' || cast(offer_target.value as varchar)
            when offer_target.target_type = 'CountryCodes' then '3GeoTargeted'
            -- well, there is no other case today, but rather be safe...
            else '4Other Targeting'
            end
        ),
        -- Remove the first character that was only used for the priority in the "min" aggregate function:
        2
    ) as offer_target,
    min(offer_target.catalog)  as min_data_catalog,
    max(offer_target.catalog)  as max_data_catalog
from
    offer_targets_with_uni_temporal_data as offer_target
group by
    offer_id,
    offer_revision
),

-- Join offers to targets, override CPPO offers to Private.
-- NOTE: offers_with_uni_temporal_data does not include offer_revision, so we join only on offer_id.
offers_with_history_with_target_type as (
select
    offer.offer_id,
    offer.offer_revision,

    bool_or(offer.is_legal_seller_the_feed_recipient) as is_legal_seller_the_feed_recipient,

    max(offer.cppo_flag) as cppo_flag,
    -- even though today it is not possible to combine several types of targeting in a single offer,
    -- let's ensure the query is still predictable if this gets possible in the future
    min(
        case
            when NOT offer.is_legal_seller_the_feed_recipient then 'Private'
            when off_tgt.offer_target is null then 'Public'
            else off_tgt.offer_target
        end
    ) as offer_target_with_private,
    min(offer.name) as name,
    min(offer.opportunity_name) as opportunity_name,
    min(offer.opportunity_description) as opportunity_description,
    offer.valid_from,
    offer.valid_from_adjusted,
    offer.valid_to,
    offer.opportunity_id,
    min(least(offer.min_data_catalog, off_tgt.min_data_catalog)) as min_data_catalog,
    max(greatest(offer.max_data_catalog, off_tgt.max_data_catalog)) as max_data_catalog
from offers_with_history as offer
left join offer_target_type as off_tgt on
    offer.offer_id = off_tgt.offer_id
    and offer.offer_revision = off_tgt.offer_revision
group by
    offer.offer_id,
    offer.offer_revision,
    offer.valid_from,
    offer.valid_from_adjusted,
    offer.valid_to,
    offer.opportunity_id
),

-- Build validity time ranges for products.
-- The first revision is extended backward to handle backdated BYOL agreements:
--   Before 2021-04-01: 3857 days
--   On or after 2021-04-01: 2190 days
products_with_history as (
select
    product_id,
    title,
    valid_from,
    case
        when lag(valid_from) over (partition by product_id order by valid_from asc) is null and valid_from < cast('2021-04-01' as TIMESTAMP WITH TIME ZONE)
            then date_add('Day', -3857, valid_from)
        -- 3857 is the longest delay between acceptance_date of an agreement and the product
        when lag(valid_from) over (partition by product_id order by valid_from asc) is null and valid_from >= cast('2021-04-01' as TIMESTAMP WITH TIME ZONE)
            then date_add('Day', -2190, valid_from)
        -- 2190 days for products created on or after 2021-04-01
        else valid_from
    end as valid_from_adjusted,
    coalesce(
        lead(valid_from) over (partition by product_id order by valid_from asc),
        cast('2999-01-01 00:00:00' as TIMESTAMP WITH TIME ZONE)
    ) as valid_to,
    product_code,
    manufacturer_account_id,
    catalog as min_data_catalog,
    catalog as max_data_catalog
from
    products_with_uni_temporal_data
),

-- ============================================================================
-- Layer 3: Usage report
-- Enriched with offer, product, account and address data
-- ============================================================================

usage_line_items as (
select
    usage_feed_id,
    recipient_account_id,
    valid_from,
    coalesce(
        lead(valid_from) over (partition by usage_feed_id order by valid_from asc),
        cast('2999-01-01 00:00:00' as timestamp))
    as valid_to,
    end_user_account_id,
    payer_account_id,
    usage_date,
    usage_reported_date,
    resource_type,
    usage_unit_types,
    currency,
    charge_item_description,
    usage_rate_per_unit,
    usage_units,
    estimated_revenue,
    region,
    agreement_id,
    product_id,
    offer_id as internal_offer_id,
    catalog as min_data_catalog,
    catalog as max_data_catalog,
    catalog as data_catalog
FROM
    usage_with_uni_temporal_data
),

-- Enrich with agreement data.
usage_line_items_with_agreement as (
SELECT
    usage_lines.usage_feed_id,
    usage_lines.recipient_account_id,
    agreement.agreement_valid_from,
    usage_lines.valid_from,
    usage_lines.end_user_account_id,
    usage_lines.payer_account_id,
    agreement.acceptor_account_id AS subscriber_account_id,
    usage_lines.usage_date,
    usage_lines.usage_reported_date,
    usage_lines.resource_type,
    usage_lines.usage_unit_types,
    usage_lines.currency,
    usage_lines.charge_item_description,
    usage_lines.usage_rate_per_unit,
    usage_lines.usage_units,
    usage_lines.estimated_revenue,
    usage_lines.region,
    usage_lines.agreement_id,
    agreement.agreement_revision,
    CASE WHEN agreement.offer_id LIKE 'aiqoffer-%' THEN NULL ELSE agreement.start_date END AS agreement_start_date,
    CASE WHEN agreement.offer_id LIKE 'aiqoffer-%' THEN NULL ELSE agreement.end_date END AS agreement_end_date,
    CASE WHEN agreement.offer_id LIKE 'aiqoffer-%' THEN NULL ELSE agreement.acceptance_date END AS agreement_acceptance_date,
    CASE WHEN agreement.offer_id like 'aiqoffer-%' THEN NULL ELSE agreement.valid_from END AS agreement_updated_date,
    agreement.status,
    agreement.estimated_charges_currency_code,
    agreement.estimated_charges_net_amount,
    agreement.proposer_account_id,
    -- Show offer via billing_product_code when agreement cannot be found
    COALESCE(agreement.offer_id, usage_lines.internal_offer_id) as offer_id,
    agreement.offer_set_id,
    usage_lines.product_id,
    least(usage_lines.min_data_catalog, agreement.min_data_catalog) as min_data_catalog,
    greatest(usage_lines.max_data_catalog, agreement.max_data_catalog) as max_data_catalog,
    usage_lines.data_catalog
from
    usage_line_items as usage_lines
    left join agreements_revisions_with_history as agreement on
        usage_lines.agreement_id = agreement.agreement_id and
        usage_lines.usage_date >= agreement.valid_from_adjusted and
        usage_lines.usage_date < agreement.valid_to
),

-- Enrich with offer data.
usage_line_items_with_agreement_offer as (
select
    usage_lines.usage_feed_id,
    usage_lines.recipient_account_id,
    usage_lines.agreement_valid_from,
    usage_lines.valid_from,
    usage_lines.end_user_account_id,
    usage_lines.payer_account_id,
    usage_lines.subscriber_account_id,
    usage_lines.usage_date,
    usage_lines.usage_reported_date,
    usage_lines.resource_type,
    usage_lines.usage_unit_types,
    usage_lines.currency,
    usage_lines.charge_item_description,
    usage_lines.usage_rate_per_unit,
    usage_lines.usage_units,
    usage_lines.estimated_revenue,
    usage_lines.region,
    usage_lines.agreement_id,
    usage_lines.agreement_revision,
    usage_lines.agreement_start_date,
    usage_lines.agreement_end_date,
    usage_lines.agreement_acceptance_date,
    usage_lines.agreement_updated_date,
    usage_lines.status,
    usage_lines.estimated_charges_currency_code,
    usage_lines.estimated_charges_net_amount,
    usage_lines.proposer_account_id,
    usage_lines.offer_id,
    usage_lines.offer_set_id,
    offer.offer_target_with_private as offer_visibility,
    offer.cppo_flag,
    offer.name as offer_name,
    offer.opportunity_id AS resale_authorization_id,
    offer.opportunity_name AS resale_authorization_name,
    offer.opportunity_description AS resale_authorization_description,
    usage_lines.product_id,
    least(usage_lines.min_data_catalog, offer.min_data_catalog) as min_data_catalog,
    greatest(usage_lines.max_data_catalog, offer.max_data_catalog) as max_data_catalog,
    usage_lines.data_catalog
from
    usage_line_items_with_agreement as usage_lines
    left join offers_with_history_with_target_type as offer on
        usage_lines.offer_id = offer.offer_id and
        usage_lines.usage_date >= offer.valid_from_adjusted and
        usage_lines.usage_date < offer.valid_to
),

-- Enrich wih product data.
usage_line_items_with_product as (
select
    usage_lines.usage_feed_id,
    usage_lines.recipient_account_id,
    usage_lines.agreement_valid_from,
    usage_lines.valid_from,
    usage_lines.end_user_account_id,
    usage_lines.payer_account_id,
    usage_lines.subscriber_account_id,
    usage_lines.usage_date,
    usage_lines.usage_reported_date,
    usage_lines.resource_type,
    usage_lines.usage_unit_types,
    usage_lines.currency,
    usage_lines.charge_item_description,
    usage_lines.usage_rate_per_unit,
    usage_lines.usage_units,
    usage_lines.estimated_revenue,
    usage_lines.region,
    usage_lines.agreement_id,
    usage_lines.agreement_revision,
    usage_lines.agreement_start_date,
    usage_lines.agreement_end_date,
    usage_lines.agreement_acceptance_date,
    usage_lines.agreement_updated_date,
    usage_lines.status,
    usage_lines.estimated_charges_currency_code,
    usage_lines.estimated_charges_net_amount,
    usage_lines.proposer_account_id,
    usage_lines.offer_id,
    usage_lines.offer_set_id,
    usage_lines.offer_visibility,
    usage_lines.cppo_flag,
    usage_lines.offer_name,
    usage_lines.resale_authorization_id,
    usage_lines.resale_authorization_name,
    usage_lines.resale_authorization_description,
    coalesce(legacy_products.legacy_id, usage_lines.product_id) as product_id,
    legacy_products.legacy_id AS legacy_product_id,
    products.title as product_title,
    products.product_code,
    products.manufacturer_account_id,
    acc_products.aws_account_id as manufacturer_aws_account_id,
    acc_products.encrypted_account_id as manufacturer_encrypted_account_id,
    least(usage_lines.min_data_catalog, products.min_data_catalog, legacy_products.min_data_catalog) as min_data_catalog,
    greatest(usage_lines.max_data_catalog, products.max_data_catalog, legacy_products.max_data_catalog) as max_data_catalog,
    usage_lines.data_catalog
from
    usage_line_items_with_agreement_offer as usage_lines
    left join products_with_history as products on
        usage_lines.product_id = products.product_id and
        usage_lines.usage_date >= products.valid_from_adjusted and
        usage_lines.usage_date < products.valid_to
    left join legacy_products as legacy_products on
        usage_lines.product_id = legacy_products.new_id
    left join accounts_with_history_with_company_name as acc_products on
        products.manufacturer_account_id = acc_products.account_id and 
        usage_lines.usage_date >= acc_products.valid_from_adjusted and 
        usage_lines.usage_date < acc_products.valid_to
),

-- Enrich with payer data.
usage_line_items_with_payer_account as (
select
    usage_lines.usage_feed_id,
    usage_lines.recipient_account_id,
    usage_lines.agreement_valid_from,
    usage_lines.valid_from,
    usage_lines.end_user_account_id,
    usage_lines.payer_account_id,
    payer_account.aws_account_id as payer_aws_account_id,
    payer_account.encrypted_account_id as payer_encrypted_account_id,
    payer_account.tax_legal_name as payer_tax_legal_name,
    payer_account.tax_address_id as payer_tax_address_id,
    payer_account.mailing_address_id as payer_mailing_address_id,
    coalesce(payer_account.tax_address_id, payer_account.mailing_address_id) as payer_address_id,
    usage_lines.subscriber_account_id,
    usage_lines.usage_date,
    usage_lines.usage_reported_date,
    usage_lines.resource_type,
    usage_lines.usage_unit_types,
    usage_lines.currency,
    usage_lines.charge_item_description,
    usage_lines.usage_rate_per_unit,
    usage_lines.usage_units,
    usage_lines.estimated_revenue,
    usage_lines.region,
    usage_lines.agreement_id,
    usage_lines.agreement_revision,
    usage_lines.agreement_start_date,
    usage_lines.agreement_end_date,
    usage_lines.agreement_acceptance_date,
    usage_lines.agreement_updated_date,
    usage_lines.status,
    usage_lines.estimated_charges_currency_code,
    usage_lines.estimated_charges_net_amount,
    usage_lines.proposer_account_id,
    usage_lines.offer_id,
    usage_lines.offer_set_id,
    usage_lines.offer_visibility,
    usage_lines.cppo_flag,
    usage_lines.offer_name,
    usage_lines.resale_authorization_id,
    usage_lines.resale_authorization_name,
    usage_lines.resale_authorization_description,
    usage_lines.product_id,
    usage_lines.legacy_product_id,
    usage_lines.product_title,
    usage_lines.product_code,
    usage_lines.manufacturer_account_id,
    usage_lines.manufacturer_aws_account_id,
    usage_lines.manufacturer_encrypted_account_id,
    least(usage_lines.min_data_catalog, payer_account.min_data_catalog) as min_data_catalog,
    greatest(usage_lines.max_data_catalog, payer_account.max_data_catalog) as max_data_catalog,
    usage_lines.data_catalog
from
    usage_line_items_with_product as usage_lines
    left join accounts_with_history_with_company_name as payer_account on
        usage_lines.payer_account_id = payer_account.account_id and
        usage_lines.usage_date >= payer_account.valid_from_adjusted and
        usage_lines.usage_date < payer_account.valid_to
),

-- Enrich with end user data.
usage_line_items_with_end_user_account as (
select
    usage_lines.usage_feed_id,
    usage_lines.recipient_account_id,
    usage_lines.agreement_valid_from,
    usage_lines.valid_from,
    usage_lines.end_user_account_id,
    end_user_account.aws_account_id as end_user_aws_account_id,
    end_user_account.encrypted_account_id as end_user_encrypted_account_id,
    end_user_account.tax_legal_name as end_user_tax_legal_name,
    end_user_account.tax_address_id as end_user_tax_address_id,
    end_user_account.mailing_address_id as end_user_mailing_address_id,
    coalesce(end_user_account.tax_address_id, end_user_account.mailing_address_id) as end_user_address_id,
    usage_lines.payer_account_id,
    usage_lines.payer_aws_account_id,
    usage_lines.payer_encrypted_account_id,
    usage_lines.payer_tax_legal_name,
    usage_lines.payer_tax_address_id,
    usage_lines.payer_mailing_address_id,
    usage_lines.payer_address_id,
    usage_lines.subscriber_account_id,
    usage_lines.usage_date,
    usage_lines.usage_reported_date,
    usage_lines.resource_type,
    usage_lines.usage_unit_types,
    usage_lines.currency,
    usage_lines.charge_item_description,
    usage_lines.usage_rate_per_unit,
    usage_lines.usage_units,
    usage_lines.estimated_revenue,
    usage_lines.region,
    usage_lines.agreement_id,
    usage_lines.agreement_revision,
    usage_lines.agreement_start_date,
    usage_lines.agreement_end_date,
    usage_lines.agreement_acceptance_date,
    usage_lines.agreement_updated_date,
    usage_lines.status,
    usage_lines.estimated_charges_currency_code,
    usage_lines.estimated_charges_net_amount,
    usage_lines.proposer_account_id,
    usage_lines.offer_id,
    usage_lines.offer_set_id,
    usage_lines.offer_visibility,
    usage_lines.cppo_flag,
    usage_lines.offer_name,
    usage_lines.resale_authorization_id,
    usage_lines.resale_authorization_name,
    usage_lines.resale_authorization_description,
    usage_lines.product_id,
    usage_lines.legacy_product_id,
    usage_lines.product_title,
    usage_lines.product_code,
    usage_lines.manufacturer_account_id,
    usage_lines.manufacturer_aws_account_id,
    usage_lines.manufacturer_encrypted_account_id,
    least(usage_lines.min_data_catalog, end_user_account.min_data_catalog) as min_data_catalog,
    greatest(usage_lines.max_data_catalog, end_user_account.max_data_catalog) as max_data_catalog,
    usage_lines.data_catalog
from
    usage_line_items_with_payer_account as usage_lines
    left join accounts_with_history_with_company_name as end_user_account on
        usage_lines.end_user_account_id = end_user_account.account_id and
        usage_lines.usage_date >= end_user_account.valid_from_adjusted and
        usage_lines.usage_date < end_user_account.valid_to
),

-- Enrich with subscriber data.
usage_line_items_with_subscriber_account as (
select
    usage_lines.usage_feed_id,
    usage_lines.recipient_account_id,
    usage_lines.agreement_valid_from,
    usage_lines.valid_from,
    usage_lines.end_user_account_id,
    usage_lines.end_user_aws_account_id,
    usage_lines.end_user_encrypted_account_id,
    usage_lines.end_user_tax_legal_name,
    usage_lines.end_user_tax_address_id,
    usage_lines.end_user_mailing_address_id,
    usage_lines.end_user_address_id,
    usage_lines.payer_account_id,
    usage_lines.payer_aws_account_id,
    usage_lines.payer_encrypted_account_id,
    usage_lines.payer_tax_legal_name,
    usage_lines.payer_tax_address_id,
    usage_lines.payer_mailing_address_id,
    usage_lines.payer_address_id,
    usage_lines.subscriber_account_id,
    subscriber_account.aws_account_id as subscriber_aws_account_id,
    subscriber_account.encrypted_account_id as subscriber_encrypted_account_id,
    subscriber_account.tax_legal_name as subscriber_tax_legal_name,
    subscriber_account.tax_address_id as subscriber_tax_address_id,
    subscriber_account.mailing_address_id as subscriber_mailing_address_id,
    coalesce(subscriber_account.tax_address_id, subscriber_account.mailing_address_id) as subscriber_address_id,
    usage_lines.usage_date,
    usage_lines.usage_reported_date,
    usage_lines.resource_type,
    usage_lines.usage_unit_types,
    usage_lines.currency,
    usage_lines.charge_item_description,
    usage_lines.usage_rate_per_unit,
    usage_lines.usage_units,
    usage_lines.estimated_revenue,
    usage_lines.region,
    usage_lines.agreement_id,
    usage_lines.agreement_revision,
    usage_lines.agreement_start_date,
    usage_lines.agreement_end_date,
    usage_lines.agreement_acceptance_date,
    usage_lines.agreement_updated_date,
    usage_lines.status,
    usage_lines.estimated_charges_currency_code,
    usage_lines.estimated_charges_net_amount,
    usage_lines.proposer_account_id,
    usage_lines.offer_id,
    usage_lines.offer_set_id,
    usage_lines.offer_visibility,
    usage_lines.cppo_flag,
    usage_lines.offer_name,
    usage_lines.resale_authorization_id,
    usage_lines.resale_authorization_name,
    usage_lines.resale_authorization_description,
    usage_lines.product_id,
    usage_lines.legacy_product_id,
    usage_lines.product_title,
    usage_lines.product_code,
    usage_lines.manufacturer_account_id,
    usage_lines.manufacturer_aws_account_id,
    usage_lines.manufacturer_encrypted_account_id,
    least(usage_lines.min_data_catalog, subscriber_account.min_data_catalog) as min_data_catalog,
    greatest(usage_lines.max_data_catalog, subscriber_account.max_data_catalog) as max_data_catalog,
    usage_lines.data_catalog
from
    usage_line_items_with_end_user_account as usage_lines
    left join accounts_with_history_with_company_name as subscriber_account on
        usage_lines.subscriber_account_id = subscriber_account.account_id and
        usage_lines.usage_date >= subscriber_account.valid_from_adjusted and
        usage_lines.usage_date < subscriber_account.valid_to
),

-- Enriched with address data.
usage_line_items_enriched_with_address as (
select
    usage_lines.usage_feed_id,
    usage_lines.recipient_account_id,
    usage_lines.agreement_valid_from,

    usage_lines.end_user_account_id,
    usage_lines.end_user_aws_account_id,
    usage_lines.end_user_encrypted_account_id,
    usage_lines.end_user_address_id,
    -- For company name, we take it from the tax/legal company name or the address record with a non-null value
    coalesce(
        usage_lines.end_user_tax_legal_name,
        end_user_tax_address.company_name,
        end_user_mailing_address.company_name
    ) as end_user_company_name,
    end_user_address.email_domain AS end_user_email_domain,
    end_user_address.city AS end_user_city,
    end_user_address.state_or_region AS end_user_state_or_region,
    end_user_address.country_code AS end_user_country,
    end_user_address.postal_code AS end_user_postal_code,

    usage_lines.payer_account_id,
    usage_lines.payer_aws_account_id,
    usage_lines.payer_encrypted_account_id,
    usage_lines.payer_address_id,
    -- For company name, we take it from the tax/legal company name or the address record with a non-null value
    coalesce(
        usage_lines.payer_tax_legal_name,
        payer_tax_address.company_name,
        payer_mailing_address.company_name
    ) as payer_company_name,
    payer_address.email_domain AS payer_email_domain,
    payer_address.city AS payer_city,
    payer_address.state_or_region AS payer_state_or_region,
    payer_address.country_code AS payer_country,
    payer_address.postal_code AS payer_postal_code,

    usage_lines.subscriber_account_id,
    usage_lines.subscriber_aws_account_id,
    usage_lines.subscriber_encrypted_account_id,
    usage_lines.subscriber_tax_legal_name,
    usage_lines.subscriber_tax_address_id,
    usage_lines.subscriber_mailing_address_id,
    usage_lines.subscriber_address_id,
    -- For company name, we take it from the tax/legal company name or the address record with a non-null value
    coalesce(
        usage_lines.subscriber_tax_legal_name,
        subscriber_tax_address.company_name,
        subscriber_mailing_address.company_name
    ) as subscriber_company_name,
    subscriber_address.email_domain AS subscriber_email_domain,
    subscriber_address.city AS subscriber_city,
    subscriber_address.state_or_region AS subscriber_state_or_region,
    subscriber_address.country_code AS subscriber_country,
    subscriber_address.postal_code AS subscriber_postal_code,

    usage_lines.usage_date,
    usage_lines.usage_reported_date,
    usage_lines.resource_type,
    usage_lines.usage_unit_types,
    usage_lines.currency,
    usage_lines.charge_item_description,
    usage_lines.usage_rate_per_unit,
    usage_lines.usage_units,
    usage_lines.estimated_revenue,
    usage_lines.region,
    usage_lines.product_id,
    usage_lines.legacy_product_id,
    usage_lines.product_title,
    usage_lines.agreement_id,
    usage_lines.agreement_revision,
    usage_lines.agreement_start_date,
    usage_lines.agreement_end_date,
    usage_lines.agreement_acceptance_date,
    usage_lines.agreement_updated_date,
    usage_lines.status,
    usage_lines.estimated_charges_currency_code,
    usage_lines.estimated_charges_net_amount,
    usage_lines.proposer_account_id,
    usage_lines.offer_id,
    usage_lines.offer_set_id,
    usage_lines.offer_visibility,
    usage_lines.cppo_flag,
    usage_lines.offer_name,
    usage_lines.resale_authorization_id,
    usage_lines.resale_authorization_name,
    usage_lines.resale_authorization_description,
    -- Reseller: when proposer is not the recipient of the data, the proposer is the reseller
    CASE
        WHEN usage_lines.proposer_account_id = usage_lines.recipient_account_id
            THEN NULL
        ELSE proposer_account.aws_account_id
    END AS reseller_aws_account_id,
    CASE
        WHEN usage_lines.proposer_account_id = usage_lines.recipient_account_id
            THEN NULL
        ELSE proposer_account.encrypted_account_id
    END AS reseller_encrypted_account_id,
    CASE
        WHEN usage_lines.proposer_account_id = usage_lines.recipient_account_id
            THEN NULL
        ELSE proposer_account.mailing_company_name
    END AS reseller_company_name,
    usage_lines.product_code,
    usage_lines.manufacturer_account_id,
    usage_lines.manufacturer_aws_account_id,
    usage_lines.manufacturer_encrypted_account_id,
    manufacturer_account.mailing_company_name AS manufacturer_company_name,
    least(usage_lines.min_data_catalog, proposer_account.min_data_catalog, manufacturer_account.min_data_catalog, payer_address.min_data_catalog, payer_tax_address.min_data_catalog, payer_mailing_address.min_data_catalog, end_user_address.min_data_catalog, subscriber_tax_address.min_data_catalog, subscriber_mailing_address.min_data_catalog, subscriber_address.min_data_catalog, end_user_tax_address.min_data_catalog, end_user_mailing_address.min_data_catalog) as min_data_catalog,
    greatest(usage_lines.max_data_catalog, proposer_account.max_data_catalog, manufacturer_account.max_data_catalog, payer_address.max_data_catalog, payer_tax_address.max_data_catalog, payer_mailing_address.max_data_catalog, end_user_address.max_data_catalog, subscriber_tax_address.max_data_catalog, subscriber_mailing_address.max_data_catalog, subscriber_address.max_data_catalog, end_user_tax_address.max_data_catalog, end_user_mailing_address.max_data_catalog) as max_data_catalog,
     usage_lines.data_catalog
from
    usage_line_items_with_subscriber_account as usage_lines
    left join accounts_with_history_with_company_name as proposer_account on
        usage_lines.proposer_account_id = proposer_account.account_id and
        usage_lines.usage_date >= proposer_account.valid_from_adjusted and
        usage_lines.usage_date <= proposer_account.valid_to
    left join accounts_with_history_with_company_name as manufacturer_account on
        usage_lines.manufacturer_account_id = manufacturer_account.account_id and
        usage_lines.usage_date >= manufacturer_account.valid_from_adjusted and
        usage_lines.usage_date <= manufacturer_account.valid_to
    left join address_with_latest_revision as payer_address on
        usage_lines.payer_address_id = payer_address.address_id
    left join address_with_latest_revision as payer_tax_address on
        usage_lines.payer_tax_address_id = payer_tax_address.address_id
    left join address_with_latest_revision as payer_mailing_address on
        usage_lines.payer_mailing_address_id = payer_mailing_address.address_id
    left join address_with_latest_revision as end_user_address on
        usage_lines.end_user_address_id = end_user_address.address_id
    left join address_with_latest_revision as end_user_tax_address on
        usage_lines.end_user_tax_address_id = end_user_tax_address.address_id
    left join address_with_latest_revision as end_user_mailing_address on
        usage_lines.end_user_mailing_address_id = end_user_mailing_address.address_id
    left join address_with_latest_revision as subscriber_address on
        usage_lines.subscriber_address_id = subscriber_address.address_id
    left join address_with_latest_revision as subscriber_tax_address on
        usage_lines.subscriber_tax_address_id = subscriber_tax_address.address_id
    left join address_with_latest_revision as subscriber_mailing_address on
        usage_lines.subscriber_mailing_address_id = subscriber_mailing_address.address_id
),

-- Final report.
usage_report as (
SELECT
    usage_lines.usage_feed_id AS internal_usage_line_item_id,
    usage_lines.usage_date,
    usage_lines.usage_reported_date,

    ---------------------
    -- End User Info --
    ---------------------
    coalesce(usage_lines.end_user_company_name, 'Not available') AS end_user_company_name,
    usage_lines.end_user_aws_account_id,
    usage_lines.end_user_encrypted_account_id,
    usage_lines.end_user_email_domain,
    usage_lines.end_user_city,
    usage_lines.end_user_state_or_region,
    usage_lines.end_user_country,
    usage_lines.end_user_postal_code,
    
    ------------------
    -- Product Info --
    ------------------
    coalesce(usage_lines.product_title, 'Not provided') AS product_title,
    coalesce(usage_lines.product_id, 'Not provided') AS product_id,
    coalesce(usage_lines.product_code, 'Not provided') AS product_code,
    
    --------------------
    -- Offer Info --
    --------------------
    CASE
        WHEN usage_lines.offer_id IS NULL THEN 'Not available'
        ELSE usage_lines.offer_id
    END AS offer_id,
    case
        WHEN usage_lines.agreement_id is null then 'Not available'
        WHEN usage_lines.offer_set_id is null then 'Not applicable'
        ELSE usage_lines.offer_set_id
    END AS offer_set_id,
    CASE
        WHEN usage_lines.offer_id IS NULL THEN 'Not available'
        WHEN usage_lines.offer_name IS NULL AND usage_lines.offer_visibility = 'Public' THEN 'Not applicable'
        ELSE usage_lines.offer_name
    END AS offer_name,
        CASE
        WHEN usage_lines.offer_id IS NULL THEN 'Not available'
        ELSE usage_lines.offer_visibility
    END AS offer_visibility,
    
    --------------------
    -- Agreement Info --
    --------------------
    coalesce(usage_lines.agreement_id, 'Not available') AS agreement_id,
    CASE
        WHEN usage_lines.agreement_id IS NULL THEN cast(null as timestamp)
        ELSE usage_lines.agreement_acceptance_date
    END AS agreement_acceptance_date,
    CASE
        WHEN usage_lines.agreement_id IS NULL THEN cast(null as timestamp)
        ELSE usage_lines.agreement_start_date
    END AS agreement_start_date,
    CASE
        WHEN usage_lines.agreement_id IS NULL THEN cast(null as timestamp)
        ELSE usage_lines.agreement_end_date
    END AS agreement_end_date,
    terms.accepted_term_types,
    usage_lines.data_catalog as catalog,

    -----------
    -- Usage --
    -----------
    usage_lines.resource_type as dimension_key,
    usage_lines.region,
    usage_lines.usage_units as estimated_usage_units,
    usage_lines.usage_unit_types,
    usage_lines.charge_item_description,
    usage_lines.usage_rate_per_unit,
    usage_lines.estimated_revenue,
    usage_lines.currency,

    ---------------------
    -- Subscriber Info --
    ---------------------
    CASE
        WHEN usage_lines.agreement_id IS NULL THEN 'Not available'
        WHEN usage_lines.subscriber_company_name IS NULL THEN 'Not provided'
        ELSE usage_lines.subscriber_company_name
    END AS subscriber_company_name,
    CASE
        WHEN usage_lines.agreement_id IS NULL THEN 'Not available'
        ELSE usage_lines.subscriber_aws_account_id
    END AS subscriber_aws_account_id,
    CASE
        WHEN usage_lines.agreement_id IS NULL THEN 'Not available'
        ELSE usage_lines.subscriber_encrypted_account_id
    END AS subscriber_encrypted_account_id,
    CASE
        WHEN usage_lines.agreement_id IS NULL THEN 'Not available'
        WHEN usage_lines.subscriber_email_domain IS NULL THEN 'Not provided'
        ELSE usage_lines.subscriber_email_domain
    END AS subscriber_email_domain,
    CASE
        WHEN usage_lines.agreement_id IS NULL THEN 'Not available'
        WHEN usage_lines.subscriber_city IS NULL THEN 'Not provided'
        ELSE usage_lines.subscriber_city
    END AS subscriber_city,
    CASE
        WHEN usage_lines.agreement_id IS NULL THEN 'Not available'
        WHEN usage_lines.subscriber_state_or_region IS NULL THEN 'Not provided'
        ELSE usage_lines.subscriber_state_or_region
    END AS subscriber_state_or_region,
    CASE
        WHEN usage_lines.agreement_id IS NULL THEN 'Not available'
        WHEN usage_lines.subscriber_country IS NULL THEN 'Not provided'
        ELSE usage_lines.subscriber_country
    END AS subscriber_country,
    CASE
        WHEN usage_lines.agreement_id IS NULL THEN 'Not available'
        WHEN usage_lines.subscriber_postal_code IS NULL THEN 'Not provided'
        ELSE usage_lines.subscriber_postal_code
    END AS subscriber_postal_code,

    ---------------------
    -- Payer Info --
    ---------------------
    coalesce(usage_lines.payer_company_name, 'Not available') AS payer_company_name,
    usage_lines.payer_aws_account_id,
    usage_lines.payer_encrypted_account_id,
    usage_lines.payer_email_domain,
    usage_lines.payer_city,
    usage_lines.payer_state_or_region,
    usage_lines.payer_country,
    usage_lines.payer_postal_code,

    ---------------------
    -- Reseller Info --
    ---------------------
    CASE
        WHEN usage_lines.reseller_aws_account_id IS NOT NULL AND usage_lines.reseller_company_name IS NULL THEN 'Not available'
        WHEN usage_lines.reseller_aws_account_id IS NULL AND usage_lines.resale_authorization_id IS NULL THEN 'Not applicable'
        WHEN usage_lines.recipient_account_id <> usage_lines.manufacturer_account_id AND usage_lines.reseller_aws_account_id IS NULL THEN 'Not applicable'
        ELSE usage_lines.reseller_company_name
    END AS reseller_company_name,
    CASE
        WHEN usage_lines.reseller_aws_account_id IS NULL AND usage_lines.resale_authorization_id IS NULL THEN 'Not applicable'
        WHEN usage_lines.recipient_account_id <> usage_lines.manufacturer_account_id AND usage_lines.reseller_aws_account_id IS NULL THEN 'Not applicable'
        ELSE usage_lines.reseller_aws_account_id
    END AS reseller_aws_account_id,
    CASE
        WHEN usage_lines.resale_authorization_id IS NULL THEN
            CASE
                WHEN usage_lines.offer_visibility = 'Public' THEN 'Not applicable'
                WHEN usage_lines.offer_visibility IS NULL AND usage_lines.agreement_id IS NOT NULL THEN 'Not applicable'
                ELSE NULL
            END
        ELSE usage_lines.resale_authorization_id
    END AS resale_authorization_id,
    CASE
        WHEN usage_lines.resale_authorization_name IS NULL THEN
            CASE
                WHEN usage_lines.offer_visibility = 'Public' THEN 'Not applicable'
                WHEN usage_lines.offer_visibility IS NULL AND usage_lines.agreement_id IS NOT NULL THEN 'Not applicable'
                ELSE NULL
            END
        ELSE usage_lines.resale_authorization_name
    END AS resale_authorization_name,
    usage_lines.resale_authorization_description,
    usage_lines.cppo_flag,

    ---------------------
    -- ISV Info --
    ---------------------
    usage_lines.manufacturer_aws_account_id as isv_aws_account_id,
    coalesce(usage_lines.manufacturer_company_name, 'Not available') AS isv_company_name
FROM
    usage_line_items_enriched_with_address as usage_lines
    left join agreement_with_all_accepted_term_types as terms on
        usage_lines.agreement_id = terms.agreement_id and
        usage_lines.agreement_valid_from = terms.revision_creation_time
)

select *
from usage_report
where usage_date >= date_add('DAY', -90, current_date)
