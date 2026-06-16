-- Billed revenue report

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

-- An address_id has several valid_from dates (each representing a separate revision of the data)
-- but because of bi-temporality, an address_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
address_with_uni_temporal_data as (
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
        case when email_domain = '' then null else email_domain end as email_domain,
        case when country_code = '' then null else country_code end as country_code,
        case when state_or_region = '' then null else state_or_region end as state_or_region,
        case when city = '' then null else city end as city,
        case when postal_code = '' then null else postal_code end as postal_code,
        row_number() over (partition by address_id, valid_from order by from_iso8601_timestamp(update_date) desc) as row_num
    from
        addressfeed_v1
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

-- A given billing_event_id represents an accounting event and thus has only one valid_from date,
-- but because of bi-temporality, a billing_event_id (+ its valid_from) can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
billing_events_with_uni_temporal_data as (
select
    billing_event_id,
    parent_billing_event_id,
    valid_from,
    update_date,
    delete_date,
    invoice_date,
    transaction_type,
    transaction_reference_id,
    bank_trace_id,
    broker_id,
    product_id,
    disbursement_billing_event_id,
    action,
    from_account_id,
    to_account_id,
    end_user_account_id,
    billing_address_id,
    amount,
    currency,
    balance_impacting,
    agreement_id,
    invoice_id,
    payment_due_date,
    usage_period_start_date,
    usage_period_end_date,
    buyer_transaction_reference_id,
    disbursement_amount,
    disbursement_currency,
    disbursement_reference_number,
    catalog,
    buyer_invoice_id,
    invoice_variant,
    action_date,
    offer_id,
    buyer_invoice_date,
    line_item_id,
    buyer_line_item_id,
    charge_variant,
    charge_side,
    recipient_account_id
from
    (
    select
        billing_event_id,
        parent_billing_event_id,
        from_iso8601_timestamp(valid_from) as valid_from,
        from_iso8601_timestamp(update_date) as update_date,
        delete_date,
        case when invoice_date is null or invoice_date = '' then null else from_iso8601_timestamp(invoice_date) end as invoice_date,
        transaction_type,
        case when transaction_reference_id = '' then null else transaction_reference_id end as transaction_reference_id,
        -- casting in case data was imported as number
        cast(bank_trace_id as varchar) as bank_trace_id,
        case when broker_id = '' then null else broker_id end as broker_id,
        case when product_id = '' then null else product_id end as product_id,
        case when disbursement_billing_event_id = '' then null else disbursement_billing_event_id end as disbursement_billing_event_id,
        action,
        case when from_account_id = '' then null else from_account_id end as from_account_id,
        case when to_account_id = '' then null else to_account_id end as to_account_id,
        case when end_user_account_id = '' then null else end_user_account_id end as end_user_account_id,
        case when billing_address_id = '' then null else billing_address_id end as billing_address_id,
        -- casting in case data was imported as varchar
        cast(amount as decimal(38,6)) as amount,
        currency,
        case when cast(balance_impacting as int) = 1 then true else false end as balance_impacting,
        -- empty value in Athena shows as '', change all '' value to null
        case when agreement_id = '' then null else agreement_id end as agreement_id,
        cast(invoice_id as varchar) as invoice_id,
        case when payment_due_date is null or payment_due_date = '' then null else from_iso8601_timestamp(payment_due_date) end as payment_due_date,
        case when usage_period_start_date is null or usage_period_start_date = '' then null else from_iso8601_timestamp(usage_period_start_date) end as usage_period_start_date,
        case when usage_period_end_date is null or usage_period_end_date = '' then null else from_iso8601_timestamp(usage_period_end_date) end as usage_period_end_date,
        case when buyer_transaction_reference_id = '' then null else buyer_transaction_reference_id end as buyer_transaction_reference_id,
        cast(disbursement_amount as decimal(38,6)) as disbursement_amount,
        case when disbursement_currency = '' then null else disbursement_currency end as disbursement_currency,
        case when disbursement_reference_number = '' then cast(null as varchar) else disbursement_reference_number end as disbursement_reference_number,
        catalog,
        case when buyer_invoice_id = '' then null else cast(buyer_invoice_id as varchar) end as buyer_invoice_id,
        invoice_variant,
        case when action_date = '' then cast(null as TIMESTAMP WITH TIME ZONE) else cast(action_date as TIMESTAMP WITH TIME ZONE) end as action_date,
        offer_id,
        case when buyer_invoice_date = '' then null else cast(buyer_invoice_date as TIMESTAMP WITH TIME ZONE) end as buyer_invoice_date,
        line_item_id,
        buyer_line_item_id,
        charge_variant,
        charge_side,
        recipient_account_id,
        row_number() over (partition by billing_event_id order by from_iso8601_timestamp(update_date) desc) as row_num
    from
        billingeventfeed_v1
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
    min(catalog) as max_data_catalog
from
    legacyidmappingfeed_v1
where
    mapping_type = 'PRODUCT'
group by
    legacy_id,
    new_id
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
    least(awh.min_data_catalog, address.min_data_catalog) as min_data_catalog,
    greatest(awh.max_data_catalog, address.max_data_catalog) as max_data_catalog
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
            then timestamp '1970-01-01 00:00:00'
            else valid_from
        end as valid_from,
        coalesce(
            lead(valid_from) over (partition by agreement_id order by valid_from asc),
            timestamp '2999-01-01 00:00:00'
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

-- Identify the seller account from billing events.
-- WARNING: If multiple distinct recipient_account_id values exist, the query behavior is undefined.
-- This CTE is expected to return exactly one row.
seller_account as (
select distinct
    recipient_account_id
from
    billing_events_with_uni_temporal_data
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
    min(offer_target.catalog)  as max_data_catalog
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
-- Layer 3: Billing Event Classification & Aggregation
-- Construct surrogate IDs, classify billing events by invoice variant and
-- transaction type, categorize amounts into financial buckets, aggregate by
-- line item, listagg seller-issued invoices
-- ============================================================================

-- Construct surrogate IDs for grouping billing events into line items.
billing_event_feed_with_fks as (
select
    feed.recipient_account_id,
    feed.billing_event_id,
    feed.valid_from,
    feed.end_user_account_id,
    feed.agreement_id,

    feed.offer_id,
    
    feed.from_account_id,
    feed.to_account_id,
    feed.product_id,
    feed.action,
    feed.transaction_type,
    feed.parent_billing_event_id,
    feed.disbursement_billing_event_id,
    feed.amount,
    feed.currency,
    feed.disbursement_amount,
    feed.disbursement_currency,
    feed.balance_impacting,
    feed.invoice_date,
    feed.payment_due_date,
    feed.usage_period_start_date,
    feed.usage_period_end_date,
    feed.invoice_id,

    feed.line_item_id,

    feed.buyer_invoice_id as purchase_invoice_id,
    feed.buyer_line_item_id as buyer_line_item_id,
    feed.buyer_invoice_date as purchase_invoice_date,

    feed.invoice_variant,
    feed.charge_variant,
    feed.charge_side,

    feed.billing_address_id,
    feed.transaction_reference_id,
    feed.buyer_transaction_reference_id,
    feed.broker_id,

    case
        -- [CASE 1]:
        -- BALANCE_ADJUSTMENT uses a simplified key
        when transaction_type = 'BALANCE_ADJUSTMENT' then 'BALANCE_ADJUSTMENT:' || recipient_account_id || ':' || cast(feed.invoice_date as varchar)

        -- [CASE 2]:
        -- Anthropic SELLER invoices do not have an associated BUYER invoice, so we need to
        -- fall back on the SELLER invoice itself:
        when buyer_invoice_id is null then 'NO_BUYER_INVOICE_FOR:' || invoice_id ||':'|| line_item_id ||':'|| recipient_account_id

        -- [CASE 3]:
        -- This is the normal case
        else
            buyer_invoice_id
            ||':'|| buyer_line_item_id  -- We switch to the invoice line item ID (currently FDR-generated GUID), so that things will break when FDR/Basilisk/GFS changes their invoice line item aggregation
            ||':'|| recipient_account_id                      
    end as internal_buyer_invoice_line_item_surrogate_id,

    feed.bank_trace_id,
    feed.disbursement_reference_number as amazon_reference_id,
    feed.action_date,
    feed.catalog as min_data_catalog,
    feed.catalog as max_data_catalog,
    feed.catalog as data_catalog
from
    billing_events_with_uni_temporal_data as feed
),

-- Self-join to disbursement records to resolve disbursement date, bank_trace_id, and amazon_reference_id.
-- Resolve NULL disbursement_currency for non-balance-impacting records using MAX() OVER window function.
-- Join billing events to agreements and compute invoice classification flags and COGS detection flags.
-- Uses the Dashboard_Query model for invoice classification (based on invoice_variant).
-- COGS detection uses charge_variant and charge_side (NOT to_account_id).
billing_event_with_business_flags as (
select
    bl.recipient_account_id,
    bl.billing_event_id,
    bl.end_user_account_id,
    bl.agreement_id,
     agreement.proposer_account_id,
    -- Use agreement's offer_id with fallback to billing event's offer_id
    coalesce(agreement.offer_id, bl.offer_id) as offer_id,
    agreement.acceptor_account_id,
    case
        -- For AWS and BALANCE_ADJUSTMENT, the billing event feed will show the "AWS Marketplace" account as the
        -- receiver of the funds and the seller as the payer. We are not interested in this information here.
        -- Null values will be ignored by the `max` aggregation function.
        when bl.transaction_type like 'AWS%' then null
        -- For BALANCE_ADJUSTMENT, payer is seller themselves
        when bl.invoice_id is null then bl.to_account_id /* transaction_type = 'BALANCE_ADJUSTMENT' */
        -- We get the payer of the invoice from *any* transaction type that is not AWS and not BALANCE_ADJUSTMENT (because they are the same for a given end user + agreement + product).
        else bl.from_account_id end as payer_account_id,
    bl.product_id,
    bl.action,
    bl.transaction_type,
    bl.parent_billing_event_id,
    bl.disbursement_billing_event_id,
    -- The sign on the amount column in the billing event feed indicates the balance due to the seller.
    --
    -- In the simplest hypothetical case of a direct sale from the ISV to the buyer with no taxes and no listing fees,
    -- the amount on the buyer invoice will be positive (indicating that it is owned to the seller) and
    -- the amount on the disbursement will be negative (indicate that it is no longer owned to the seller).
    -- If the disbursement fails (or if those amounts are later refunded to the buyer), then the new amounts
    -- will have opposite signs.
    --
    -- Extending the previous example, listing fees and taxes listing feeds that are paid by the seller are represented
    -- as negative amounts because they reduce amount that is owned to the seller. Disbursements of these amounts are
    -- positive because resolve that pending reduction.
    --
    -- However, seller-remitted taxes are a little different. If tax law requites AWS to collect taxes from the
    -- buyer and disburse to the seller, then the invoiced amounts for those taxes are represented as positive because
    -- they increase the amount that is owed to the seller.
    --
    -- POSITIVE amounts increase the money is owned TO the seller
    -- 1. (a) Used for invoiced amounts from the buyer that are due to the seller
    --    (b) Used for failures (reversals) of disbursement of invoiced amounts from the buyer that were previously due to the seller
    -- 2. (a) Used for refunds on listing fees and taxes that are collected from the seller
    --    (b) Used when listing fees are "disbursed", which really means a reduction (clawback) of a disbursement
    --
    -- NEGATIVE amounts decreased the money is owned BY the seller
    -- 1. (a) Used for refunds on invoiced amounts from the buyer that were previously due to the seller
    --    (b) Used for disbursements of invoiced amounts from the buyer that were previously due to the seller
    -- 2. (a) Used for listing fees and taxes that are collected from the seller
    --    (b) Used to failures (reversals) for clawbacks of listing fees from disbursements
    --
    bl.amount,
    --bl.legal_amount,
    bl.currency,
    --bl.legal_currency,
    -- Notes on the balance_impacting column:
    -- * For action=INVOICED,  balance_impacting is 1 if it affects the amount due to the seller
    -- * For action=INVOICED,  balance_impacting is 0 if it does _not_ affect the amount due to the seller,
    --   such as taxes collected from the buyer and remitted by AWS
    -- * For action=DISBURSED, balance_impacting is always 0, even though all action=DISBURSED records
    --   actually _do_ impact the balance.
    bl.balance_impacting,
    bl.invoice_date,
    bl.payment_due_date,
    bl.usage_period_start_date,
    bl.usage_period_end_date,
    bl.invoice_id,
    bl.line_item_id,

    bl.purchase_invoice_id as purchase_invoice_id,

    bl.buyer_line_item_id,
    bl.purchase_invoice_date as purchase_invoice_date,
    bl.invoice_variant,
    bl.charge_variant,
    bl.charge_side,
    bl.billing_address_id,
    bl.transaction_reference_id,
    bl.buyer_transaction_reference_id,
    case
        -- The report dashboards replace placeholder bank trace ID values with null for all historical data.
        -- NOTE: For new disbursements, this will be done in o_awsmp_disbursement_events__sanitized-transformation.sql
        when disbursement.bank_trace_id in ('EMEA_MP_TEST_TRACE_ID', 'testTraceId') then null
        else disbursement.bank_trace_id
    end as bank_trace_id,
    disbursement.amazon_reference_id,
    case when bl.action = 'COLLECTED' then bl.action_date else cast(null as TIMESTAMP WITH TIME ZONE) end as collection_date,
    disbursement.invoice_date as disbursement_date,
    bl.disbursement_billing_event_id as disbursement_id,
    disbursement.transaction_type as disbursement_status,
    -- We will use disbursement_id_or_invoiced_or_collected as part of the PK, so it cannot be null:
    case
        when disbursement.billing_event_id is not null then disbursement.billing_event_id
        when bl.action = 'COLLECTED' then '<collected>'
        else '<invoiced>'
    end as disbursement_id_or_invoiced_or_collected,
    bl.broker_id,

    bl.internal_buyer_invoice_line_item_surrogate_id,

    -- When disbursement currency is null it means the invoiced amount is not balance impacting and will not be disbursed.
    -- This will never appear in the collections and disbursement dashboard so the disbursement currency doesn't matter.
    -- In this case, we will use the disbursement currency of other records to group the amount with similar amounts
    -- in the billed revenue dashboard where disbursement currency is not surfaced.
    coalesce(bl.disbursement_currency, max(bl.disbursement_currency) over (partition by bl.internal_buyer_invoice_line_item_surrogate_id, bl.currency)) as disbursement_currency_non_null,


    -- Surprise! In some cases, the MP subledger system records accounting entries for the same buyer
    -- invoice line item in multiple (inconsistent) pricing currencies. In this case, we need to report
    -- them in different records in the aggregated records in the report table because each
    -- aggregated report record displays all amounts (in separate columns) in the same pricing currency:
    bl.internal_buyer_invoice_line_item_surrogate_id || 
        '_PricingCurrency:' || 
        coalesce(bl.currency, '') ||  
        '_DisbursementCurrency:' || 
        coalesce(coalesce(bl.disbursement_currency, max(bl.disbursement_currency) over (partition by bl.internal_buyer_invoice_line_item_surrogate_id, bl.currency)), '') as internal_buyer_invoice_line_item_surrogate_id_with_currency,

    -- NOTE: We should eventually rename invoice_variant to invoice_variant in the reports domain:
    coalesce(bl.invoice_variant, '<null>') IN ('RESALE')  AS is_resale_invoice,
    coalesce(bl.invoice_variant, '<null>') IN ('TAX_VAT') AS is_seller_issued_invoice,

    coalesce(bl.invoice_variant, '<null>') IN ('LISTING_FEE') AS is_listing_fee_invoice, --> Indicates that invoice_id is literally a seller listing fee invoice

    coalesce(bl.invoice_variant, '<null>') IN ('PURCHASE') AS is_purchase_invoice,

    (
        coalesce(bl.invoice_variant, '<null>') IN ('LISTING_FEE')
        or
        (
            -- The following handles cases where AWS_INC has SELLER invoices (which were previously not surfaced in the invoice_id column in the
            -- billing event feed). In other words, the invoice_id is presented as a BUYER invoice (for backwards compatibility),
            -- but the data is actually all linked to the SELLER invoice in the immutable subledger system:
            coalesce(bl.invoice_variant, '<null>') IN ('PURCHASE')
            and coalesce(bl.charge_variant, '<null>') = 'LISTING_FEE'
        )
    ) AS is_listing_fee_charge, --> Indicates that there is a "secret" seller listing fee invoice that is not surfaced in the invoice_id

    --case
    --    when bl.transaction_type like '%TAX%' and is_resale_invoice then true
    --    else false
    --end as is_resale_tax_or_refund,

    case when bl.transaction_type = 'SELLER_REV_SHARE' and bl.charge_variant = 'RESALE' and bl.charge_side = 'BUYING' then true else false end as is_cog,

    case when bl.transaction_type in('SELLER_REV_SHARE_CREDIT', 'SELLER_REV_SHARE_REFUND') and bl.charge_variant = 'RESALE' and bl.charge_side = 'BUYING' then true else false end as is_cog_refund,

    case when bl.transaction_type = 'SELLER_REV_SHARE' and bl.charge_variant = 'RESALE' and bl.charge_side = 'SELLING' then true else false end as is_channel_sale,

    case when bl.transaction_type in('SELLER_REV_SHARE_CREDIT', 'SELLER_REV_SHARE_REFUND') and bl.charge_variant = 'RESALE' and bl.charge_side = 'SELLING' then true else false end as is_channel_sale_refund,

    (
        case when bl.transaction_type = 'SELLER_REV_SHARE' and bl.charge_variant = 'RESALE' and bl.charge_side = 'SELLING' then true else false end or 
        case when bl.transaction_type in('SELLER_REV_SHARE_CREDIT', 'SELLER_REV_SHARE_REFUND') and bl.charge_variant = 'RESALE' and bl.charge_side = 'SELLING' then true else false end
        ) 
        and coalesce(bl.invoice_variant, '<null>') IN ('RESALE') as is_isv_wholesale_invoice,

    case when bl.charge_variant = 'RESALE' and bl.charge_side = 'SELLING' then true else false end as is_manufacturer_view_of_reseller,

    bl.disbursement_amount,
    coalesce(bl.disbursement_currency, max(bl.disbursement_currency) over (partition by bl.internal_buyer_invoice_line_item_surrogate_id, bl.currency)) as disbursement_currency,
    bl.data_catalog as min_data_catalog,
    bl.data_catalog as max_data_catalog,
    bl.data_catalog

from
    billing_event_feed_with_fks as bl
    left join billing_event_feed_with_fks as disbursement on
        disbursement.transaction_type like 'DISBURSEMENT%' and 
        (
            disbursement.action = 'DISBURSED' and
            disbursement.transaction_type IN ('DISBURSEMENT', 'DISBURSEMENT_FAILURE') and
            bl.disbursement_billing_event_id = disbursement.billing_event_id
        -- and bl.action = 'DISBURSED'
        -- and bl.transaction_type NOT IN ('DISBURSEMENT', 'DISBURSEMENT_FAILURE')
        )    
    left join agreements_revisions_with_history as agreement on
        bl.agreement_id = agreement.agreement_id and
        bl.invoice_date >= agreement.valid_from_adjusted and
        bl.invoice_date < agreement.valid_to
where
    coalesce(bl.transaction_type, '<null>') not like 'DISBURSEMENT%'
    and (bl.agreement_id is null or agreement.agreement_id is not null)
),


-- Use the flags from billing_event_with_business_flags to categorize amounts into financial buckets.
-- Computes ~24 financial buckets in both pricing currency (amount) and disbursement currency (disbursement_amount).
-- Also computes invoice ID columns for listagg with dense_rank guards.
-- NOTE: This CTE has no joins and no window functions except the dense_rank ones for invoice IDs.
billing_event_with_categorized_transaction as (
select
    is_resale_invoice,
    case
        when transaction_type like '%TAX%' and is_resale_invoice then true
        else false
    end as is_resale_tax_or_refund_helper,

    recipient_account_id,
    billing_event_id,
    end_user_account_id,
    agreement_id,

    case when is_resale_invoice then agreement_id else cast(null as varchar) end as wholesale_agreement_id,

    proposer_account_id,
    offer_id,
    acceptor_account_id,
    case 
        when is_cog or 
            is_cog_refund or 
            case
                when transaction_type like '%TAX%' and is_resale_invoice then true
                else false
                end 
            then null 
        else payer_account_id end as payer_account_id,
    product_id,
    action,
    transaction_type,
    parent_billing_event_id,
    disbursement_billing_event_id,
    amount,
    currency,
    disbursement_amount,
    disbursement_currency,
    --legal_amount,
    --legal_currency,
    balance_impacting,
    invoice_date,
    payment_due_date,
    usage_period_start_date,
    usage_period_end_date,
    invoice_id,
    line_item_id,
    purchase_invoice_id,
    buyer_line_item_id,
    invoice_variant,
    charge_variant,
    charge_side,
    billing_address_id,
    transaction_reference_id,
    buyer_transaction_reference_id,
    bank_trace_id,
    amazon_reference_id,
    collection_date,
    disbursement_date,
    disbursement_id,
    disbursement_status,
    disbursement_id_or_invoiced_or_collected,
    broker_id,
    internal_buyer_invoice_line_item_surrogate_id,
    internal_buyer_invoice_line_item_surrogate_id_with_currency,
    is_listing_fee_invoice,
    is_listing_fee_charge,
    is_cog,
    is_cog_refund,
    is_manufacturer_view_of_reseller,
    is_isv_wholesale_invoice,

    -- BUYER invoice columns:
    case when is_purchase_invoice then invoice_id when is_resale_invoice then purchase_invoice_id else cast(null as varchar) end as purchase_invoice_id_or_null,
    case when is_purchase_invoice or transaction_type = 'BALANCE_ADJUSTMENT' then invoice_date when is_resale_invoice then purchase_invoice_date else cast(null as TIMESTAMP WITH TIME ZONE)end as purchase_invoice_date_or_null,

    -- SELLER invoice columns:
    -- We use single_listing_fee_invoice_id_or_null_dense_rank as a guard on exceeding the listagg 65535 character limit in the next transformation.
    -- NOTE: the reason why one buyer invoice line item can be link to multiple seller invoice, is because seller invoice can be refunded and reissued linking to the same buyer invoice:
    case when is_listing_fee_invoice then invoice_date else cast(null as TIMESTAMP WITH TIME ZONE) end as single_listing_fee_invoice_date_or_null,
    case when is_listing_fee_invoice then invoice_id   else cast(null as varchar)   end as single_listing_fee_invoice_id_or_null,
    dense_rank() over (
        partition by internal_buyer_invoice_line_item_surrogate_id_with_currency 
        order by 
            case when is_listing_fee_invoice then invoice_date else cast(null as TIMESTAMP WITH TIME ZONE) end, 
            case when is_listing_fee_invoice then invoice_id   else cast(null as varchar)   end
            ) as single_listing_fee_invoice_id_or_null_dense_rank,

    -- RESALE invoice columns:
    -- We use single_resale_invoice_id_or_null_dense_rank as a guard on exceeding the listagg 65535 character limit in the next transformation.
    -- NOTE: from our current understanding a resale invoice have to be refunded with the buyer invoice together, and can not be refunded by it self, so not yet need a list
    --       we should receive a PK validation failure if that was to occur
    case when is_resale_invoice then invoice_date else cast(null as TIMESTAMP WITH TIME ZONE) end as single_resale_invoice_date_or_null,
    case when is_resale_invoice then invoice_id   else cast(null as varchar)   end as single_resale_invoice_id_or_null,
    dense_rank() over (
        partition by internal_buyer_invoice_line_item_surrogate_id_with_currency 
        order by 
            case when is_resale_invoice then invoice_date else cast(null as TIMESTAMP WITH TIME ZONE) end, 
            case when is_resale_invoice then invoice_id   else cast(null as varchar)   end
            ) as single_resale_invoice_id_or_null_dense_rank,

    -- Categorized amounts by transaction type.  These categorization are later surfaced in the dashboards as amount columns and used to calculate net revenue/seller payable.
    -- We calculate and surface these amounts in three currencies:
    -- 1. Pricing: This is the default currency as surface by us and is the price on the offer. Columns might be surfaced as gross_revenue, seller_tax_share, etc.
    -- 2. Disbursement: This is the currency that the seller is disbursed in.  Columns might be surfaced as gross_revenue_disbursement_currency, seller_tax_share_disbursement_currency, etc.
    -- 3. Legal: This is the legal or local currency for tax records.  This is not currently surfaced but only calculated.

    -- Pricing Currency --------------------------------------------------------------------------------------------------------------------------------------------------------

    -- Gross amounts: The product or wholesale revenue for a seller.  Does not include any costs such as taxes, listing fee, etc.
    case when transaction_type =   'SELLER_REV_SHARE' and not is_cog then amount else 0 end as gross_revenue,
    case when transaction_type in ('SELLER_REV_SHARE_REFUND','SELLER_REV_SHARE_CREDIT') and not is_cog_refund then amount else 0 end as gross_refund,

    -- Cost of goods: The cost a reseller pays to the manufacture.
    case when transaction_type =   'SELLER_REV_SHARE' and     is_cog then amount else 0 end as cogs,
    case when transaction_type in ('SELLER_REV_SHARE_REFUND','SELLER_REV_SHARE_CREDIT') and is_cog_refund then amount else 0 end as cogs_refund,

    -- AWS rev share: This is the listing fee.
    case when transaction_type =   'AWS_REV_SHARE' then amount else 0 end as aws_rev_share,
    case when transaction_type in ('AWS_REV_SHARE_REFUND','AWS_REV_SHARE_CREDIT') then amount else 0 end as aws_refund_share,

    -- AWS tax share: This is the share of taxes AWS remits from the product charge.
    -- This excludes wholesale and listing fee taxes which are surfaced in their own columns
    case when transaction_type =   'AWS_TAX_SHARE' and not is_listing_fee_charge and not is_resale_invoice then amount else 0 end as aws_tax_share,             -- AWS tax share from _buyer_  invoice
    case when transaction_type =   'AWS_TAX_SHARE_REFUND' and not is_listing_fee_charge and not is_resale_invoice then amount else 0 end as aws_tax_share_refund,

    -- AWS tax share listing fee: The GST/Sales Tax/etc. (indirect taxes) that AWS collected from the Manufacturer and remitted to the tax authority
    case when transaction_type =   'AWS_TAX_SHARE' and     is_listing_fee_charge and not is_resale_invoice then amount else 0 end as aws_tax_share_listing_fee, -- AWS tax share from _seller_ invoice
    case when transaction_type =   'AWS_TAX_SHARE_REFUND' and     is_listing_fee_charge and not is_resale_invoice then amount else 0 end as aws_tax_share_refund_listing_fee,

    -- Seller tax share: tax that is disbursed to the seller to remit.  This is specifically product charge tax and does not
    -- include tax on the wholesale charge which has its own column.
    case when transaction_type =   'SELLER_TAX_SHARE' and not is_resale_invoice then amount else 0 end as seller_tax_share,
    case when transaction_type =   'SELLER_TAX_SHARE_REFUND' and not is_resale_invoice then amount else 0 end as seller_tax_share_refund,

    -- Other seller tax share: tax that is disbursed to a different seller to pay and is deducted from the amounts that is
    -- disbursed to this seller to pay.
    -- This is specifically product charge tax and does not include tax on the wholesale charge which has its own column.
    case when transaction_type =   'OTHER_SELLER_TAX_SHARE' and not is_resale_invoice then amount else 0 end as other_seller_tax_share,
    case when transaction_type =   'OTHER_SELLER_TAX_SHARE_REFUND' and not is_resale_invoice then amount else 0 end as other_seller_tax_share_refund,

    -- Wholesale aws tax share: AWS tax share from the wholesale charge excluding listing fee and product charges which
    -- have their own columns:

    case when transaction_type =   'AWS_TAX_SHARE' and is_resale_invoice then amount else 0 end as wholesale_aws_tax_share,
    case when transaction_type =   'AWS_TAX_SHARE_REFUND' and is_resale_invoice then amount else 0 end as wholesale_aws_tax_share_refund,

    -- AWS Tax Share balance impacting: An internal column used to help with the calculation of net revenue, disbursed and undisbursed net revenue.
    -- NOTE: net revenue is named in a misleading way.  It is actually net disbursed amount or seller payable and includes
    -- taxes the seller must pay.
    -- NOTE: Since we do not surface what portion of AWS_TAX_SHARE is balance impacting, and gross_revenue does not include
    -- taxes collected from the buyer, it is impossible for sellers to calculate their net revenue or net disbursement
    -- from other surfaced columns.
    case when transaction_type = 'AWS_TAX_SHARE' and (balance_impacting or action='DISBURSED') then amount else 0 end as aws_tax_share_balance_impacting,
    case when transaction_type = 'AWS_TAX_SHARE_REFUND' and (balance_impacting or action='DISBURSED') then amount else 0 end as aws_tax_share_refund_balance_impacting,

    -- Wholesale_aws_tax_share_balance_impacting: helper column, used for calculating seller_net_revenue/disbursed_net_revenue/undisbursed_net_revenue etc.
    -- for wholesale invoice AWS tax share, currently the tax is paid by Reseller to AWS, then emitted by AWS
    -- so it only needed to be accounted in net revenue for resellers not ISV
    -- background: the reason why it's named _balance_impacting is when we first implemented this, we didn't have to calculate the new disbursed/undibursed net revenue
    -- billing records with action='invoiced' has balance impacting properly configured, but 'disbursed' are all set to zero

    case when transaction_type = 'AWS_TAX_SHARE' and is_resale_invoice and (balance_impacting or action='DISBURSED')
        then amount else 0 end as wholesale_aws_tax_share_balance_impacting,
    case when transaction_type =   'AWS_TAX_SHARE_REFUND' and is_resale_invoice and (balance_impacting or (action = 'DISBURSED' and transaction_type not in ('DISBURSEMENT', 'DISBURSEMENT_FAILURE') and not is_isv_wholesale_invoice))
        then amount else 0 end as wholesale_aws_tax_share_refund_balance_impacting,

    -- Seller tax share: tax that is disbursed to the seller to remit.  This is specifically wholesale charge tax and does not
    -- include tax on the product charge which has its own column.

    case when transaction_type =   'SELLER_TAX_SHARE' and is_resale_invoice then amount else 0 end as wholesale_seller_tax_share,
    case when transaction_type =   'SELLER_TAX_SHARE_REFUND' and is_resale_invoice then amount else 0 end as wholesale_seller_tax_share_refund,

    -- Other seller tax share: tax that is disbursed to a different seller to pay and is deducted from the amounts that is
    -- disbursed to this seller to pay.
    -- This is specifically wholesale charge tax and does not include tax on the wholesale charge which has its own column.
    -- For the reseller charge this is typically the portion of an indirect tax that is the responsibility of the
    -- ISV to remit.

    case when transaction_type =   'OTHER_SELLER_TAX_SHARE' and is_resale_invoice then amount else 0 end as wholesale_other_seller_tax_share,
    case when transaction_type =   'OTHER_SELLER_TAX_SHARE_REFUND' and is_resale_invoice then amount else 0 end as wholesale_other_seller_tax_share_refund,

    -- This is for Bedrock sellers, but it is being phased out as of May, 2025:
    case when transaction_type IN   ('AWS_INFRA_SHARE', 'INFRA_FEE_PRETAX' /* old name */) then amount else 0 end as infrastructure_netting,

    case when transaction_type =   'BALANCE_ADJUSTMENT' then amount else 0 end as balance_adjustment,
    case when transaction_type =   'SELLER_REV_SHARE_CREDIT' then amount else 0 end as seller_rev_credit,
    case when transaction_type =   'AWS_REV_SHARE_CREDIT' then amount else 0 end as aws_ref_fee_credit,

    case when balance_impacting or action in ('DISBURSED', 'COLLECTED') then amount else 0 end as all_balance_impacting,

    -- Disbursement Currency --------------------------------------------------------------------------------------------------------------------------------------------------------

    -- Gross amounts: The product or wholesale revenue for a seller.  Does not include any costs such as taxes, listing fee, etc.
    case when transaction_type =   'SELLER_REV_SHARE' and not is_cog then disbursement_amount else 0 end as gross_revenue_disbursement_currency,
    case when transaction_type in ('SELLER_REV_SHARE_REFUND','SELLER_REV_SHARE_CREDIT') and not is_cog_refund then disbursement_amount else 0 end as gross_refund_disbursement_currency,

    -- Cost of goods: The cost a reseller pays to the manufacture.
    case when transaction_type =   'SELLER_REV_SHARE' and     is_cog then disbursement_amount else 0 end as cogs_disbursement_currency,
    case when transaction_type in ('SELLER_REV_SHARE_REFUND','SELLER_REV_SHARE_CREDIT') and is_cog_refund then disbursement_amount else 0 end as cogs_refund_disbursement_currency,

    -- AWS rev share: This is the listing fee.
    case when transaction_type =   'AWS_REV_SHARE' then disbursement_amount else 0 end as aws_rev_share_disbursement_currency,
    case when transaction_type in ('AWS_REV_SHARE_REFUND','AWS_REV_SHARE_CREDIT') then disbursement_amount else 0 end as aws_refund_share_disbursement_currency,

    -- AWS tax share: This is the share of taxes AWS remits from the product charge.
    -- This excludes wholesale and listing fee taxes which are surfaced in their own columns
    case when transaction_type =   'AWS_TAX_SHARE' and not is_listing_fee_charge and not is_resale_invoice then disbursement_amount else 0 end as aws_tax_share_disbursement_currency,             -- AWS tax share from _buyer_  invoice
    case when transaction_type =   'AWS_TAX_SHARE_REFUND' and not is_listing_fee_charge and not is_resale_invoice then disbursement_amount else 0 end as aws_tax_share_refund_disbursement_currency,

    -- AWS tax share listing fee: The GST/Sales Tax/etc. (indirect taxes) that AWS collected from the Manufacturer and remitted to the tax authority
    case when transaction_type =   'AWS_TAX_SHARE' and     is_listing_fee_charge and not is_resale_invoice then disbursement_amount else 0 end as aws_tax_share_listing_fee_disbursement_currency, -- AWS tax share from _seller_ invoice
    case when transaction_type =   'AWS_TAX_SHARE_REFUND' and     is_listing_fee_charge and not is_resale_invoice then disbursement_amount else 0 end as aws_tax_share_refund_listing_fee_disbursement_currency,

    -- Seller tax share: tax that is disbursed to the seller to remit.  This is specifically product charge tax and does not
    -- include tax on the wholesale charge which has its own column.
    case when transaction_type =   'SELLER_TAX_SHARE' and not is_resale_invoice then disbursement_amount else 0 end as seller_tax_share_disbursement_currency,
    case when transaction_type =   'SELLER_TAX_SHARE_REFUND' and not is_resale_invoice then disbursement_amount else 0 end as seller_tax_share_refund_disbursement_currency,

    -- Other seller tax share: tax that is disbursed to a different seller to pay and is deducted from the amounts that is
    -- disbursed to this seller to pay.
    -- This is specifically product charge tax and does not include tax on the wholesale charge which has its own column.
    case when transaction_type =   'OTHER_SELLER_TAX_SHARE' and not is_resale_invoice then disbursement_amount else 0 end as other_seller_tax_share_disbursement_currency,
    case when transaction_type =   'OTHER_SELLER_TAX_SHARE_REFUND' and not is_resale_invoice then disbursement_amount else 0 end as other_seller_tax_share_refund_disbursement_currency,

    -- Wholesale aws tax share: AWS tax share from the wholesale charge excluding listing fee and product charges which
    -- have their own columns.
    case when transaction_type =   'AWS_TAX_SHARE' and is_resale_invoice then disbursement_amount else 0 end as wholesale_aws_tax_share_disbursement_currency,
    case when transaction_type =   'AWS_TAX_SHARE_REFUND' and is_resale_invoice then disbursement_amount else 0 end as wholesale_aws_tax_share_refund_disbursement_currency,

    -- AWS Tax Share balance impacting: An internal column used to help with the calculation of net revenue, disbursed and undisbursed net revenue.
    -- NOTE: net revenue is named in a misleading way.  It is actually net disbursed amount or seller payable and includes
    -- taxes the seller must pay.
    -- NOTE: Since we do not surface what portion of AWS_TAX_SHARE is balance impacting, and gross_revenue does not include
    -- taxes collected from the buyer, it is impossible for sellers to calculate their net revenue or net disbursement
    -- from other surfaced columns.
    case when transaction_type = 'AWS_TAX_SHARE' and (balance_impacting or action='DISBURSED') then disbursement_amount else 0 end as aws_tax_share_balance_impacting_disbursement_currency,
    case when transaction_type = 'AWS_TAX_SHARE_REFUND' and (balance_impacting or action='DISBURSED') then disbursement_amount else 0 end as aws_tax_share_balance_impacting_refund_disbursement_currency,

    -- Wholesale_aws_tax_share_balance_impacting: helper column, used for calculating seller_net_revenue/disbursed_net_revenue/undisbursed_net_revenue etc.
    -- for wholesale invoice AWS tax share, currently the tax is paid by Reseller to AWS, then emitted by AWS
    -- so it only needed to be accounted in net revenue for resellers not ISV
    -- background: the reason why it's named _balance_impacting is when we first implemented this, we didn't have to calculate the new disbursed/undibursed net revenue
    -- billing records with action='invoiced' has balance impacting properly configured, but 'disbursed' are all set to zero
    case
        when transaction_type = 'AWS_TAX_SHARE' and is_resale_invoice and (balance_impacting or action='DISBURSED')
            then disbursement_amount else 0 end as wholesale_aws_tax_share_balance_impacting_disbursement_currency,
    case when transaction_type =   'AWS_TAX_SHARE_REFUND' and is_resale_invoice and (balance_impacting or (action = 'DISBURSED' and transaction_type not in ('DISBURSEMENT', 'DISBURSEMENT_FAILURE') and not is_isv_wholesale_invoice))
             then disbursement_amount else 0 end as wholesale_aws_tax_share_refund_balance_impacting_disbursement_currency,

    -- Seller tax share: tax that is disbursed to the seller to remit.  This is specifically wholesale charge tax and does not
    -- include tax on the product charge which has its own column.
    case when transaction_type =   'SELLER_TAX_SHARE' and is_resale_invoice then disbursement_amount else 0 end as wholesale_seller_tax_share_disbursement_currency,
    case when transaction_type =   'SELLER_TAX_SHARE_REFUND' and is_resale_invoice then disbursement_amount else 0 end as wholesale_seller_tax_share_refund_disbursement_currency,

    -- Other seller tax share: tax that is disbursed to a different seller to pay and is deducted from the amounts that is
    -- disbursed to this seller to pay.
    -- This is specifically wholesale charge tax and does not include tax on the wholesale charge which has its own column.
    -- For the reseller charge this is typically the portion of an indirect tax that is the responsibility of the
    -- ISV to remit.
    case when transaction_type =   'OTHER_SELLER_TAX_SHARE' and is_resale_invoice then disbursement_amount else 0 end as wholesale_other_seller_tax_share_disbursement_currency,
    case when transaction_type =   'OTHER_SELLER_TAX_SHARE_REFUND' and is_resale_invoice then disbursement_amount else 0 end as wholesale_other_seller_tax_share_refund_disbursement_currency,

    -- This is for Bedrock sellers, but it is being phased out as of May, 2025:
    case when transaction_type IN   ('AWS_INFRA_SHARE', 'INFRA_FEE_PRETAX' /* old name */) then disbursement_amount else 0 end as infrastructure_netting_disbursement_currency,

    case when transaction_type =   'BALANCE_ADJUSTMENT' then disbursement_amount else 0 end as balance_adjustment_disbursement_currency,
    case when transaction_type =   'SELLER_REV_SHARE_CREDIT' then disbursement_amount else 0 end as seller_rev_credit_disbursement_currency,
    case when transaction_type =   'AWS_REV_SHARE_CREDIT' then disbursement_amount else 0 end as aws_ref_fee_credit_disbursement_currency,

    case when balance_impacting or action in ('DISBURSED', 'COLLECTED') then disbursement_amount else 0 end as all_balance_impacting_disbursement_currency,

    is_seller_issued_invoice,

    case when is_seller_issued_invoice then invoice_date      else cast(null as TIMESTAMP WITH TIME ZONE) end as single_seller_issued_invoice_date_or_null,
    case when is_seller_issued_invoice then invoice_variant   else cast(null as varchar)   end as single_seller_issued_invoice_variant_or_null,
    case when is_seller_issued_invoice then invoice_id        else cast(null as varchar)   end as single_seller_issued_invoice_id_or_null,

    dense_rank() over (
        partition by internal_buyer_invoice_line_item_surrogate_id_with_currency 
        order by 
            case when is_seller_issued_invoice then invoice_date      else cast(null as TIMESTAMP WITH TIME ZONE) end, 
            case when is_seller_issued_invoice then invoice_variant   else cast(null as varchar)   end, 
            case when is_seller_issued_invoice then invoice_id        else cast(null as varchar)   end
            ) as single_seller_issued_invoice_id_or_null_dense_rank,

    bl.min_data_catalog as min_data_catalog,
    bl.max_data_catalog as max_data_catalog,
    data_catalog

from
    billing_event_with_business_flags as bl
),

-- ============================================================================
-- Invoice Listagg CTEs
-- Athena's listagg doesn't support PARTITION BY, so we need separate CTEs
-- with GROUP BY to aggregate invoice IDs into comma-separated lists.
-- Each uses a dense_rank guard (< 10) to prevent exceeding the 65535 char limit.
-- ============================================================================

-- Aggregate listing fee invoice IDs into comma-separated lists per line item surrogate ID
listing_fee_invoice_id_listagg_columns as (
select
    internal_buyer_invoice_line_item_surrogate_id_with_currency,
    listagg(case when single_listing_fee_invoice_id_or_null_dense_rank < 10 then cast(single_listing_fee_invoice_date_or_null as varchar) else '...' end, ',') within group (order by single_listing_fee_invoice_id_or_null_dense_rank) as listing_fee_invoice_date_or_ellipsis,
    listagg(case when single_listing_fee_invoice_id_or_null_dense_rank < 10 then single_listing_fee_invoice_id_or_null else '...' end, ',') within group (order by single_listing_fee_invoice_id_or_null_dense_rank) as listing_fee_invoice_id_or_ellipsis
from (
    select distinct internal_buyer_invoice_line_item_surrogate_id_with_currency, single_listing_fee_invoice_id_or_null_dense_rank, single_listing_fee_invoice_id_or_null, single_listing_fee_invoice_date_or_null
    from billing_event_with_categorized_transaction
) distinct_line_item_surrogate
group by internal_buyer_invoice_line_item_surrogate_id_with_currency
),

-- Aggregate resale invoice IDs into comma-separated lists per line item surrogate ID
resale_invoice_id_listagg_columns as (
select
    internal_buyer_invoice_line_item_surrogate_id_with_currency,
    listagg(case when single_resale_invoice_id_or_null_dense_rank < 10 then cast(single_resale_invoice_date_or_null as varchar) else '...' end, ',') within group (order by single_resale_invoice_id_or_null_dense_rank) as resale_invoice_date_or_ellipsis,
    listagg(case when single_resale_invoice_id_or_null_dense_rank < 10 then single_resale_invoice_id_or_null else '...' end, ',') within group (order by single_resale_invoice_id_or_null_dense_rank) as resale_invoice_id_or_ellipsis
from (
    select distinct internal_buyer_invoice_line_item_surrogate_id_with_currency, single_resale_invoice_id_or_null_dense_rank, single_resale_invoice_id_or_null, single_resale_invoice_date_or_null
    from billing_event_with_categorized_transaction
) distinct_line_item_surrogate
group by internal_buyer_invoice_line_item_surrogate_id_with_currency
),

-- Aggregate seller-issued (TAX_VAT) invoice IDs and variants into comma-separated lists per line item surrogate ID
seller_issued_invoice_id_listagg_columns as (
select
    internal_buyer_invoice_line_item_surrogate_id_with_currency,
    listagg(case when single_seller_issued_invoice_id_or_null_dense_rank < 10 then single_seller_issued_invoice_id_or_null else '...' end, ',') within group (order by single_seller_issued_invoice_id_or_null_dense_rank) as seller_issued_invoice_id_or_ellipsis,
    listagg(case when single_seller_issued_invoice_id_or_null_dense_rank < 10 then single_seller_issued_invoice_variant_or_null else '...' end, ',') within group (order by single_seller_issued_invoice_id_or_null_dense_rank) as seller_issued_invoice_variant_or_ellipsis
from (
    select distinct internal_buyer_invoice_line_item_surrogate_id_with_currency, single_seller_issued_invoice_id_or_null_dense_rank, single_seller_issued_invoice_id_or_null, single_seller_issued_invoice_variant_or_null
    from billing_event_with_categorized_transaction
) distinct_line_item_surrogate
group by internal_buyer_invoice_line_item_surrogate_id_with_currency
),

-- Use the flags that were created in the previous transformation in more calculated columns:
-- NOTE: This transformation has no joins and no window functions

billing_event_with_listagg as (
select
    recipient_account_id,
    billing_event_id,
    end_user_account_id,
    agreement_id,
    wholesale_agreement_id,
    proposer_account_id,
    offer_id,
    acceptor_account_id,
    payer_account_id,
    product_id,
    action,
    transaction_type,
    parent_billing_event_id,
    disbursement_billing_event_id,
    amount,
    currency,
    disbursement_amount,
    disbursement_currency,
    balance_impacting,
    invoice_date,
    payment_due_date,
    usage_period_start_date,
    usage_period_end_date,
    invoice_id,
    line_item_id,
    purchase_invoice_id,
    buyer_line_item_id,
    invoice_variant,
    charge_variant,
    charge_side,
    billing_address_id,
    transaction_reference_id,
    buyer_transaction_reference_id,
    bank_trace_id,
    amazon_reference_id,
    collection_date,
    disbursement_date,
    disbursement_id,
    disbursement_status,
    disbursement_id_or_invoiced_or_collected,
    broker_id,
    internal_buyer_invoice_line_item_surrogate_id,
    bl.internal_buyer_invoice_line_item_surrogate_id_with_currency,
    is_listing_fee_invoice,
    is_listing_fee_charge,
    is_cog,
    is_cog_refund,
    is_manufacturer_view_of_reseller,
    is_isv_wholesale_invoice,

    -- BUYER invoice columns:
    purchase_invoice_id_or_null,
    purchase_invoice_date_or_null,

    -- SELLER invoice columns:
    -- the reason why one buyer invoice line item can be link to multiple seller invoice, is because seller invoice can be refunded and reissued linking to the same buyer invoice:
    single_listing_fee_invoice_date_or_null,
    single_listing_fee_invoice_id_or_null,
    single_listing_fee_invoice_id_or_null_dense_rank,
    -- We use single_listing_fee_invoice_id_or_null_dense_rank as a guard on exceeding the listagg 65535 character limit in the next transformation:

    -- We use dense_rank() to get the first 10 seller invoice dates and IDs, and then use listagg to concatenate the remaining seller invoice dates and IDs:
    listing_fee_invoice_id_listagg.listing_fee_invoice_date_or_ellipsis,
    nullif(listing_fee_invoice_id_listagg.listing_fee_invoice_date_or_ellipsis, '...') as listing_fee_invoice_date_or_null, -- If every seller invoice is null (should never happen) then we want the value to be null not '...'
    listing_fee_invoice_id_listagg.listing_fee_invoice_id_or_ellipsis,
    nullif(listing_fee_invoice_id_listagg.listing_fee_invoice_id_or_ellipsis, '...') as listing_fee_invoice_id_or_null,

    -- RESALE invoice columns:
    -- from our current understanding a resale invoice have to be refunded with the buyer invoice together, and can not be refunded by it self, so not yet need a list
    -- we should receive a PK validation failure if that was to occur
    single_resale_invoice_date_or_null,
    single_resale_invoice_id_or_null,
    single_resale_invoice_id_or_null_dense_rank,
    -- We use single_resale_invoice_id_or_null_dense_rank as a guard on exceeding the listagg 65535 character limit in the next transformation:

    resale_invoice_id_listagg.resale_invoice_date_or_ellipsis,
    nullif(resale_invoice_id_listagg.resale_invoice_date_or_ellipsis, '...') as resale_invoice_date_or_null,
    resale_invoice_id_listagg.resale_invoice_id_or_ellipsis,
    nullif(resale_invoice_id_listagg.resale_invoice_id_or_ellipsis, '...') as resale_invoice_id_or_null,

    -- Categorized amounts by transaction type.  These categorization are later surfaced in the dashboards as amount columns and used to calculate net revenue/seller payable.
    -- We calculate and surface these amounts in three currencies:
    -- 1. Pricing: This is the default currency as surface by us and is the price on the offer. Columns might be surfaced as gross_revenue, seller_tax_share, etc.
    -- 2. Disbursement: This is the currency that the seller is disbursed in.  Columns might be surfaced as gross_revenue_disbursement_currency, seller_tax_share_disbursement_currency, etc.
    -- 3. Legal: This is the legal or local currency for tax records.  This is not currently surfaced but only calculated.

    -- Pricing Currency --------------------------------------------------------------------------------------------------------------------------------------------------------

    -- Gross amounts: The product or wholesale revenue for a seller.  Does not include any costs such as taxes, listing fee, etc.
    gross_revenue,
    gross_refund,

    -- Cost of goods: The cost a reseller pays to the manufacture.
    cogs,
    cogs_refund,

    -- AWS rev share: This is the listing fee.
    aws_rev_share,
    aws_refund_share,

    -- AWS tax share: This is the share of taxes AWS remits from the product charge.
    -- This excludes wholesale and listing fee taxes which are surfaced in their own columns
    aws_tax_share,             -- AWS tax share from _buyer_  invoice
    aws_tax_share_refund,

    -- AWS tax share listing fee: The GST/Sales Tax/etc. (indirect taxes) that AWS collected from the Manufacturer and remitted to the tax authority
    aws_tax_share_listing_fee, -- AWS tax share from _seller_ invoice
    aws_tax_share_refund_listing_fee,

    -- Seller tax share: tax that is disbursed to the seller to remit.  This is specifically product charge tax and does not
    -- include tax on the wholesale charge which has its own column.
    seller_tax_share,
    seller_tax_share_refund,

    -- Other seller tax share: tax that is disbursed to a different seller to pay and is deducted from the amounts that is
    -- disbursed to this seller to pay.
    -- This is specifically product charge tax and does not include tax on the wholesale charge which has its own column.
    other_seller_tax_share,
    other_seller_tax_share_refund,

    -- Wholesale aws tax share: AWS tax share from the wholesale charge excluding listing fee and product charges which
    -- have their own columns.
    wholesale_aws_tax_share,
    wholesale_aws_tax_share_refund,

    -- AWS Tax Share balance impacting: An internal column used to help with the calculation of net revenue, disbursed and undisbursed net revenue.
    -- NOTE: net revenue is named in a misleading way.  It is actually net disbursed amount or seller payable and includes
    -- taxes the seller must pay.
    -- NOTE: Since we do not surface what portion of AWS_TAX_SHARE is balance impacting, and gross_revenue does not include
    -- taxes collected from the buyer, it is impossible for sellers to calculate their net revenue or net disbursement
    -- from other surfaced columns.
    aws_tax_share_balance_impacting,
    aws_tax_share_refund_balance_impacting,

    -- Wholesale_aws_tax_share_balance_impacting: helper column, used for calculating seller_net_revenue/disbursed_net_revenue/undisbursed_net_revenue etc.
    -- for wholesale invoice AWS tax share, currently the tax is paid by Reseller to AWS, then emitted by AWS
    -- so it only needed to be accounted in net revenue for resellers not ISV
    -- background: the reason why it's named _balance_impacting is when we first implemented this, we didn't have to calculate the new disbursed/undibursed net revenue
    -- billing records with action='invoiced' has balance impacting properly configured, but 'disbursed' are all set to zero
    wholesale_aws_tax_share_balance_impacting,
    wholesale_aws_tax_share_refund_balance_impacting,

    -- Seller tax share: tax that is disbursed to the seller to remit.  This is specifically wholesale charge tax and does not
    -- include tax on the product charge which has its own column.
    wholesale_seller_tax_share,
    wholesale_seller_tax_share_refund,

    -- Other seller tax share: tax that is disbursed to a different seller to pay and is deducted from the amounts that is
    -- disbursed to this seller to pay.
    -- This is specifically wholesale charge tax and does not include tax on the wholesale charge which has its own column.
    -- For the reseller charge this is typically the portion of an indirect tax that is the responsibility of the
    -- ISV to remit.
    wholesale_other_seller_tax_share,
    wholesale_other_seller_tax_share_refund,

    -- This is for Bedrock sellers, but it is being phased out as of May, 2025:
    infrastructure_netting,

    balance_adjustment,
    seller_rev_credit,
    aws_ref_fee_credit,

    all_balance_impacting,


    -- Disbursement Currency --------------------------------------------------------------------------------------------------------------------------------------------------------
    -- Gross amounts: The product or wholesale revenue for a seller.  Does not include any costs such as taxes, listing fee, etc.
    gross_revenue_disbursement_currency,
    gross_refund_disbursement_currency,

    -- Cost of goods: The cost a reseller pays to the manufacture.
    cogs_disbursement_currency,
    cogs_refund_disbursement_currency,

    -- AWS rev share: This is the listing fee.
    aws_rev_share_disbursement_currency,
    aws_refund_share_disbursement_currency,

    -- AWS tax share: This is the share of taxes AWS remits from the product charge.
    -- This excludes wholesale and listing fee taxes which are surfaced in their own columns
    aws_tax_share_disbursement_currency,             -- AWS tax share from _buyer_  invoice
    aws_tax_share_refund_disbursement_currency,

    -- AWS tax share listing fee: The GST/Sales Tax/etc. (indirect taxes) that AWS collected from the Manufacturer and remitted to the tax authority
    aws_tax_share_listing_fee_disbursement_currency, -- AWS tax share from _seller_ invoice
    aws_tax_share_refund_listing_fee_disbursement_currency,

    -- Seller tax share: tax that is disbursed to the seller to remit.  This is specifically product charge tax and does not
    -- include tax on the wholesale charge which has its own column.
    seller_tax_share_disbursement_currency,
    seller_tax_share_refund_disbursement_currency,

    -- Other seller tax share: tax that is disbursed to a different seller to pay and is deducted from the amounts that is
    -- disbursed to this seller to pay.
    -- This is specifically product charge tax and does not include tax on the wholesale charge which has its own column.
    other_seller_tax_share_disbursement_currency,
    other_seller_tax_share_refund_disbursement_currency,

    -- Wholesale aws tax share: AWS tax share from the wholesale charge excluding listing fee and product charges which
    -- have their own columns.
    wholesale_aws_tax_share_disbursement_currency,
    wholesale_aws_tax_share_refund_disbursement_currency,

    -- AWS Tax Share balance impacting: An internal column used to help with the calculation of net revenue, disbursed and undisbursed net revenue.
    -- NOTE: net revenue is named in a misleading way.  It is actually net disbursed amount or seller payable and includes
    -- taxes the seller must pay.
    -- NOTE: Since we do not surface what portion of AWS_TAX_SHARE is balance impacting, and gross_revenue does not include
    -- taxes collected from the buyer, it is impossible for sellers to calculate their net revenue or net disbursement
    -- from other surfaced columns.
    aws_tax_share_balance_impacting_disbursement_currency,
    aws_tax_share_balance_impacting_refund_disbursement_currency,

    -- Wholesale_aws_tax_share_balance_impacting: helper column, used for calculating seller_net_revenue/disbursed_net_revenue/undisbursed_net_revenue etc.
    -- for wholesale invoice AWS tax share, currently the tax is paid by Reseller to AWS, then emitted by AWS
    -- so it only needed to be accounted in net revenue for resellers not ISV
    -- background: the reason why it's named _balance_impacting is when we first implemented this, we didn't have to calculate the new disbursed/undibursed net revenue
    -- billing records with action='invoiced' has balance impacting properly configured, but 'disbursed' are all set to zero
    wholesale_aws_tax_share_balance_impacting_disbursement_currency,
    wholesale_aws_tax_share_refund_balance_impacting_disbursement_currency,

    -- Seller tax share: tax that is disbursed to the seller to remit.  This is specifically wholesale charge tax and does not
    -- include tax on the product charge which has its own column.
    wholesale_seller_tax_share_disbursement_currency,
    wholesale_seller_tax_share_refund_disbursement_currency,

    -- Other seller tax share: tax that is disbursed to a different seller to pay and is deducted from the amounts that is
    -- disbursed to this seller to pay.
    -- This is specifically wholesale charge tax and does not include tax on the wholesale charge which has its own column.
    -- For the reseller charge this is typically the portion of an indirect tax that is the responsibility of the
    -- ISV to remit.
    wholesale_other_seller_tax_share_disbursement_currency,
    wholesale_other_seller_tax_share_refund_disbursement_currency,

    -- This is for Bedrock sellers, but it is being phased out as of May, 2025:
    infrastructure_netting_disbursement_currency,

    balance_adjustment_disbursement_currency,
    seller_rev_credit_disbursement_currency,
    aws_ref_fee_credit_disbursement_currency,

    all_balance_impacting_disbursement_currency,

    -- AWS invoice columns (for deemed VAT (and future) where AWS is the recipient):
    single_seller_issued_invoice_date_or_null,
    single_seller_issued_invoice_variant_or_null,
    single_seller_issued_invoice_id_or_null,
    single_seller_issued_invoice_id_or_null_dense_rank,
    -- We use single_seller_issued_invoice_id_or_null_dense_rank as a guard on exceeding the listagg 65535 character limit in the next transformation:
    seller_issued_invoice_id_listagg.seller_issued_invoice_id_or_ellipsis,
    nullif(seller_issued_invoice_id_listagg.seller_issued_invoice_id_or_ellipsis, '...') as seller_issued_invoice_id_or_null,            -- If every deemed vat invoice is null (happens most of the time!) then we want the value to be null not '...'
    seller_issued_invoice_id_listagg.seller_issued_invoice_variant_or_ellipsis,
    nullif(seller_issued_invoice_id_listagg.seller_issued_invoice_variant_or_ellipsis, '...') as seller_issued_invoice_variant_or_null,

    bl.min_data_catalog as min_data_catalog,
    bl.max_data_catalog as max_data_catalog,
    data_catalog
from billing_event_with_categorized_transaction as bl
left join listing_fee_invoice_id_listagg_columns listing_fee_invoice_id_listagg
    on bl.internal_buyer_invoice_line_item_surrogate_id_with_currency = listing_fee_invoice_id_listagg.internal_buyer_invoice_line_item_surrogate_id_with_currency
left join resale_invoice_id_listagg_columns resale_invoice_id_listagg
    on bl.internal_buyer_invoice_line_item_surrogate_id_with_currency = resale_invoice_id_listagg.internal_buyer_invoice_line_item_surrogate_id_with_currency
left join seller_issued_invoice_id_listagg_columns seller_issued_invoice_id_listagg
    on bl.internal_buyer_invoice_line_item_surrogate_id_with_currency = seller_issued_invoice_id_listagg.internal_buyer_invoice_line_item_surrogate_id_with_currency
),

-- ============================================================================
-- Line Item Aggregation CTE
-- Aggregates financial amounts by summing within each extended surrogate ID
-- and disbursement grouping. Uses MAX for non-financial columns (same value
-- within group) and SUM for all financial buckets.
-- NOTE: This is the only GROUP BY in the entire CTE chain.
-- NOTE: This transformation has no joins and no window functions
-- ============================================================================

line_items_aggregated as (
select
    internal_buyer_invoice_line_item_surrogate_id_with_currency,
    count(distinct coalesce(currency, '')) as count_distinct_pricing_currency,
    count(distinct coalesce(disbursement_currency, '')) as count_distinct_disbursement_currency,
    internal_buyer_invoice_line_item_surrogate_id,
    disbursement_id,
    disbursement_id_or_invoiced_or_collected,
    billing_listagg.recipient_account_id,
    product_id,
    broker_id,
    currency,
    disbursement_currency,

    agreement_id,
    -- wholesale agreement id is currently not in scope and will be internal use only
    cast(null as varchar) as wholesale_agreement_id,
    proposer_account_id,
    acceptor_account_id,
    max(payer_account_id) as payer_account_id,
    offer_id,
    end_user_account_id,
    usage_period_start_date,
    usage_period_end_date,
    max(payment_due_date) as payment_due_date,
    buyer_transaction_reference_id,
    -- We aggregate data by buyer invoice line item and use buyer_transaction_reference_id in the normal case.
    -- However, Anthropic SELLER invoices do not have an associated BUYER invoice, so we need need to fallback
    -- on the transaction_reference_id of the SELLER invoice:
    coalesce(buyer_transaction_reference_id, transaction_reference_id) as report_transaction_reference_id,
    bank_trace_id,
    amazon_reference_id,
    max(collection_date) as collection_date,
    disbursement_date,
    disbursement_status,
    max(billing_address_id) as billing_address_id,

    -- Buyer/seller columns:
    max(purchase_invoice_id_or_null) as purchase_invoice_id,
    max(listing_fee_invoice_id_or_null) as listing_fee_invoice_id,       -- TBD:  If there are multiple seller invoices linked to the same buyer invoice, should we show all of them?
    max(resale_invoice_id_or_null) as resale_invoice_id, -- TBD:  If there are multiple resale invoices linked to the same buyer invoice, should we show all of them?
    max(purchase_invoice_date_or_null) as purchase_invoice_date,
    max(listing_fee_invoice_date_or_null) as listing_fee_invoice_date,   -- TBD:  If there are multiple seller invoices linked to the same buyer invoice, should we show all of the listing_fee_invoice_dates?

    CASE
        WHEN MAX(CASE WHEN is_isv_wholesale_invoice THEN 1 ELSE 0 END) = 1 THEN TRUE
        ELSE FALSE
        END as is_isv_wholesale_invoice,

    -- Categorized amounts by transaction type:
    -- When disbursement_id_or_invoiced_or_collected = '<invoiced>',    these are invoiced amounts
    -- When disbursement_id_or_invoiced_or_collected = '<collected>',   all_balance_impacting_this_disbursement_id_or_invoiced_or_collected is collected amounts
    -- When disbursement_id_or_invoiced not in ('<invoiced>', '<collected>') these are disbursed amounts for _this_ specific disbursement_id

    -- Pricing Currency --
    sum(gross_revenue) as gross_revenue_this_disbursement_id_or_invoiced,
    sum(gross_refund) as gross_refund_this_disbursement_id_or_invoiced,
    sum(cogs) as cogs_this_disbursement_id_or_invoiced,
    sum(cogs_refund) as cogs_refund_this_disbursement_id_or_invoiced,
    sum(aws_rev_share) as aws_rev_share_this_disbursement_id_or_invoiced,
    sum(aws_refund_share) as aws_refund_share_this_disbursement_id_or_invoiced,
    sum(aws_tax_share) as aws_tax_share_this_disbursement_id_or_invoiced,
    sum(aws_tax_share_listing_fee) as aws_tax_share_listing_fee_this_disbursement_id_or_invoiced,
    sum(aws_tax_share_refund) as aws_tax_share_refund_this_disbursement_id_or_invoiced,
    sum(aws_tax_share_refund_listing_fee) as aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced,
    sum(seller_tax_share) as seller_tax_share_this_disbursement_id_or_invoiced,
    sum(seller_tax_share_refund) as seller_tax_share_refund_this_disbursement_id_or_invoiced,
    sum(infrastructure_netting) as infrastructure_netting_this_disbursement_id_or_invoiced,
    sum(balance_adjustment) as balance_adjustment_this_disbursement_id_or_invoiced,
    sum(seller_rev_credit) as seller_rev_credit_this_disbursement_id_or_invoiced,
    sum(aws_ref_fee_credit) as aws_ref_fee_credit_this_disbursement_id_or_invoiced,
    sum(other_seller_tax_share) as other_seller_tax_share_this_disbursement_id_or_invoiced,
    sum(other_seller_tax_share_refund) as other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

    sum(aws_tax_share_balance_impacting) as aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    sum(aws_tax_share_refund_balance_impacting) as aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,

    sum(wholesale_aws_tax_share) as wholesale_aws_tax_share_this_disbursement_id_or_invoiced,
    sum(wholesale_aws_tax_share_refund) as wholesale_aws_tax_share_refund_this_disbursement_id_or_invoiced,
    sum(wholesale_aws_tax_share_balance_impacting)        as wholesale_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    sum(wholesale_aws_tax_share_refund_balance_impacting) as wholesale_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,
    sum(wholesale_seller_tax_share) as wholesale_seller_tax_share_this_disbursement_id_or_invoiced,
    sum(wholesale_seller_tax_share_refund) as wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced,
    sum(wholesale_other_seller_tax_share) as wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced,
    sum(wholesale_other_seller_tax_share_refund) as wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

    sum(all_balance_impacting) as all_balance_impacting_this_disbursement_id_or_invoiced,
    sum(all_balance_impacting) as all_balance_impacting_this_disbursement_id_or_invoiced_or_collected,

    -- Disbursement Currency --
    sum(gross_revenue_disbursement_currency) as disbursement_gross_revenue_this_disbursement_id_or_invoiced,
    sum(gross_refund_disbursement_currency) as disbursement_gross_refund_this_disbursement_id_or_invoiced,
    sum(cogs_disbursement_currency) as disbursement_cogs_this_disbursement_id_or_invoiced,
    sum(cogs_refund_disbursement_currency) as disbursement_cogs_refund_this_disbursement_id_or_invoiced,
    sum(aws_rev_share_disbursement_currency) as disbursement_aws_rev_share_this_disbursement_id_or_invoiced,
    sum(aws_refund_share_disbursement_currency) as disbursement_aws_refund_share_this_disbursement_id_or_invoiced,
    sum(aws_tax_share_disbursement_currency) as disbursement_aws_tax_share_this_disbursement_id_or_invoiced,
    sum(aws_tax_share_refund_disbursement_currency) as disbursement_aws_tax_share_refund_this_disbursement_id_or_invoiced,
    sum(aws_tax_share_listing_fee_disbursement_currency) as disbursement_aws_tax_share_listing_fee_this_disbursement_id_or_invoiced,
    sum(aws_tax_share_refund_listing_fee_disbursement_currency) as disbursement_aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced,
    sum(seller_tax_share_disbursement_currency) as disbursement_seller_tax_share_this_disbursement_id_or_invoiced,
    sum(seller_tax_share_refund_disbursement_currency) as disbursement_seller_tax_share_refund_this_disbursement_id_or_invoiced,
    sum(other_seller_tax_share_disbursement_currency) as disbursement_other_seller_tax_share_this_disbursement_id_or_invoiced,
    sum(other_seller_tax_share_refund_disbursement_currency) as disbursement_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

    sum(aws_tax_share_balance_impacting_disbursement_currency) as disbursement_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    sum(aws_tax_share_balance_impacting_refund_disbursement_currency) as disbursement_aws_tax_share_balance_impacting_refund_this_disbursement_id_or_invoiced,

    sum(wholesale_aws_tax_share_disbursement_currency) as disbursement_wholesale_aws_tax_share_this_disbursement_id_or_invoiced,
    sum(wholesale_aws_tax_share_refund_disbursement_currency) as disbursement_wholesale_aws_tax_share_refund_this_disbursement_id_or_invoiced,
    sum(wholesale_seller_tax_share_disbursement_currency) as disbursement_wholesale_seller_tax_share_this_disbursement_id_or_invoiced,
    sum(wholesale_seller_tax_share_refund_disbursement_currency) as disbursement_wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced,
    sum(wholesale_other_seller_tax_share_disbursement_currency) as disbursement_wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced,
    sum(wholesale_other_seller_tax_share_refund_disbursement_currency) as disbursement_wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

    sum(wholesale_aws_tax_share_balance_impacting_disbursement_currency) as disbursement_wholesale_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    sum(wholesale_aws_tax_share_refund_balance_impacting_disbursement_currency) as disbursement_wholesale_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,
    sum(infrastructure_netting_disbursement_currency) as disbursement_infrastructure_netting_this_disbursement_id_or_invoiced,
    sum(balance_adjustment_disbursement_currency) as disbursement_balance_adjustment_this_disbursement_id_or_invoiced,
    sum(seller_rev_credit_disbursement_currency) as disbursement_seller_rev_credit_this_disbursement_id_or_invoiced,
    sum(aws_ref_fee_credit_disbursement_currency) as disbursement_aws_ref_fee_credit_this_disbursement_id_or_invoiced,
    sum(all_balance_impacting_disbursement_currency) as disbursement_all_balance_impacting_this_disbursement_id_or_invoiced,

    max(seller_issued_invoice_id_or_null) as seller_issued_invoice_id,      -- For deemed VAT (and future) where AWS is the recipient
    max(seller_issued_invoice_variant_or_null) as seller_issued_invoice_variant, -- For deemed VAT (and future) where AWS is the recipient

    min(billing_listagg.min_data_catalog) as min_data_catalog,
    max(billing_listagg.max_data_catalog) as max_data_catalog,
    min(data_catalog) as data_catalog
from
    billing_event_with_listagg as billing_listagg
group by
    internal_buyer_invoice_line_item_surrogate_id_with_currency,
    internal_buyer_invoice_line_item_surrogate_id,
    disbursement_id,
    disbursement_id_or_invoiced_or_collected,
    billing_listagg.recipient_account_id,
    broker_id,
    -- The following columns are included the in group by but they are intentionally omitted from the PK.
    -- These columns should have the _same_ values for each record in the PK.
    product_id,
    currency,
    disbursement_currency,
    agreement_id,
    -- wholesale agreement id is currently not in scope and will be internal use only
    -- wholesale_agreement_id,
    proposer_account_id,
    acceptor_account_id,
    offer_id,
    end_user_account_id,
    usage_period_start_date,
    usage_period_end_date,
    buyer_transaction_reference_id,
    coalesce(buyer_transaction_reference_id, transaction_reference_id),
    bank_trace_id,
    amazon_reference_id,
    disbursement_date,
    disbursement_status
),

-- ============================================================================
-- Layer 4: Window Functions & Disbursement Tracking
-- Compute disbursement dense rank, listagg columns, and window function totals
-- ============================================================================

-- Assign dense_rank to disbursement events per line item.
-- This rank is used as a guard for listagg overflow protection (50-entry limit).
line_items_with_disbursement_dense_rank as (
select
    line.*,
    dense_rank() over (
        partition by line.internal_buyer_invoice_line_item_surrogate_id_with_currency
        order by disbursement_date, bank_trace_id, amazon_reference_id
    ) as disbursement_dense_rank
from line_items_aggregated as line
),

disbursement_date_listagg_columns as (
select
    internal_buyer_invoice_line_item_surrogate_id_with_currency,
    listagg(
        case when rn <= 50 then cast(disbursement_date as varchar) else '...' end,
        ','
    ) within group (order by min_disbursement_dense_rank) as disbursement_date_or_ellipsis
from (
    select
        internal_buyer_invoice_line_item_surrogate_id_with_currency,
        disbursement_date,
        min(disbursement_dense_rank) as min_disbursement_dense_rank,
        row_number() over (
            partition by internal_buyer_invoice_line_item_surrogate_id_with_currency
            order by min(disbursement_dense_rank)
        ) as rn
    from (
        select distinct
            internal_buyer_invoice_line_item_surrogate_id_with_currency,
            disbursement_dense_rank,
            disbursement_date
        from line_items_with_disbursement_dense_rank
        where disbursement_date is not null
    ) distinct_disbursement_date
    group by internal_buyer_invoice_line_item_surrogate_id_with_currency, disbursement_date
) numbered_disbursement_date
group by internal_buyer_invoice_line_item_surrogate_id_with_currency
),

bank_trace_id_listagg_columns as (
select
    internal_buyer_invoice_line_item_surrogate_id_with_currency,
    listagg(
        case when rn <= 50 then bank_trace_id else '...' end,
        ','
    ) within group (order by min_disbursement_dense_rank) as disburse_bank_trace_id_or_ellipsis
from (
    select
        internal_buyer_invoice_line_item_surrogate_id_with_currency,
        bank_trace_id,
        min(disbursement_dense_rank) as min_disbursement_dense_rank,
        row_number() over (
            partition by internal_buyer_invoice_line_item_surrogate_id_with_currency
            order by min(disbursement_dense_rank)
        ) as rn
    from (
        select distinct
            internal_buyer_invoice_line_item_surrogate_id_with_currency,
            disbursement_dense_rank,
            bank_trace_id
        from line_items_with_disbursement_dense_rank
        where disbursement_date is not null
    ) distinct_bank_trace_ids
    group by internal_buyer_invoice_line_item_surrogate_id_with_currency, bank_trace_id
) numbered_bank_trace_ids
group by internal_buyer_invoice_line_item_surrogate_id_with_currency
),

amazon_reference_id_listagg_columns as (
select
    internal_buyer_invoice_line_item_surrogate_id_with_currency,
    listagg(
        case when rn <= 50 then amazon_reference_id else '...' end,
        ','
    ) within group (order by min_disbursement_dense_rank) as disburse_amazon_reference_id_or_ellipsis
from (
    select
        internal_buyer_invoice_line_item_surrogate_id_with_currency,
        amazon_reference_id,
        min(disbursement_dense_rank) as min_disbursement_dense_rank,
        row_number() over (
            partition by internal_buyer_invoice_line_item_surrogate_id_with_currency
            order by min(disbursement_dense_rank)
        ) as rn
    from (
        select distinct
            internal_buyer_invoice_line_item_surrogate_id_with_currency,
            disbursement_dense_rank,
            amazon_reference_id
        from line_items_with_disbursement_dense_rank
        where disbursement_date is not null
    ) distinct_amazon_reference_id
    group by internal_buyer_invoice_line_item_surrogate_id_with_currency, amazon_reference_id
) numbered_amazon_reference_id
group by internal_buyer_invoice_line_item_surrogate_id_with_currency
),


-- ============================================================================
-- Window Functions CTE
-- Computes invoiced/disbursed/collected totals using SUM() OVER window functions,
-- renames financial buckets with _this_disbursement_id_or_invoiced suffix,
-- surfaces last disbursement date/bank_trace_id/amazon_reference_id,
-- and joins listagg columns from the 3 listagg CTEs.
-- add flag next step compare gross_revenue and gross_revenue_disbursed or 
-- gross_refund and gross_refund_disbursed
-- ============================================================================

line_items_with_window_functions as (
select
    line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency,
    count_distinct_pricing_currency,
    count_distinct_disbursement_currency,
    internal_buyer_invoice_line_item_surrogate_id,
    report_transaction_reference_id,
    disbursement_id,
    disbursement_id_or_invoiced_or_collected,
    line_item.recipient_account_id,
    product_id,
    broker_id,
    currency,
    disbursement_currency,
    agreement_id,
    wholesale_agreement_id,
    proposer_account_id,
    acceptor_account_id,
    -- when there's aws_rev_Share adjustment/refund to a seller_rev_share invoice, it can happen that for the same aws_rev_share invoice_id, there are multiple disbursement events,
    -- using windows function to map payer_account_id of seller_rev_share to all corresponding aws_rev_Share
    -- IMPORTANT: For invoice and account IDs, window functions can safely partition by internal_buyer_invoice_line_item_surrogate_id, ignoring the currency:
    max(payer_account_id) over (partition by internal_buyer_invoice_line_item_surrogate_id) as payer_account_id,
    offer_id,
    end_user_account_id,
    usage_period_start_date,
    usage_period_end_date,
    payment_due_date,
    bank_trace_id,
    amazon_reference_id,
    disbursement_date,
    disbursement_status,
    billing_address_id,

    -- Buyer/seller columns:
    -- IMPORTANT: For invoice and account IDs, window functions can safely partition by internal_buyer_invoice_line_item_surrogate_id, ignoring the currency:
    max(purchase_invoice_id) over (partition by internal_buyer_invoice_line_item_surrogate_id) as purchase_invoice_id,
    listing_fee_invoice_id,
    resale_invoice_id,
    max(purchase_invoice_date) over (partition by internal_buyer_invoice_line_item_surrogate_id) as purchase_invoice_date,
    listing_fee_invoice_date,
    is_isv_wholesale_invoice,

    -- When disbursement_id_or_invoiced_or_collected = '<invoiced>', these are actually invoiced amounts
    -- When disbursement_id_or_invoiced_or_collected = '<collected>', all_balance_impacting_this_disbursement_id_or_invoiced_or_collected indicates the collected amounts, other columns are dummies
    -- In other cases, these are disbursed amounts for _this_ specific disbursement_id
    -- Pricing Currency
    gross_revenue_this_disbursement_id_or_invoiced,
    gross_refund_this_disbursement_id_or_invoiced,
    cogs_this_disbursement_id_or_invoiced,
    cogs_refund_this_disbursement_id_or_invoiced,
    aws_rev_share_this_disbursement_id_or_invoiced,
    aws_refund_share_this_disbursement_id_or_invoiced,
    aws_tax_share_this_disbursement_id_or_invoiced,
    aws_tax_share_listing_fee_this_disbursement_id_or_invoiced,
    aws_tax_share_refund_this_disbursement_id_or_invoiced,
    aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced,
    seller_tax_share_this_disbursement_id_or_invoiced,
    seller_tax_share_refund_this_disbursement_id_or_invoiced,
    infrastructure_netting_this_disbursement_id_or_invoiced,
    balance_adjustment_this_disbursement_id_or_invoiced,
    seller_rev_credit_this_disbursement_id_or_invoiced,
    aws_ref_fee_credit_this_disbursement_id_or_invoiced,
    other_seller_tax_share_this_disbursement_id_or_invoiced,
    other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

    wholesale_aws_tax_share_this_disbursement_id_or_invoiced,
    wholesale_aws_tax_share_refund_this_disbursement_id_or_invoiced,
    wholesale_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    wholesale_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,
    wholesale_seller_tax_share_this_disbursement_id_or_invoiced,
    wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced,
    wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced,
    wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

    aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,

    -- TODO: To be removed.
    all_balance_impacting_this_disbursement_id_or_invoiced_or_collected as all_balance_impacting_this_disbursement_id_or_invoiced,

    all_balance_impacting_this_disbursement_id_or_invoiced_or_collected,

    -- Disbursement Currency
    disbursement_gross_revenue_this_disbursement_id_or_invoiced,
    disbursement_gross_refund_this_disbursement_id_or_invoiced,
    disbursement_cogs_this_disbursement_id_or_invoiced,
    disbursement_cogs_refund_this_disbursement_id_or_invoiced,
    disbursement_aws_rev_share_this_disbursement_id_or_invoiced,
    disbursement_aws_refund_share_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_listing_fee_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_refund_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced,
    disbursement_seller_tax_share_this_disbursement_id_or_invoiced,
    disbursement_seller_tax_share_refund_this_disbursement_id_or_invoiced,

    disbursement_other_seller_tax_share_this_disbursement_id_or_invoiced,
    disbursement_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

    disbursement_wholesale_aws_tax_share_this_disbursement_id_or_invoiced,
    disbursement_wholesale_aws_tax_share_refund_this_disbursement_id_or_invoiced,

    disbursement_wholesale_seller_tax_share_this_disbursement_id_or_invoiced,
    disbursement_wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced,
    disbursement_wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced,
    disbursement_wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

    disbursement_wholesale_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    disbursement_wholesale_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,
    disbursement_infrastructure_netting_this_disbursement_id_or_invoiced,
    disbursement_balance_adjustment_this_disbursement_id_or_invoiced,
    disbursement_seller_rev_credit_this_disbursement_id_or_invoiced,
    disbursement_aws_ref_fee_credit_this_disbursement_id_or_invoiced,

    disbursement_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_balance_impacting_refund_this_disbursement_id_or_invoiced as disbursement_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,

    disbursement_all_balance_impacting_this_disbursement_id_or_invoiced,

    -- IMPORTANT: For monetary amounts, window functions must partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency because we must NOT ignore the currency:

    -- Invoiced amounts, categorized by transaction type:
    -- Pricing Currency
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then gross_revenue_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as gross_revenue_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then gross_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as gross_refund_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then cogs_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as cogs_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then cogs_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as cogs_refund_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then aws_rev_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_rev_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then aws_refund_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_refund_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then aws_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_tax_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then aws_tax_share_listing_fee_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_tax_share_listing_fee_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then aws_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_tax_share_refund_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_tax_share_refund_listing_fee_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as seller_tax_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as seller_tax_share_refund_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then infrastructure_netting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as infrastructure_netting_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then balance_adjustment_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as balance_adjustment_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then seller_rev_credit_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as seller_rev_credit_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then aws_ref_fee_credit_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_ref_fee_credit_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then other_seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as other_seller_tax_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then other_seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as other_seller_tax_share_refund_invoiced,

    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then wholesale_aws_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_aws_tax_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then wholesale_aws_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_aws_tax_share_refund_invoiced,

    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then wholesale_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_aws_tax_share_balance_impacting_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then wholesale_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_aws_tax_share_refund_balance_impacting_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then wholesale_seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_seller_tax_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_seller_tax_share_refund_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_other_seller_tax_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_other_seller_tax_share_refund_invoiced,

    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_tax_share_balance_impacting_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_tax_share_refund_balance_impacting_invoiced,

    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then all_balance_impacting_this_disbursement_id_or_invoiced_or_collected else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as all_balance_impacting_invoiced,

    sum(case when disbursement_id_or_invoiced_or_collected = '<collected>' then all_balance_impacting_this_disbursement_id_or_invoiced_or_collected else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as all_balance_impacting_collected,

    -- Disbursement Currency
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_gross_revenue_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_gross_revenue_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_gross_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_gross_refund_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_cogs_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_cogs_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_cogs_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_cogs_refund_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_aws_rev_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_rev_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_aws_refund_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_refund_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_aws_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_tax_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_aws_tax_share_listing_fee_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_tax_share_listing_fee_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_aws_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_tax_share_refund_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_tax_share_refund_listing_fee_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_seller_tax_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_seller_tax_share_refund_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_other_seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_other_seller_tax_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_other_seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_other_seller_tax_share_refund_invoiced,

    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_wholesale_aws_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_aws_tax_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_wholesale_aws_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_aws_tax_share_refund_invoiced,

    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_wholesale_seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_seller_tax_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_seller_tax_share_refund_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_other_seller_tax_share_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_other_seller_tax_share_refund_invoiced,

    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_infrastructure_netting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_infrastructure_netting_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_balance_adjustment_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_balance_adjustment_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_seller_rev_credit_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_seller_rev_credit_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_aws_ref_fee_credit_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_ref_fee_credit_invoiced,

    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_wholesale_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_aws_tax_share_balance_impacting_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_wholesale_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_aws_tax_share_refund_balance_impacting_invoiced,

    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_tax_share_balance_impacting_invoiced,
    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_aws_tax_share_balance_impacting_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_tax_share_refund_balance_impacting_invoiced,

    sum(case when disbursement_id_or_invoiced_or_collected = '<invoiced>' then disbursement_all_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_all_balance_impacting_invoiced,

    -- Total disbursed amounts (for all disbursement_id values), categorized by transaction type:
    -- Pricing Currency
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then gross_revenue_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as gross_revenue_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then gross_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as gross_refund_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then cogs_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as cogs_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then cogs_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as cogs_refund_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then aws_rev_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_rev_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then aws_refund_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_refund_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then aws_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_tax_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then aws_tax_share_listing_fee_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_tax_share_listing_fee_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then aws_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_tax_share_refund_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_tax_share_refund_listing_fee_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as seller_tax_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as seller_tax_share_refund_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then infrastructure_netting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as infrastructure_netting_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then balance_adjustment_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as balance_adjustment_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then seller_rev_credit_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as seller_rev_credit_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then aws_ref_fee_credit_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_ref_fee_credit_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then other_seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as other_seller_tax_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then other_seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as other_seller_tax_share_refund_disbursed,

    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then wholesale_aws_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_aws_tax_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then wholesale_aws_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_aws_tax_share_refund_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then wholesale_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_aws_tax_share_balance_impacting_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then wholesale_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_aws_tax_share_refund_balance_impacting_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then wholesale_seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_seller_tax_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_seller_tax_share_refund_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_other_seller_tax_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as wholesale_other_seller_tax_share_refund_disbursed,

    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_tax_share_balance_impacting_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as aws_tax_share_refund_balance_impacting_disbursed,


    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then all_balance_impacting_this_disbursement_id_or_invoiced_or_collected else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as all_balance_impacting_disbursed,

    -- Disbursement Currency
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_gross_revenue_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_gross_revenue_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_gross_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_gross_refund_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_cogs_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_cogs_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_cogs_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_cogs_refund_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_aws_rev_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_rev_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_aws_refund_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_refund_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_aws_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_tax_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_aws_tax_share_listing_fee_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_tax_share_listing_fee_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_aws_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_tax_share_refund_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_tax_share_refund_listing_fee_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_seller_tax_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_seller_tax_share_refund_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_other_seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_other_seller_tax_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_other_seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_other_seller_tax_share_refund_disbursed,

    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_wholesale_aws_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_aws_tax_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_wholesale_aws_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_aws_tax_share_refund_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_wholesale_seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_seller_tax_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_seller_tax_share_refund_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_other_seller_tax_share_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_other_seller_tax_share_refund_disbursed,

    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_infrastructure_netting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_infrastructure_netting_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_balance_adjustment_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_balance_adjustment_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_seller_rev_credit_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_seller_rev_credit_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_aws_ref_fee_credit_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_ref_fee_credit_disbursed,

    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_wholesale_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_aws_tax_share_balance_impacting_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_wholesale_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_wholesale_aws_tax_share_refund_balance_impacting_disbursed,

    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_tax_share_balance_impacting_disbursed,
    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_aws_tax_share_balance_impacting_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_aws_tax_share_refund_balance_impacting_disbursed,

    sum(case when disbursement_id_or_invoiced_or_collected not in ('<invoiced>', '<collected>') then disbursement_all_balance_impacting_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as disbursement_all_balance_impacting_disbursed,

    -- aggregate multiple disbursement

    -- IMPORTANT: For disbursement ID/date and bank trace ID, window functions must partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency because we must NOT ignore the currency:
    max(collection_date) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as last_collection_date,
    max(disbursement_date) over (partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency) as last_disbursement_date,

    case
        -- We need '<invoiced>' and '<collected>' records to have an _older_ date (for sorting purposes):
        -- FAQ: Why can't we use null here?
        --   A null value used on an "order by" will be _greater_ than all other values.
        --   In this case, we need the null value to be _less_ than all other values because
        --   we will be using the "first_value" window function with
        --   "order by disbursement_date_with_older_date_for_invoiced_records desc" to find the record with
        --   the greatest (most recent) disbursement_date.
        when disbursement_id_or_invoiced_or_collected in ('<invoiced>', '<collected>') then cast('1800-01-01' as TIMESTAMP WITH TIME ZONE)
        -- We need disbursed records to have a _newer_ date than '<invoiced>' records (for sorting purposes),
        -- even when the disbursement_date is null.
        --
        -- FAQ: How can the disbursement_date be null on a disbursed record!?
        --   The disbursement_date _should_ never by null, but we have a small number of records where the join in the
        --   upstream billing_event_with_business_flags-transformation.sql fails to find the parent DISBURSED billing
        --   event, resulting in a null disbursement_date.
        else coalesce(disbursement_date, cast('1900-01-01' as TIMESTAMP WITH TIME ZONE))
    end as disbursement_date_with_older_date_for_invoiced_records,

    first_value(disbursement_id) over (
        partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency 
        order by 
            case
                when disbursement_id_or_invoiced_or_collected in ('<invoiced>', '<collected>') then cast('1800-01-01' as TIMESTAMP WITH TIME ZONE)
                else coalesce(disbursement_date, cast('1900-01-01' as TIMESTAMP WITH TIME ZONE))
                end 
            desc rows between unbounded preceding and unbounded following) as last_disbursement_id,
    first_value(bank_trace_id) over (
        partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency 
        order by 
            case
                when disbursement_id_or_invoiced_or_collected in ('<invoiced>', '<collected>') then cast('1800-01-01' as TIMESTAMP WITH TIME ZONE)
                else coalesce(disbursement_date, cast('1900-01-01' as TIMESTAMP WITH TIME ZONE))
                end
            desc rows between unbounded preceding and unbounded following) as last_disburse_bank_trace_id,
    first_value(amazon_reference_id) over (
        partition by line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency 
        order by 
            case
                when disbursement_id_or_invoiced_or_collected in ('<invoiced>', '<collected>') then cast('1800-01-01' as TIMESTAMP WITH TIME ZONE)
                else coalesce(disbursement_date, cast('1900-01-01' as TIMESTAMP WITH TIME ZONE))
                end 
            desc rows between unbounded preceding and unbounded following) as last_disburse_amazon_reference_id,

    -- We use disbursement_dense_rank as a guard on exceeding the listagg 65535 character limit in the next transformation:
    disbursement_dense_rank, -- for debugging:
    disbursement_date_listagg.disbursement_date_or_ellipsis,
    nullif(disbursement_date_listagg.disbursement_date_or_ellipsis, '...') as disbursement_date_list, -- If every disbursement date is null then we want the value to be null not '...'
    bank_trace_id_listagg.disburse_bank_trace_id_or_ellipsis,
    nullif(bank_trace_id_listagg.disburse_bank_trace_id_or_ellipsis, '...') as disburse_bank_trace_id_list,
    amazon_reference_id_listagg.disburse_amazon_reference_id_or_ellipsis,
    nullif(amazon_reference_id_listagg.disburse_amazon_reference_id_or_ellipsis, '...') as disburse_amazon_reference_id_list,

    line_item.seller_issued_invoice_id,      -- For deemed VAT (and future) where AWS is the recipient
    line_item.seller_issued_invoice_variant, -- For deemed VAT (and future) where AWS is the recipient

    line_item.min_data_catalog as min_data_catalog,
    line_item.max_data_catalog as max_data_catalog,
    data_catalog

from line_items_with_disbursement_dense_rank as line_item
left join disbursement_date_listagg_columns disbursement_date_listagg
    on line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency = disbursement_date_listagg.internal_buyer_invoice_line_item_surrogate_id_with_currency
left join bank_trace_id_listagg_columns bank_trace_id_listagg
    on line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency = bank_trace_id_listagg.internal_buyer_invoice_line_item_surrogate_id_with_currency
left join amazon_reference_id_listagg_columns amazon_reference_id_listagg
    on line_item.internal_buyer_invoice_line_item_surrogate_id_with_currency = amazon_reference_id_listagg.internal_buyer_invoice_line_item_surrogate_id_with_currency
),

-- ============================================================================
-- Layer 5: Enrichment & Final Output
-- ============================================================================

-- Join line items to offers, products, accounts, addresses at invoice time.
-- Resolve reseller details for CPPO offers.
-- Filter out collected-only rows (keep invoiced and disbursed).
line_items_with_window_functions_enrich_offer_product_address as (
select
    internal_buyer_invoice_line_item_surrogate_id_with_currency,
    count_distinct_pricing_currency,
    count_distinct_disbursement_currency,
    internal_buyer_invoice_line_item_surrogate_id,
    report_transaction_reference_id,
    disbursement_id,
    disbursement_id_or_invoiced_or_collected as disbursement_id_or_invoiced,
    line.recipient_account_id,
    agreement.agreement_valid_from,
    coalesce(legacy_product.legacy_id, line.product_id) as product_id,
    legacy_product.legacy_id as legacy_product_id,
    products.title as product_title,
    line.broker_id,
    line.currency,
    line.disbursement_currency,
    line.end_user_account_id,
    cast(null as varchar) as end_user_encrypted_account_id,
    acc_enduser.aws_account_id as end_user_aws_account_id,
    acc_payer.aws_account_id as payer_aws_account_id,
    acc_payer.encrypted_account_id payer_encrypted_account_id,
    line.agreement_id,
    line.wholesale_agreement_id,
    agreement.agreement_revision,
    agreement.status,
    agreement.estimated_charges_currency_code,
    agreement.estimated_charges_net_amount,
    line.proposer_account_id,
    case when offer.offer_id like 'aiqoffer-%' then null else agreement.start_date end as Agreement_Start_Date,
    case when offer.offer_id like 'aiqoffer-%' then null else agreement.end_date end as Agreement_End_Date,
    case when offer.offer_id like 'aiqoffer-%' then null else agreement.acceptance_date end as Agreement_Acceptance_Date,
    case when offer.offer_id like 'aiqoffer-%' then null else agreement.valid_from end as agreement_updated_date,
    case when offer.offer_id like 'aiqoffer-%' then null else line.usage_period_start_date end as Usage_Period_Start_Date,
    case when offer.offer_id like 'aiqoffer-%' then null else line.usage_period_end_date end as Usage_Period_End_Date,

    line.acceptor_account_id,
    acc_subscriber.aws_account_id as subscriber_aws_account_id,
    acc_subscriber.encrypted_account_id as subscriber_encrypted_account_id,
    offer.offer_id,
    agreement.offer_set_id,
    offer.offer_target_with_private as offer_target,
    offer.name offer_name,
    offer.opportunity_name offer_opportunity_name,
    offer.opportunity_description offer_opportunity_description,
    offer.opportunity_id,
    payment_due_date,
    line.bank_trace_id,
    amazon_reference_id,
    disbursement_date,
    disbursement_status,
    billing_address_id,
    purchase_invoice_id,
    listing_fee_invoice_id,
    resale_invoice_id,
    purchase_invoice_date,
    -- Only used in downstream line_item_invoiced_with_eur_amounts_for_tax_reporting for french tax reporting
    DATE_TRUNC('day', at_timezone(purchase_invoice_date, 'CET')) purchase_invoice_date_cet,
    listing_fee_invoice_date,
    is_isv_wholesale_invoice,

    -- Pricing Currency
    gross_revenue_this_disbursement_id_or_invoiced,
    gross_refund_this_disbursement_id_or_invoiced,
    cogs_this_disbursement_id_or_invoiced,
    cogs_refund_this_disbursement_id_or_invoiced,
    aws_rev_share_this_disbursement_id_or_invoiced,
    aws_refund_share_this_disbursement_id_or_invoiced,
    aws_tax_share_this_disbursement_id_or_invoiced,
    aws_tax_share_listing_fee_this_disbursement_id_or_invoiced,
    aws_tax_share_refund_this_disbursement_id_or_invoiced,
    aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced,
    seller_tax_share_this_disbursement_id_or_invoiced,
    seller_tax_share_refund_this_disbursement_id_or_invoiced,
    infrastructure_netting_this_disbursement_id_or_invoiced,
    balance_adjustment_this_disbursement_id_or_invoiced,
    seller_rev_credit_this_disbursement_id_or_invoiced,
    aws_ref_fee_credit_this_disbursement_id_or_invoiced,
    other_seller_tax_share_this_disbursement_id_or_invoiced,
    other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

    wholesale_aws_tax_share_this_disbursement_id_or_invoiced,
    wholesale_aws_tax_share_refund_this_disbursement_id_or_invoiced,
    wholesale_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    wholesale_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,
    wholesale_seller_tax_share_this_disbursement_id_or_invoiced,
    wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced,
    wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced,
    wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

    aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,

    -- To be replaced with
    -- all_balance_impacting_this_disbursement_id_or_invoiced_or_collected as all_balance_impacting_this_disbursement_id_or_invoiced,
    all_balance_impacting_this_disbursement_id_or_invoiced,

    -- Disbursement currency
    disbursement_gross_revenue_this_disbursement_id_or_invoiced,
    disbursement_gross_refund_this_disbursement_id_or_invoiced,
    disbursement_cogs_this_disbursement_id_or_invoiced,
    disbursement_cogs_refund_this_disbursement_id_or_invoiced,
    disbursement_aws_rev_share_this_disbursement_id_or_invoiced,
    disbursement_aws_refund_share_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_listing_fee_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_refund_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced,
    disbursement_seller_tax_share_this_disbursement_id_or_invoiced,
    disbursement_seller_tax_share_refund_this_disbursement_id_or_invoiced,

    disbursement_other_seller_tax_share_this_disbursement_id_or_invoiced,
    disbursement_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

    disbursement_wholesale_aws_tax_share_this_disbursement_id_or_invoiced,
    disbursement_wholesale_aws_tax_share_refund_this_disbursement_id_or_invoiced,
    disbursement_wholesale_seller_tax_share_this_disbursement_id_or_invoiced,
    disbursement_wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced,
    disbursement_wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced,
    disbursement_wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,


    disbursement_infrastructure_netting_this_disbursement_id_or_invoiced,
    disbursement_balance_adjustment_this_disbursement_id_or_invoiced,
    disbursement_seller_rev_credit_this_disbursement_id_or_invoiced,
    disbursement_aws_ref_fee_credit_this_disbursement_id_or_invoiced,

    disbursement_wholesale_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    disbursement_wholesale_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,

    disbursement_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,

    disbursement_all_balance_impacting_this_disbursement_id_or_invoiced,

    -- Pricing Currency
    gross_revenue_invoiced,
    gross_refund_invoiced,
    cogs_invoiced,
    cogs_refund_invoiced,
    aws_rev_share_invoiced,
    aws_refund_share_invoiced,
    aws_tax_share_invoiced,
    aws_tax_share_listing_fee_invoiced,
    aws_tax_share_refund_invoiced,
    aws_tax_share_refund_listing_fee_invoiced,
    seller_tax_share_invoiced,
    seller_tax_share_refund_invoiced,
    infrastructure_netting_invoiced,
    balance_adjustment_invoiced,
    seller_rev_credit_invoiced,
    aws_ref_fee_credit_invoiced,
    other_seller_tax_share_invoiced,
    other_seller_tax_share_refund_invoiced,

    wholesale_aws_tax_share_invoiced,
    wholesale_aws_tax_share_refund_invoiced,
    wholesale_aws_tax_share_balance_impacting_invoiced,
    wholesale_aws_tax_share_refund_balance_impacting_invoiced,
    wholesale_seller_tax_share_invoiced,
    wholesale_seller_tax_share_refund_invoiced,
    wholesale_other_seller_tax_share_invoiced,
    wholesale_other_seller_tax_share_refund_invoiced,

    aws_tax_share_balance_impacting_invoiced,
    aws_tax_share_refund_balance_impacting_invoiced,

    all_balance_impacting_invoiced,
    all_balance_impacting_collected,

    -- Disbursement Currency
    disbursement_gross_revenue_invoiced,
    disbursement_gross_refund_invoiced,
    disbursement_cogs_invoiced,
    disbursement_cogs_refund_invoiced,
    disbursement_aws_rev_share_invoiced,
    disbursement_aws_refund_share_invoiced,
    disbursement_aws_tax_share_invoiced,
    disbursement_aws_tax_share_listing_fee_invoiced,
    disbursement_aws_tax_share_refund_invoiced,
    disbursement_aws_tax_share_refund_listing_fee_invoiced,
    disbursement_seller_tax_share_invoiced,
    disbursement_seller_tax_share_refund_invoiced,
    disbursement_other_seller_tax_share_invoiced,
    disbursement_other_seller_tax_share_refund_invoiced,

    disbursement_wholesale_aws_tax_share_balance_impacting_invoiced,
    disbursement_wholesale_aws_tax_share_refund_balance_impacting_invoiced,
    disbursement_wholesale_seller_tax_share_invoiced,
    disbursement_wholesale_seller_tax_share_refund_invoiced,
    disbursement_wholesale_other_seller_tax_share_invoiced,
    disbursement_wholesale_other_seller_tax_share_refund_invoiced,

    disbursement_infrastructure_netting_invoiced,
    disbursement_balance_adjustment_invoiced,
    disbursement_seller_rev_credit_invoiced,
    disbursement_aws_ref_fee_credit_invoiced,

    disbursement_wholesale_aws_tax_share_invoiced,
    disbursement_wholesale_aws_tax_share_refund_invoiced,

    disbursement_aws_tax_share_balance_impacting_invoiced,
    disbursement_aws_tax_share_refund_balance_impacting_invoiced,

    disbursement_all_balance_impacting_invoiced,

    -- Pricing Currency
    gross_revenue_disbursed,
    gross_refund_disbursed,
    cogs_disbursed,
    cogs_refund_disbursed,
    aws_rev_share_disbursed,
    aws_refund_share_disbursed,
    aws_tax_share_disbursed,
    aws_tax_share_listing_fee_disbursed,
    aws_tax_share_refund_disbursed,
    aws_tax_share_refund_listing_fee_disbursed,
    seller_tax_share_disbursed,
    seller_tax_share_refund_disbursed,
    infrastructure_netting_disbursed,
    balance_adjustment_disbursed,
    seller_rev_credit_disbursed,
    aws_ref_fee_credit_disbursed,
    other_seller_tax_share_disbursed,
    other_seller_tax_share_refund_disbursed,

    wholesale_aws_tax_share_disbursed,
    wholesale_aws_tax_share_refund_disbursed,
    wholesale_aws_tax_share_balance_impacting_disbursed,
    wholesale_aws_tax_share_refund_balance_impacting_disbursed,
    wholesale_seller_tax_share_disbursed,
    wholesale_seller_tax_share_refund_disbursed,
    wholesale_other_seller_tax_share_disbursed,
    wholesale_other_seller_tax_share_refund_disbursed,

    aws_tax_share_balance_impacting_disbursed,
    aws_tax_share_refund_balance_impacting_disbursed,

    all_balance_impacting_disbursed,

    --Disbursement Currency
    disbursement_gross_revenue_disbursed,
    disbursement_gross_refund_disbursed,
    disbursement_cogs_disbursed,
    disbursement_cogs_refund_disbursed,
    disbursement_aws_rev_share_disbursed,
    disbursement_aws_refund_share_disbursed,
    disbursement_aws_tax_share_disbursed,
    disbursement_aws_tax_share_listing_fee_disbursed,
    disbursement_aws_tax_share_refund_disbursed,
    disbursement_aws_tax_share_refund_listing_fee_disbursed,
    disbursement_seller_tax_share_disbursed,
    disbursement_seller_tax_share_refund_disbursed,
    disbursement_other_seller_tax_share_disbursed,
    disbursement_other_seller_tax_share_refund_disbursed,

    disbursement_wholesale_aws_tax_share_balance_impacting_disbursed,
    disbursement_wholesale_aws_tax_share_refund_balance_impacting_disbursed,
    disbursement_wholesale_seller_tax_share_disbursed,
    disbursement_wholesale_seller_tax_share_refund_disbursed,
    disbursement_wholesale_other_seller_tax_share_disbursed,
    disbursement_wholesale_other_seller_tax_share_refund_disbursed,

    disbursement_infrastructure_netting_disbursed,
    disbursement_balance_adjustment_disbursed,
    disbursement_seller_rev_credit_disbursed,
    disbursement_aws_ref_fee_credit_disbursed,

    disbursement_wholesale_aws_tax_share_disbursed,
    disbursement_wholesale_aws_tax_share_refund_disbursed,

    disbursement_aws_tax_share_balance_impacting_disbursed,
    disbursement_aws_tax_share_refund_balance_impacting_disbursed,

    disbursement_all_balance_impacting_disbursed,

    last_collection_date,

    last_disbursement_date,
    case when last_disbursement_id = '' then null else last_disbursement_id end as last_disbursement_id,
    last_disburse_bank_trace_id,
    last_disburse_amazon_reference_id,
    disbursement_date_list,
    disburse_bank_trace_id_list,
    disburse_amazon_reference_id_list,
    products.product_code,
    acc_products.aws_account_id as manufacturer_aws_account_id,
    --TODO: Get manufacturer_encrypted_account_id from products
    cast(null as varchar) as manufacturer_encrypted_account_id, -- DEPRECATED
    products.manufacturer_account_id,

    acc_subscriber.tax_legal_name as subscriber_tax_legal_name,
    acc_payer.tax_legal_name as payer_tax_legal_name,
    acc_enduser.tax_legal_name as end_user_tax_legal_name,

    acc_subscriber.tax_address_id as subscriber_tax_address_id,
    acc_subscriber.mailing_address_id as subscriber_mailing_address_id,
    acc_payer.tax_address_id as payer_tax_address_id,
    acc_payer.mailing_address_id as payer_mailing_address_id,
    acc_enduser.tax_address_id as end_user_tax_address_id,
    acc_enduser.mailing_address_id as end_user_mailing_address_id,

    coalesce(acc_subscriber.tax_address_id, acc_subscriber.mailing_address_id) as subscriber_address_id,                    -- DEPRECATED!
    coalesce(acc_payer.tax_address_id, line.billing_address_id, acc_payer.mailing_address_id) as payer_address_id,          -- DEPRECATED!
    coalesce(acc_enduser.tax_address_id, line.billing_address_id, acc_enduser.mailing_address_id) as end_user_address_id, -- DEPRECATED!

    line.seller_issued_invoice_id,      -- For deemed VAT (and future) where AWS is the recipient
    line.seller_issued_invoice_variant, -- For deemed VAT (and future) where AWS is the recipient

    least(line.min_data_catalog, agreement.min_data_catalog, offer.min_data_catalog, products.min_data_catalog, legacy_product.min_data_catalog, acc_payer.min_data_catalog, acc_enduser.min_data_catalog, acc_subscriber.min_data_catalog) as min_data_catalog,
    greatest(line.max_data_catalog, agreement.max_data_catalog, offer.max_data_catalog, products.max_data_catalog, legacy_product.max_data_catalog, acc_payer.max_data_catalog, acc_enduser.max_data_catalog, acc_subscriber.max_data_catalog) as max_data_catalog,
    line.data_catalog

from
    line_items_with_window_functions as line
    left join agreements_revisions_with_history as agreement on
        (line.agreement_id = agreement.agreement_id and line.purchase_invoice_date >= agreement.valid_from_adjusted and line.purchase_invoice_date < agreement.valid_to )
    left join offers_with_history_with_target_type as offer on
        (line.offer_id = offer.offer_id and line.purchase_invoice_date >= offer.valid_from and line.purchase_invoice_date < offer.valid_to)
    left join products_with_history as products on
        (line.product_id = products.product_id and line.purchase_invoice_date >= products.valid_from_adjusted and line.purchase_invoice_date < products.valid_to )
    left join legacy_products as legacy_product on
        (line.product_id = legacy_product.new_id)
    left join accounts_with_history_with_company_name as acc_payer on
        (line.payer_account_id = acc_payer.account_id and line.purchase_invoice_date >= acc_payer.valid_from and line.purchase_invoice_date < acc_payer.valid_to)
    left join accounts_with_history_with_company_name as acc_enduser on
        (line.end_user_account_id = acc_enduser.account_id and line.purchase_invoice_date >= acc_enduser.valid_from and line.purchase_invoice_date < acc_enduser.valid_to)
    left join accounts_with_history_with_company_name as acc_subscriber on
        (line.acceptor_account_id = acc_subscriber.account_id and line.purchase_invoice_date >= acc_subscriber.valid_from and line.purchase_invoice_date < acc_subscriber.valid_to)
    left join accounts_with_history_with_company_name as acc_products on
        (products.manufacturer_account_id = acc_products.account_id and line.purchase_invoice_date >= acc_products.valid_from and line.purchase_invoice_date < acc_products.valid_to)
where
    disbursement_id_or_invoiced_or_collected <> '<collected>'
),

-- ============================================================================
-- CTE: line_items_with_window_functions_enrich_offer_product_address_name_disbursed
-- Joins to address tables for payer/enduser/subscriber address details,
-- resolves company names via COALESCE, computes seller_net_revenue and
-- disbursed_net_revenue in both pricing and disbursement currencies.
-- ============================================================================


line_items_with_window_functions_enrich_offer_product_address_name_disbursed as (
-- Athena does not recognize calculation in the same query, adding steps here to do those calculations
select
        line.internal_buyer_invoice_line_item_surrogate_id_with_currency,
        line.count_distinct_pricing_currency,
        line.count_distinct_disbursement_currency,
        line.internal_buyer_invoice_line_item_surrogate_id,
        report_transaction_reference_id,
        disbursement_id,
        disbursement_id_or_invoiced,
        line.recipient_account_id,
        line.agreement_valid_from,
        product_id,
        legacy_product_id,
        product_title,
        broker_id,
        currency,
        line.disbursement_currency,

        line.subscriber_tax_legal_name,
        line.payer_tax_legal_name,
        line.end_user_tax_legal_name,
        line.subscriber_tax_address_id,
        line.subscriber_mailing_address_id,
        line.payer_tax_address_id,
        line.payer_mailing_address_id,
        line.end_user_tax_address_id,
        line.end_user_mailing_address_id,

        end_user_account_id,
        end_user_encrypted_account_id,
        end_user_aws_account_id,

        end_user_address_id,
        -- For company name, we take it from the tax/legal company name or the address record with a non-null value:
        coalesce(
            line.end_user_tax_legal_name,
            end_user_tax_address.company_name,
            billing_address.company_name,
            end_user_mailing_address.company_name
        ) as end_user_company_name,
        add_enduser.email_domain as end_user_email_domain,
        add_enduser.city as end_user_city,
        add_enduser.state_or_region as end_user_state_or_region,
        add_enduser.country_code as end_user_country,
        add_enduser.postal_code as end_user_postal_code,

        payer_aws_account_id,
        payer_encrypted_account_id,

        payer_address_id,
        -- For company name, we take it from the tax/legal company name or the address record with a non-null value:
        coalesce(
            line.payer_tax_legal_name,
            payer_tax_address.company_name,
            billing_address.company_name,
            payer_mailing_address.company_name
        ) as payer_company_name,
        add_payer.email_domain as payer_email_domain,
        add_payer.city as payer_city,
        add_payer.state_or_region as payer_state,
        add_payer.country_code as payer_country,
        add_payer.postal_code as payer_postal_code,

        agreement_id,
        wholesale_agreement_id,
        agreement_revision,
        agreement_start_date,
        agreement_end_date,
        agreement_acceptance_date,
        agreement_updated_date,
        line.status,
        line.estimated_charges_currency_code,
        line.estimated_charges_net_amount,

        -- TODO: We wrongly assume that we are reporting to the manufacturer when proposer_account_id <> recipient_account_id.
        -- The proposer_account_id <> recipient_account_id expression is true if the recipient of the data is NOT the legal seller of record,
        -- in which case the recipient may be the manufacturer (or in the future, the distributor, subsidiary, etc).
        case when proposer_account_id = recipient_account_id then null else acc_proposer.aws_account_id end as reseller_aws_account_id,
        case when proposer_account_id = recipient_account_id then null else acc_proposer.encrypted_account_id end as reseller_encrypted_account_id,
        case when proposer_account_id = recipient_account_id then null else acc_proposer.mailing_company_name end as reseller_company_name,

        usage_period_start_date,
        usage_period_end_date,
        proposer_account_id,
        acc_proposer.aws_account_id as proposer_aws_account_id,
        acceptor_account_id,
        subscriber_aws_account_id,
        subscriber_encrypted_account_id,

        subscriber_address_id,
        -- For company name, we take it from the tax/legal company name or the address record with a non-null value:
        coalesce(
            line.subscriber_tax_legal_name,
            subscriber_tax_address.company_name,
            subscriber_mailing_address.company_name
        ) as subscriber_company_name,
        add_subscriber.email_domain as subscriber_email_domain,
        add_subscriber.city as subscriber_city,
        add_subscriber.state_or_region as subscriber_state_or_region,
        add_subscriber.country_code as subscriber_country,
        add_subscriber.postal_code as subscriber_postal_code,
        
        offer_id,
        offer_set_id,
        offer_target,
        offer_name,
        offer_opportunity_name,
        offer_opportunity_description,
        opportunity_id,
        payment_due_date,
        bank_trace_id,
        amazon_reference_id,
        disbursement_date,
        disbursement_status,
        billing_address_id,

        max(purchase_invoice_id) as purchase_invoice_id,         
        max(listing_fee_invoice_id) as listing_fee_invoice_id,       
        max(resale_invoice_id) as resale_invoice_id, 
        max(purchase_invoice_date) as purchase_invoice_date,     
        max(listing_fee_invoice_date) as listing_fee_invoice_date,  
        is_isv_wholesale_invoice,

        -- Pricing Currency
        gross_revenue_this_disbursement_id_or_invoiced,
        gross_refund_this_disbursement_id_or_invoiced,
        cogs_this_disbursement_id_or_invoiced,
        cogs_refund_this_disbursement_id_or_invoiced,
        aws_rev_share_this_disbursement_id_or_invoiced,
        aws_refund_share_this_disbursement_id_or_invoiced,
        aws_tax_share_this_disbursement_id_or_invoiced,
        aws_tax_share_listing_fee_this_disbursement_id_or_invoiced,
        aws_tax_share_refund_this_disbursement_id_or_invoiced,
        aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced,
        seller_tax_share_this_disbursement_id_or_invoiced,
        seller_tax_share_refund_this_disbursement_id_or_invoiced,
        infrastructure_netting_this_disbursement_id_or_invoiced,
        balance_adjustment_this_disbursement_id_or_invoiced,
        seller_rev_credit_this_disbursement_id_or_invoiced,
        aws_ref_fee_credit_this_disbursement_id_or_invoiced,
        other_seller_tax_share_this_disbursement_id_or_invoiced,
        other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

        wholesale_aws_tax_share_this_disbursement_id_or_invoiced,
        wholesale_aws_tax_share_refund_this_disbursement_id_or_invoiced,
        wholesale_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
        wholesale_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,
        wholesale_seller_tax_share_this_disbursement_id_or_invoiced,
        wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced,
        wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced,
        wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

        aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
        aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,

        all_balance_impacting_this_disbursement_id_or_invoiced,

        -- Disbursement Currency
        disbursement_gross_revenue_this_disbursement_id_or_invoiced,
        disbursement_gross_refund_this_disbursement_id_or_invoiced,
        disbursement_cogs_this_disbursement_id_or_invoiced,
        disbursement_cogs_refund_this_disbursement_id_or_invoiced,
        disbursement_aws_rev_share_this_disbursement_id_or_invoiced,
        disbursement_aws_refund_share_this_disbursement_id_or_invoiced,
        disbursement_aws_tax_share_this_disbursement_id_or_invoiced,
        disbursement_aws_tax_share_listing_fee_this_disbursement_id_or_invoiced,
        disbursement_aws_tax_share_refund_this_disbursement_id_or_invoiced,
        disbursement_aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced,
        disbursement_seller_tax_share_this_disbursement_id_or_invoiced,
        disbursement_seller_tax_share_refund_this_disbursement_id_or_invoiced,

        disbursement_other_seller_tax_share_this_disbursement_id_or_invoiced,
        disbursement_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

        disbursement_wholesale_aws_tax_share_this_disbursement_id_or_invoiced,
        disbursement_wholesale_aws_tax_share_refund_this_disbursement_id_or_invoiced,
        disbursement_wholesale_seller_tax_share_this_disbursement_id_or_invoiced,
        disbursement_wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced,
        disbursement_wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced,
        disbursement_wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

        disbursement_infrastructure_netting_this_disbursement_id_or_invoiced,
        disbursement_balance_adjustment_this_disbursement_id_or_invoiced,
        disbursement_seller_rev_credit_this_disbursement_id_or_invoiced,
        disbursement_aws_ref_fee_credit_this_disbursement_id_or_invoiced,

        disbursement_wholesale_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
        disbursement_wholesale_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,

        disbursement_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
        disbursement_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,

        disbursement_all_balance_impacting_this_disbursement_id_or_invoiced,

        -- Pricing Currency
        gross_revenue_invoiced,
        gross_refund_invoiced,
        cogs_invoiced,
        cogs_refund_invoiced,
        aws_rev_share_invoiced,
        aws_refund_share_invoiced,
        aws_tax_share_invoiced,
        aws_tax_share_listing_fee_invoiced,
        aws_tax_share_refund_invoiced,
        aws_tax_share_refund_listing_fee_invoiced,
        seller_tax_share_invoiced,
        seller_tax_share_refund_invoiced,
        infrastructure_netting_invoiced,
        balance_adjustment_invoiced,
        seller_rev_credit_invoiced,
        aws_ref_fee_credit_invoiced,
        other_seller_tax_share_invoiced,
        other_seller_tax_share_refund_invoiced,

        wholesale_aws_tax_share_invoiced,
        wholesale_aws_tax_share_refund_invoiced,
        wholesale_aws_tax_share_balance_impacting_invoiced,
        wholesale_aws_tax_share_refund_balance_impacting_invoiced,
        wholesale_seller_tax_share_invoiced,
        wholesale_seller_tax_share_refund_invoiced,
        wholesale_other_seller_tax_share_invoiced,
        wholesale_other_seller_tax_share_refund_invoiced,

        aws_tax_share_balance_impacting_invoiced,
        aws_tax_share_refund_balance_impacting_invoiced,

        all_balance_impacting_invoiced,
        all_balance_impacting_collected,

        -- Disbursement Currency
        disbursement_gross_revenue_invoiced,
        disbursement_gross_refund_invoiced,
        disbursement_cogs_invoiced,
        disbursement_cogs_refund_invoiced,
        disbursement_aws_rev_share_invoiced,
        disbursement_aws_refund_share_invoiced,
        disbursement_aws_tax_share_invoiced,
        disbursement_aws_tax_share_listing_fee_invoiced,
        disbursement_aws_tax_share_refund_invoiced,
        disbursement_aws_tax_share_refund_listing_fee_invoiced,
        disbursement_seller_tax_share_invoiced,
        disbursement_seller_tax_share_refund_invoiced,
        disbursement_other_seller_tax_share_invoiced,
        disbursement_other_seller_tax_share_refund_invoiced,

        disbursement_wholesale_aws_tax_share_balance_impacting_invoiced,
        disbursement_wholesale_aws_tax_share_refund_balance_impacting_invoiced,
        disbursement_wholesale_seller_tax_share_invoiced,
        disbursement_wholesale_seller_tax_share_refund_invoiced,
        disbursement_wholesale_other_seller_tax_share_invoiced,
        disbursement_wholesale_other_seller_tax_share_refund_invoiced,

        disbursement_infrastructure_netting_invoiced,
        disbursement_balance_adjustment_invoiced,
        disbursement_seller_rev_credit_invoiced,
        disbursement_aws_ref_fee_credit_invoiced,

        disbursement_wholesale_aws_tax_share_invoiced,
        disbursement_wholesale_aws_tax_share_refund_invoiced,

        disbursement_aws_tax_share_balance_impacting_invoiced,
        disbursement_aws_tax_share_refund_balance_impacting_invoiced,

        disbursement_all_balance_impacting_invoiced,

        -- Pricing Currency
        gross_revenue_disbursed,
        gross_refund_disbursed,
        cogs_disbursed,
        cogs_refund_disbursed,
        aws_rev_share_disbursed,
        aws_refund_share_disbursed,
        aws_tax_share_disbursed,
        aws_tax_share_listing_fee_disbursed,
        aws_tax_share_refund_disbursed,
        aws_tax_share_refund_listing_fee_disbursed,
        seller_tax_share_disbursed,
        seller_tax_share_refund_disbursed,
        infrastructure_netting_disbursed,
        balance_adjustment_disbursed,
        seller_rev_credit_disbursed,
        aws_ref_fee_credit_disbursed,
        other_seller_tax_share_disbursed,
        other_seller_tax_share_refund_disbursed,

        wholesale_aws_tax_share_disbursed,
        wholesale_aws_tax_share_refund_disbursed,
        wholesale_aws_tax_share_balance_impacting_disbursed,
        wholesale_aws_Tax_share_refund_balance_impacting_disbursed,
        wholesale_seller_tax_share_disbursed,
        wholesale_seller_tax_share_refund_disbursed,
        wholesale_other_seller_tax_share_disbursed,
        wholesale_other_seller_tax_share_refund_disbursed,

        aws_tax_share_balance_impacting_disbursed,
        aws_tax_share_refund_balance_impacting_disbursed,

        all_balance_impacting_disbursed,

        -- Disbursement Currency
        disbursement_gross_revenue_disbursed,
        disbursement_gross_refund_disbursed,
        disbursement_cogs_disbursed,
        disbursement_cogs_refund_disbursed,
        disbursement_aws_rev_share_disbursed,
        disbursement_aws_refund_share_disbursed,
        disbursement_aws_tax_share_disbursed,
        disbursement_aws_tax_share_listing_fee_disbursed,
        disbursement_aws_tax_share_refund_disbursed,
        disbursement_aws_tax_share_refund_listing_fee_disbursed,
        disbursement_seller_tax_share_disbursed,
        disbursement_seller_tax_share_refund_disbursed,
        disbursement_other_seller_tax_share_disbursed,
        disbursement_other_seller_tax_share_refund_disbursed,

        disbursement_wholesale_aws_tax_share_balance_impacting_disbursed,
        disbursement_wholesale_aws_tax_share_refund_balance_impacting_disbursed,
        disbursement_wholesale_seller_tax_share_disbursed,
        disbursement_wholesale_seller_tax_share_refund_disbursed,
        disbursement_wholesale_other_seller_tax_share_disbursed,
        disbursement_wholesale_other_seller_tax_share_refund_disbursed,

        disbursement_infrastructure_netting_disbursed,
        disbursement_balance_adjustment_disbursed,
        disbursement_seller_rev_credit_disbursed,
        disbursement_aws_ref_fee_credit_disbursed,

        disbursement_wholesale_aws_tax_share_disbursed,
        disbursement_wholesale_aws_tax_share_refund_disbursed,

        disbursement_aws_tax_share_balance_impacting_disbursed,
        disbursement_aws_tax_share_refund_balance_impacting_disbursed,

        disbursement_all_balance_impacting_disbursed,

        -- The sign on the seller_net_revenue_this_disbursement_id_or_invoiced column indicates the balance due to the seller.
        -- A POSITIVE value indicates that net money is owned TO the seller.
        -- A NEGATIVE value indicates that money is owned BY the seller.
        --
        -- When disbursement_id_or_invoiced = '<invoiced>':
        -- 1. this will normally be positive to indicate the net amount that is due to the seller
        -- 2. this will be negative for a refund of an amount that was previously due to the seller
        --
        -- When disbursement_id_or_invoiced refers to a disbursement billing event ID:
        -- 1. this will normally be negative for a disbursement of a net amount that was previously due to the seller
        -- 2. this will be positive for a disbursement of a refund of an amount that was previously due to the seller
        -- Pricing Currency
        (
            gross_revenue_this_disbursement_id_or_invoiced
            + gross_refund_this_disbursement_id_or_invoiced
            + aws_rev_share_this_disbursement_id_or_invoiced
            + aws_refund_share_this_disbursement_id_or_invoiced
            + seller_tax_share_this_disbursement_id_or_invoiced
            + seller_tax_share_refund_this_disbursement_id_or_invoiced
            + balance_adjustment_this_disbursement_id_or_invoiced
            + coalesce(infrastructure_netting_this_disbursement_id_or_invoiced, 0.0)
            + cogs_this_disbursement_id_or_invoiced
            + cogs_refund_this_disbursement_id_or_invoiced
            + aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced
            + aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced
            + other_seller_tax_share_this_disbursement_id_or_invoiced
            + other_seller_tax_share_refund_this_disbursement_id_or_invoiced
            + wholesale_seller_tax_share_this_disbursement_id_or_invoiced
            + wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced
            + wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced
            + wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced
        ) as seller_net_revenue_this_disbursement_id_or_invoiced,
        -- Disbursement Currency
        (
            disbursement_gross_revenue_this_disbursement_id_or_invoiced
                + disbursement_gross_refund_this_disbursement_id_or_invoiced
                + disbursement_aws_rev_share_this_disbursement_id_or_invoiced
                + disbursement_aws_refund_share_this_disbursement_id_or_invoiced
                + disbursement_seller_tax_share_this_disbursement_id_or_invoiced
                + disbursement_seller_tax_share_refund_this_disbursement_id_or_invoiced
                + disbursement_balance_adjustment_this_disbursement_id_or_invoiced
                + coalesce(disbursement_infrastructure_netting_this_disbursement_id_or_invoiced, 0.0)
                + disbursement_cogs_this_disbursement_id_or_invoiced
                + disbursement_cogs_refund_this_disbursement_id_or_invoiced
                + disbursement_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced
                + disbursement_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced
                + disbursement_other_seller_tax_share_this_disbursement_id_or_invoiced
                + disbursement_other_seller_tax_share_refund_this_disbursement_id_or_invoiced
                + disbursement_wholesale_seller_tax_share_this_disbursement_id_or_invoiced
                + disbursement_wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced
                + disbursement_wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced
                + disbursement_wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced
            ) as disbursement_seller_net_revenue_this_disbursement_id_or_invoiced,

        -- The sign on the seller_net_revenue column indicates the balance that was (or is) due to the seller,
        -- at the point when it was invoiced.
        --
        -- A POSITIVE seller_net_revenue (common case) indicates that money was (or is) owned TO the seller.
        -- A NEGATIVE seller_net_revenue (refund, etc) that money was (or is) owned BY the seller.
        --
        -- This column ignores the disbursement_id_or_invoiced values. Regardless of whether it is
        -- '<invoiced>' or refers to a disbursement billing event ID, this column refers to the balance
        -- that was (or is) due to the seller, at the point when it was invoiced. At the point of invoicing,
        -- the net amount owed to the seller is usually POSITIVE, unless it is a refund.
        -- Pricing Currency
        (
            gross_revenue_invoiced
            + gross_refund_invoiced
            + aws_rev_share_invoiced
            + aws_refund_share_invoiced
            + seller_tax_share_invoiced
            + seller_tax_share_refund_invoiced
            + balance_adjustment_invoiced
            + coalesce(infrastructure_netting_invoiced, 0.0)
            + cogs_invoiced
            + cogs_refund_invoiced
            + aws_tax_share_balance_impacting_invoiced
            + aws_tax_share_refund_balance_impacting_invoiced
            + other_seller_tax_share_invoiced
            + other_seller_tax_share_refund_invoiced
            + wholesale_seller_tax_share_invoiced
            + wholesale_seller_tax_share_refund_invoiced
            + wholesale_other_seller_tax_share_invoiced
            + wholesale_other_seller_tax_share_refund_invoiced
        ) as seller_net_revenue,
        -- Disbursement Currency
        (
            disbursement_gross_revenue_invoiced
                + disbursement_gross_refund_invoiced
                + disbursement_aws_rev_share_invoiced
                + disbursement_aws_refund_share_invoiced
                + disbursement_seller_tax_share_invoiced
                + disbursement_seller_tax_share_refund_invoiced
                + disbursement_balance_adjustment_invoiced
                + coalesce(disbursement_infrastructure_netting_invoiced, 0.0)
                + disbursement_cogs_invoiced
                + disbursement_cogs_refund_invoiced
                + disbursement_aws_tax_share_balance_impacting_invoiced
                + disbursement_aws_tax_share_refund_balance_impacting_invoiced
                + disbursement_other_seller_tax_share_invoiced
                + disbursement_other_seller_tax_share_refund_invoiced
                + disbursement_wholesale_seller_tax_share_invoiced
                + disbursement_wholesale_seller_tax_share_refund_invoiced
                + disbursement_wholesale_other_seller_tax_share_invoiced
                + disbursement_wholesale_other_seller_tax_share_refund_invoiced
            ) as disbursement_seller_net_revenue,

        -- The sign on the disbursed_net_revenue column indicates the change in the balance that
        -- is due to the seller that has been effected by one or more disbursements.
        --
        -- A NEGATIVE disbursed_net_revenue (common case) indicates that money that was owed TO the seller has been
        -- disbursed, thus _reducing_ the amount owed to the SELLER
        --
        -- A ZERO disbursed_net_revenue indicates that either nothing has been disbursed or the disbursement
        -- failed and it the amounts were reversed.
        --
        -- A POSITIVE disbursed_net_revenue (disbursement failure, disbursement of refund, etc) indicates that money
        -- was was owed BY the seller has been reclaimed from the seller. This is only possible when an overall
        -- disbursement (for many invoices) is larger than the reclaimed amount (for this invoice line item),
        -- such that the reclaimed amount can be deducted from the larger disbursement.
        --
        -- This column ignores the disbursement_id_or_invoiced values. Regardless of whether it is
        -- '<invoiced>' or refers to a disbursement billing event ID, this column refers to the REDUCTION
        -- of the balance that was (or is) due to the seller.
        -- Pricing Currency
        (
            gross_revenue_disbursed
            + gross_refund_disbursed
            + aws_rev_share_disbursed
            + aws_refund_share_disbursed
            + seller_tax_share_disbursed
            + seller_tax_share_refund_disbursed
            + balance_adjustment_disbursed
            + coalesce(infrastructure_netting_disbursed, 0.0)
            + cogs_disbursed
            + cogs_refund_disbursed
            + aws_tax_share_balance_impacting_disbursed
            + aws_tax_share_refund_balance_impacting_disbursed
            + other_seller_tax_share_disbursed
            + other_seller_tax_share_refund_disbursed
            + wholesale_seller_tax_share_disbursed
            + wholesale_seller_tax_share_refund_disbursed
            + wholesale_other_seller_tax_share_disbursed
            + wholesale_other_seller_tax_share_refund_disbursed
        ) as disbursed_net_revenue,
        -- Disbursement Currency
        (
            disbursement_gross_revenue_disbursed
                + disbursement_gross_refund_disbursed
                + disbursement_aws_rev_share_disbursed
                + disbursement_aws_refund_share_disbursed
                + disbursement_seller_tax_share_disbursed
                + disbursement_seller_tax_share_refund_disbursed
                + disbursement_balance_adjustment_disbursed
                + coalesce(disbursement_infrastructure_netting_disbursed, 0.0)
                + disbursement_cogs_disbursed
                + disbursement_cogs_refund_disbursed
                + disbursement_aws_tax_share_balance_impacting_disbursed
                + disbursement_aws_tax_share_refund_balance_impacting_disbursed
                + disbursement_other_seller_tax_share_disbursed
                + disbursement_other_seller_tax_share_refund_disbursed
                + disbursement_wholesale_seller_tax_share_disbursed
                + disbursement_wholesale_seller_tax_share_refund_disbursed
                + disbursement_wholesale_other_seller_tax_share_disbursed
                + disbursement_wholesale_other_seller_tax_share_refund_disbursed
            ) as disbursement_disbursed_net_revenue,

        last_collection_date,

        last_disbursement_date,
        last_disbursement_id,
        last_disburse_bank_trace_id,
        last_disburse_amazon_reference_id,
        disbursement_date_list,
        disburse_bank_trace_id_list,
        disburse_amazon_reference_id_list,
        product_code,
        manufacturer_aws_account_id,
        manufacturer_encrypted_account_id,
        manufacturer_account_id,
        acc_manu.mailing_company_name as manufacturer_company_name,
        cast(null as varchar) as AR_Period,

        line.seller_issued_invoice_id,      -- For deemed VAT (and future) where AWS is the recipient
        line.seller_issued_invoice_variant, -- For deemed VAT (and future) where AWS is the recipient

        min(least(line.min_data_catalog, acc_manu.min_data_catalog, acc_proposer.min_data_catalog, add_payer.min_data_catalog, payer_tax_address.min_data_catalog, payer_mailing_address.min_data_catalog, add_subscriber.min_data_catalog, subscriber_tax_address.min_data_catalog, subscriber_mailing_address.min_data_catalog, add_enduser.min_data_catalog, end_user_tax_address.min_data_catalog, end_user_mailing_address.min_data_catalog, billing_address.min_data_catalog)) as min_data_catalog,
        max(greatest(line.max_data_catalog, acc_manu.max_data_catalog, acc_proposer.max_data_catalog, add_payer.max_data_catalog, payer_tax_address.max_data_catalog, payer_mailing_address.max_data_catalog, add_subscriber.max_data_catalog, subscriber_tax_address.max_data_catalog, subscriber_mailing_address.max_data_catalog, add_enduser.max_data_catalog, end_user_tax_address.max_data_catalog, end_user_mailing_address.max_data_catalog, billing_address.max_data_catalog)) as max_data_catalog,
        min(line.data_catalog) as data_catalog
    from
        line_items_with_window_functions_enrich_offer_product_address as line
    left join accounts_with_history_with_company_name as acc_manu on
        line.manufacturer_account_id = acc_manu.account_id and line.purchase_invoice_date >= acc_manu.valid_from_adjusted and line.purchase_invoice_date <= acc_manu.valid_to
    left join accounts_with_history_with_company_name as acc_proposer on
        line.proposer_account_id = acc_proposer.account_id and line.purchase_invoice_date >= acc_proposer.valid_from and line.purchase_invoice_date < acc_proposer.valid_to
    left join address_with_latest_revision as add_payer on
        line.payer_address_id = add_payer.address_id /* DEPRECATED */
    left join address_with_latest_revision as payer_tax_address on
        line.payer_tax_address_id = payer_tax_address.address_id
    left join address_with_latest_revision as payer_mailing_address on
        line.payer_mailing_address_id = payer_mailing_address.address_id
    left join address_with_latest_revision as add_subscriber on
        line.subscriber_address_id = add_subscriber.address_id /* DEPRECATED */
    left join address_with_latest_revision as subscriber_tax_address on
        line.subscriber_tax_address_id = subscriber_tax_address.address_id
    left join address_with_latest_revision as subscriber_mailing_address on
        line.subscriber_mailing_address_id = subscriber_mailing_address.address_id
    left join address_with_latest_revision as add_enduser on
        line.end_user_address_id = add_enduser.address_id /* DEPRECATED */
    left join address_with_latest_revision as end_user_tax_address on
        line.end_user_tax_address_id = end_user_tax_address.address_id
    left join address_with_latest_revision as end_user_mailing_address on
        line.end_user_mailing_address_id = end_user_mailing_address.address_id
    left join address_with_latest_revision as billing_address on
        line.billing_address_id = billing_address.address_id

group by
    internal_buyer_invoice_line_item_surrogate_id_with_currency,
    count_distinct_pricing_currency,
    count_distinct_disbursement_currency,
    internal_buyer_invoice_line_item_surrogate_id,
    report_transaction_reference_id,
    disbursement_id,
    disbursement_id_or_invoiced,
    line.recipient_account_id,
    line.agreement_valid_from,
    product_id,
    legacy_product_id,
    product_title,
    broker_id,
    currency,
    line.disbursement_currency,
    subscriber_tax_legal_name,
    payer_tax_legal_name,
    end_user_tax_legal_name,
    subscriber_tax_address_id,
    subscriber_mailing_address_id,
    payer_tax_address_id,
    payer_mailing_address_id,
    end_user_tax_address_id,
    end_user_mailing_address_id,
    end_user_address_id,
    end_user_account_id,
    end_user_encrypted_account_id,
    end_user_aws_account_id,
    coalesce(
            line.end_user_tax_legal_name,
            end_user_tax_address.company_name,
            billing_address.company_name,
            end_user_mailing_address.company_name
        ),
    add_enduser.email_domain,
    add_enduser.city,
    add_enduser.state_or_region,
    add_enduser.country_code,
    add_enduser.postal_code,
    payer_aws_account_id,
    payer_encrypted_account_id,
    payer_address_id,
    coalesce(
            line.payer_tax_legal_name,
            payer_tax_address.company_name,
            billing_address.company_name,
            payer_mailing_address.company_name
        ),
    add_payer.email_domain,
    add_payer.city,
    add_payer.state_or_region,
    add_payer.country_code,
    add_payer.postal_code,
    agreement_id,
    wholesale_agreement_id,
    agreement_revision,
    status,
    estimated_charges_currency_code,
    estimated_charges_net_amount,
    case when proposer_account_id = recipient_account_id then null else acc_proposer.aws_account_id end,
    case when proposer_account_id = recipient_account_id then null else acc_proposer.encrypted_account_id end,
    case when proposer_account_id = recipient_account_id then null else acc_proposer.mailing_company_name end,
    agreement_start_date,
    agreement_end_date,
    agreement_acceptance_date,
    agreement_updated_date,
    usage_period_start_date,
    usage_period_end_date,
    acceptor_account_id,
    subscriber_aws_account_id,
    subscriber_encrypted_account_id,
    subscriber_address_id,
    coalesce(
            line.subscriber_tax_legal_name,
            subscriber_tax_address.company_name,
            subscriber_mailing_address.company_name
        ) ,
    add_subscriber.email_domain,
    add_subscriber.city,
    add_subscriber.state_or_region,
    add_subscriber.country_code,
    add_subscriber.postal_code,
    offer_id,
    offer_set_id,
    offer_target,
    offer_name,
    offer_opportunity_name,
    offer_opportunity_description,
    opportunity_id,
    payment_due_date,
    bank_trace_id,
    amazon_reference_id,
    disbursement_date,
    disbursement_status,
    billing_address_id,
    is_isv_wholesale_invoice,
    gross_revenue_this_disbursement_id_or_invoiced,
    gross_refund_this_disbursement_id_or_invoiced,
    cogs_this_disbursement_id_or_invoiced,
    cogs_refund_this_disbursement_id_or_invoiced,
    aws_rev_share_this_disbursement_id_or_invoiced,
    aws_refund_share_this_disbursement_id_or_invoiced,
    aws_tax_share_this_disbursement_id_or_invoiced,
    aws_tax_share_listing_fee_this_disbursement_id_or_invoiced,
    aws_tax_share_refund_this_disbursement_id_or_invoiced,
    aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced,
    seller_tax_share_this_disbursement_id_or_invoiced,
    seller_tax_share_refund_this_disbursement_id_or_invoiced,
    infrastructure_netting_this_disbursement_id_or_invoiced,
    balance_adjustment_this_disbursement_id_or_invoiced,
    seller_rev_credit_this_disbursement_id_or_invoiced,
    aws_ref_fee_credit_this_disbursement_id_or_invoiced,
    other_seller_tax_share_this_disbursement_id_or_invoiced,
    other_seller_tax_share_refund_this_disbursement_id_or_invoiced,
    wholesale_aws_tax_share_this_disbursement_id_or_invoiced,
    wholesale_aws_tax_share_refund_this_disbursement_id_or_invoiced,
    wholesale_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    wholesale_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,
    wholesale_seller_tax_share_this_disbursement_id_or_invoiced,
    wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced,
    wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced,
    wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,

    all_balance_impacting_this_disbursement_id_or_invoiced,
    aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,

    disbursement_gross_revenue_this_disbursement_id_or_invoiced,
    disbursement_gross_refund_this_disbursement_id_or_invoiced,
    disbursement_cogs_this_disbursement_id_or_invoiced,
    disbursement_cogs_refund_this_disbursement_id_or_invoiced,
    disbursement_aws_rev_share_this_disbursement_id_or_invoiced,
    disbursement_aws_refund_share_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_listing_fee_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_refund_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced,
    disbursement_seller_tax_share_this_disbursement_id_or_invoiced,
    disbursement_seller_tax_share_refund_this_disbursement_id_or_invoiced,
    disbursement_other_seller_tax_share_this_disbursement_id_or_invoiced,
    disbursement_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,
    disbursement_wholesale_aws_tax_share_this_disbursement_id_or_invoiced,
    disbursement_wholesale_aws_tax_share_refund_this_disbursement_id_or_invoiced,
    disbursement_wholesale_seller_tax_share_this_disbursement_id_or_invoiced,
    disbursement_wholesale_seller_tax_share_refund_this_disbursement_id_or_invoiced,
    disbursement_wholesale_other_seller_tax_share_this_disbursement_id_or_invoiced,
    disbursement_wholesale_other_seller_tax_share_refund_this_disbursement_id_or_invoiced,
    disbursement_infrastructure_netting_this_disbursement_id_or_invoiced,
    disbursement_balance_adjustment_this_disbursement_id_or_invoiced,
    disbursement_seller_rev_credit_this_disbursement_id_or_invoiced,
    disbursement_aws_ref_fee_credit_this_disbursement_id_or_invoiced,
    disbursement_wholesale_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    disbursement_wholesale_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,

    disbursement_aws_tax_share_balance_impacting_this_disbursement_id_or_invoiced,
    disbursement_aws_tax_share_refund_balance_impacting_this_disbursement_id_or_invoiced,

    disbursement_all_balance_impacting_this_disbursement_id_or_invoiced,

    gross_revenue_invoiced,
    gross_refund_invoiced,
    cogs_invoiced,
    cogs_refund_invoiced,
    aws_rev_share_invoiced,
    aws_refund_share_invoiced,
    aws_tax_share_invoiced,
    aws_tax_share_listing_fee_invoiced,
    aws_tax_share_refund_invoiced,
    aws_tax_share_refund_listing_fee_invoiced,
    seller_tax_share_invoiced,
    seller_tax_share_refund_invoiced,
    infrastructure_netting_invoiced,
    balance_adjustment_invoiced,
    seller_rev_credit_invoiced,
    aws_ref_fee_credit_invoiced,
    other_seller_tax_share_invoiced,
    other_seller_tax_share_refund_invoiced,
    wholesale_aws_tax_share_invoiced,
    wholesale_aws_tax_share_refund_invoiced,
    wholesale_aws_tax_share_balance_impacting_invoiced,
    wholesale_aws_tax_share_refund_balance_impacting_invoiced,
    wholesale_seller_tax_share_invoiced,
    wholesale_seller_tax_share_refund_invoiced,
    wholesale_other_seller_tax_share_invoiced,
    wholesale_other_seller_tax_share_refund_invoiced,

    aws_tax_share_balance_impacting_invoiced,
    aws_tax_share_refund_balance_impacting_invoiced,

    all_balance_impacting_invoiced,
    all_balance_impacting_collected,

    disbursement_gross_revenue_invoiced,
    disbursement_gross_refund_invoiced,
    disbursement_cogs_invoiced,
    disbursement_cogs_refund_invoiced,
    disbursement_aws_rev_share_invoiced,
    disbursement_aws_refund_share_invoiced,
    disbursement_aws_tax_share_invoiced,
    disbursement_aws_tax_share_listing_fee_invoiced,
    disbursement_aws_tax_share_refund_invoiced,
    disbursement_aws_tax_share_refund_listing_fee_invoiced,
    disbursement_seller_tax_share_invoiced,
    disbursement_seller_tax_share_refund_invoiced,
    disbursement_other_seller_tax_share_invoiced,
    disbursement_other_seller_tax_share_refund_invoiced,
    disbursement_wholesale_aws_tax_share_balance_impacting_invoiced,
    disbursement_wholesale_aws_tax_share_refund_balance_impacting_invoiced,
    disbursement_wholesale_seller_tax_share_invoiced,
    disbursement_wholesale_seller_tax_share_refund_invoiced,
    disbursement_wholesale_other_seller_tax_share_invoiced,
    disbursement_wholesale_other_seller_tax_share_refund_invoiced,
    disbursement_infrastructure_netting_invoiced,
    disbursement_balance_adjustment_invoiced,
    disbursement_seller_rev_credit_invoiced,
    disbursement_aws_ref_fee_credit_invoiced,
    disbursement_wholesale_aws_tax_share_invoiced,
    disbursement_wholesale_aws_tax_share_refund_invoiced,

    disbursement_aws_tax_share_balance_impacting_invoiced,
    disbursement_aws_tax_share_refund_balance_impacting_invoiced,

    disbursement_all_balance_impacting_invoiced,

    gross_revenue_disbursed,
    gross_refund_disbursed,
    cogs_disbursed,
    cogs_refund_disbursed,
    aws_rev_share_disbursed,
    aws_refund_share_disbursed,
    aws_tax_share_disbursed,
    aws_tax_share_listing_fee_disbursed,
    aws_tax_share_refund_disbursed,
    aws_tax_share_refund_listing_fee_disbursed,
    seller_tax_share_disbursed,
    seller_tax_share_refund_disbursed,
    infrastructure_netting_disbursed,
    balance_adjustment_disbursed,
    seller_rev_credit_disbursed,
    aws_ref_fee_credit_disbursed,
    other_seller_tax_share_disbursed,
    other_seller_tax_share_refund_disbursed,
    wholesale_aws_tax_share_disbursed,
    wholesale_aws_tax_share_refund_disbursed,
    wholesale_aws_tax_share_balance_impacting_disbursed,
    wholesale_aws_tax_share_refund_balance_impacting_disbursed,
    wholesale_seller_tax_share_disbursed,
    wholesale_seller_tax_share_refund_disbursed,
    wholesale_other_seller_tax_share_disbursed,
    wholesale_other_seller_tax_share_refund_disbursed,

    aws_tax_share_balance_impacting_disbursed,
    aws_tax_share_refund_balance_impacting_disbursed,

    all_balance_impacting_disbursed,

    disbursement_gross_revenue_disbursed,
    disbursement_gross_refund_disbursed,
    disbursement_cogs_disbursed,
    disbursement_cogs_refund_disbursed,
    disbursement_aws_rev_share_disbursed,
    disbursement_aws_refund_share_disbursed,
    disbursement_aws_tax_share_disbursed,
    disbursement_aws_tax_share_listing_fee_disbursed,
    disbursement_aws_tax_share_refund_disbursed,
    disbursement_aws_tax_share_refund_listing_fee_disbursed,
    disbursement_seller_tax_share_disbursed,
    disbursement_seller_tax_share_refund_disbursed,
    disbursement_other_seller_tax_share_disbursed,
    disbursement_other_seller_tax_share_refund_disbursed,
    disbursement_wholesale_aws_tax_share_balance_impacting_disbursed,
    disbursement_wholesale_aws_tax_share_refund_balance_impacting_disbursed,
    disbursement_wholesale_seller_tax_share_disbursed,
    disbursement_wholesale_seller_tax_share_refund_disbursed,
    disbursement_wholesale_other_seller_tax_share_disbursed,
    disbursement_wholesale_other_seller_tax_share_refund_disbursed,
    disbursement_infrastructure_netting_disbursed,
    disbursement_balance_adjustment_disbursed,
    disbursement_seller_rev_credit_disbursed,
    disbursement_aws_ref_fee_credit_disbursed,
    disbursement_wholesale_aws_tax_share_disbursed,
    disbursement_wholesale_aws_tax_share_refund_disbursed,


    disbursement_aws_tax_share_balance_impacting_disbursed,
    disbursement_aws_tax_share_refund_balance_impacting_disbursed,

    disbursement_all_balance_impacting_disbursed,

    last_collection_date,
    last_disbursement_date,
    last_disbursement_id,
    last_disburse_bank_trace_id,
    last_disburse_amazon_reference_id,
    disbursement_date_list,
    disburse_bank_trace_id_list,
    disburse_amazon_reference_id_list,
    product_code,
    manufacturer_aws_account_id,
    manufacturer_encrypted_account_id,
    manufacturer_account_id,
    acc_manu.mailing_company_name,
    proposer_account_id,
    acc_proposer.aws_account_id,
    seller_issued_invoice_id,
    seller_issued_invoice_variant
),


-- ============================================================================
-- CTE: line_items_with_window_functions_enrich_offer_product_address_name_undisbursed
-- Computes undisbursed_net_revenue in both pricing and disbursement currencies.
-- ============================================================================

line_items_with_window_functions_enrich_offer_product_address_name_undisbursed as (
select
    *,
    -- The sign on the undisbursed_net_revenue column indicates the balance that is still due to the seller,
    -- taking disbursements into account.
    --
    -- A POSITIVE undisbursed_net_revenue (common case) indicates that money is still owed TO the seller.
    -- A ZERO     undisbursed_net_revenue (common case) indicates that no more money is still owed TO the seller.
    -- A NEGATIVE undisbursed_net_revenue (rare over disbursed) indicates that money is still owed BY the seller,
    --                                    although for $0.01 rounding issues it will probably never be
    --                                    clawed back from the seller.
    --
    -- This column ignores the disbursement_id_or_invoiced values. Regardless of whether it is
    -- '<invoiced>' or refers to a disbursement billing event ID, this column refers to the balance
    -- that is still due to the seller, taking disbursements into account.
    -- This column is simply the sum of seller_net_revenue and disbursed_net_revenue:
    -- Pricing Currency
    (
        seller_net_revenue        -- Normally positive (but negative for refunds)
        + disbursed_net_revenue   -- Normally negative or zero
    ) as undisbursed_net_revenue, -- Normally positive or zero
    -- Disbursement Currency
    (
        disbursement_seller_net_revenue        -- Normally positive (but negative for refunds)
        + disbursement_disbursed_net_revenue   -- Normally negative or zero
    ) as disbursement_undisbursed_net_revenue -- Normally positive or zero

from
    line_items_with_window_functions_enrich_offer_product_address_name_disbursed as line
),

-- ============================================================================
-- CTE: line_items_with_window_functions_enrich_offer_product_address_name_disbursement_flag
-- Computes disbursement_flag based on disbursement status and net revenue values.
-- ============================================================================

line_items_with_window_functions_enrich_offer_product_address_name_disbursement_flag as (
-- Athena does not recognize calculation in the same query, adding steps here to do those calculations
select
    *,
    case
        when disbursement_status = 'DISBURSEMENT_FAILURE' then 'Failed'

        -- (1) there HAS been a disbursement, and there is NOTHING left to disburse:
        when last_disbursement_id is not null and undisbursed_net_revenue =  0 then 'Yes'

        -- (2) there HAS been a disbursement, but the disbursement was completely reversed (it probably failed), summing up to $0 net disbursed:
        when last_disbursement_id is not null and undisbursed_net_revenue <> 0 and disbursed_net_revenue =  0 then 'Reversed'

        -- (3) there HAS been a PARTIAL disbursement, and there is SOMETHING left to disburse:
        --     NUANCE: If the MP Disbursement Service over or under disburses by one penny $0.01 (due to a know bug),
        --             I think we still need to (simply) call it a 'Partial' disbursement because it would be
        --             confusing otherwise.
        --
        --             Not convinced? Consider that we can (easily) label over disbursement as 'Yes'
        --             without also labeling an under disbursement of a later refund as 'Yes'.
        --             If we later have a refund, a previous over-disbursement would need be clawed back,
        --             (with the signs reversed), and this would look like an under disbursement.
        --             It would be confusing (and complicated) to label an under-disbursement as 'Yes',
        --             but only if it is a refund of a previous over disbursement.
        when last_disbursement_id is not null and undisbursed_net_revenue <> 0 and disbursed_net_revenue <> 0 then
            case when undisbursed_net_revenue < 0 then 'Over' else 'Partial' end

        -- (4) there has been NO disbursement and there is NOTHING left to disburse (perhaps settled via refund):
        when last_disbursement_id is     null and undisbursed_net_revenue =  0 then 'No' -- TBD: Should we change this to 'Not applicable' or 'Not required'?

        -- (5) there has been NO disbursement and there is SOMETHING left to disburse:
        when last_disbursement_id is     null and undisbursed_net_revenue <> 0 then 'No' -- TBD: Should this be renamed to 'Never'?

        else 'Unknown' -- This should never happen!
        end as disbursement_flag
from
    line_items_with_window_functions_enrich_offer_product_address_name_undisbursed as line
),

-- ============================================================================
-- CTE: line_items_with_window_functions_enrich_offer_product_address_name
-- Computes disbursement_flag_status from disbursement_flag.
-- ============================================================================

line_items_with_window_functions_enrich_offer_product_address_name as (
-- Athena does not recognize calculation in the same query, adding steps here to do those calculations
select
    *,
    case
        when disbursement_flag = 'Failed' then 'Failed'
        -- This FIRST unioned query filters where disbursement_id_or_invoiced <> '<invoiced>', so disbursement_flag should never be 'No':
        when disbursement_flag = 'No' then 'Not disbursed' -- TBD: Should we distinguish 'Never disbursed' from settled via refund?
        when disbursement_flag = 'Reversed' then 'Disbursement reversed'
        when disbursement_flag = 'Yes' then 'Disbursed'
        when disbursement_flag = 'Partial' then 'Partially disbursed'
        when disbursement_flag = 'Over' then 'Disbursed'
        --this should never exist
        else 'Invalid status'
    end as disbursement_flag_status
from
    line_items_with_window_functions_enrich_offer_product_address_name_disbursement_flag as line
),

-- ============================================================================
-- CTE: revenue_recognition_at_invoice_time_refactor
-- Filters to invoiced-only records, joins agreement term types,
-- applies COALESCE defaults and rounding for the final output columns.
-- ============================================================================

revenue_recognition_at_invoice_time_refactor as (
select
    line.internal_buyer_invoice_line_item_surrogate_id_with_currency,
    line.count_distinct_pricing_currency,
    line.count_distinct_disbursement_currency,
    line.internal_buyer_invoice_line_item_surrogate_id,
    line.end_user_account_id as internal_end_user_account_id,

    --Payer Information
    coalesce(Payer_Address_ID, 'Not available') as Payer_Address_ID,
    coalesce(Payer_AWS_Account_ID, 'Not available') as Payer_AWS_Account_ID,
    Payer_Encrypted_Account_ID,
    coalesce(Payer_Company_Name, 'Not available') as Payer_Company_Name,
    coalesce(Payer_Email_Domain, 'Not available') as Payer_Email_Domain,
    coalesce(Payer_City, 'Not available') as Payer_City,
    coalesce(Payer_State, 'Not available') as Payer_State,
    coalesce(Payer_Country, 'Not available') as Payer_Country,
    coalesce(Payer_Postal_Code, 'Not available') as Payer_Postal_Code,

    --End Customer Information
    coalesce(End_User_Address_Id, 'Not available') as End_User_Address_Id,
    coalesce(end_user_aws_account_id, 'Not available') as End_Customer_AWS_Account_ID,
    End_User_Encrypted_Account_ID,
    coalesce(end_user_company_name, 'Not available') as End_Customer_Company_Name,
    coalesce(end_user_email_domain, 'Not available') as End_Customer_Email_Domain,
    coalesce(end_user_city, 'Not available') as End_Customer_City,
    coalesce(end_user_state_or_region, 'Not available') as End_Customer_State_Or_Region,
    coalesce(end_user_country, 'Not available') as End_Customer_Country,
    coalesce(end_user_postal_code, 'Not available') as End_Customer_Postal_Code,

    --Subscriber Information
    case
        when line.Agreement_ID is null then 'Not available'
        when line.Subscriber_Address_ID is null then 'Not provided'
        else line.Subscriber_Address_ID
        end as Subscriber_Address_ID,
    case
        when line.Agreement_Id is null then 'Not available'
        else line.Subscriber_AWS_Account_ID
        end as Subscriber_AWS_Account_ID,
    case
        when line.Agreement_Id is null then 'Not available'
        else line.Subscriber_Encrypted_Account_ID
        end as Subscriber_Encrypted_Account_ID,
    case
        when line.Agreement_Id is null then 'Not available'
        when line.Subscriber_Company_Name is null then 'Not provided'
        else line.Subscriber_Company_Name
        end as Subscriber_Company_Name,
    case
        when line.Agreement_Id is null then 'Not available'
        when line.Subscriber_Email_Domain is null then 'Not provided'
        else line.Subscriber_Email_Domain
        end as Subscriber_Email_Domain,
    case
        when line.Agreement_id is null then 'Not available'
        when line.Subscriber_City is null then 'Not provided'
        else line.Subscriber_City
        end as Subscriber_City,
    case
        when line.Agreement_Id is null then 'Not available'
        when line.Subscriber_State_Or_Region is null then 'Not provided'
        else line.Subscriber_State_Or_Region
        end as Subscriber_State_Or_Region,
    case
        when line.Agreement_Id is null then 'Not available'
        when line.Subscriber_Country is null then 'Not provided'
        else line.Subscriber_Country
        end as Subscriber_Country,
    case
        when line.Agreement_Id is null then 'Not available'
        when line.Subscriber_Postal_Code is null then 'Not provided'
        else line.Subscriber_Postal_Code
        end as Subscriber_Postal_Code,

    ------------------
    -- Product Info --
    ------------------
    Coalesce(line.Product_ID,'Not provided') as Product_ID,
    Coalesce(line.Product_Code,'Not provided') as Product_Code,
    -- product title at time of invoice. It is possible that the title changes over time and therefore there may be multiple product titles mapped to a single product id.
    Coalesce(line.Product_Title,'Not provided') as Product_Title,
    line.Legacy_Product_ID,

    ----------------------
    -- Procurement Info --
    ----------------------
    case when line.Offer_ID is null then 'Not available' else line.Offer_ID end as Offer_ID,
    case when line.agreement_id is null then 'Not available' when offer_set_id is null then 'Not applicable' else offer_set_id end as offer_set_id,
    -- offer name at time of invoice. It is possible that the name changes over time therefore there may be multiple offer names mapped to a single offer id.
    case when line.Offer_ID is null then 'Not available' when line.Offer_Name is null and line.Offer_Target = 'Public' then 'Not applicable' else line.Offer_Name end as Offer_Name,
    -- offer target at time of invoice.,
    case when line.Offer_ID is null then 'Not available' else line.Offer_Target end as Offer_Target,
    case when line.Opportunity_Id is null then
             case when line.Offer_Target = 'Public' then 'Not applicable'
                  when line.Offer_Target is null and line.Agreement_Id is not null then 'Not applicable'
                  else null end else line.Opportunity_Id end as Opportunity_Id,
    case when line.Offer_Opportunity_Name is null then
             case when line.Offer_Target = 'Public' then 'Not applicable'
                  when line.Offer_Target is null and line.Agreement_Id is not null then 'Not applicable'
                  else null end
         else line.Offer_Opportunity_Name end as Offer_Opportunity_Name,
    case when line.Offer_Opportunity_Description is null then
             case when line.Offer_Target = 'Public' then 'Not applicable'
                  when line.Offer_Target is null and line.Agreement_Id is not null then 'Not applicable'
                  else null end
         else line.Offer_Opportunity_Description end as Offer_Opportunity_Description,
    case when line.Offer_target is null and line.Agreement_Id is not null and line.Agreement_Id <> 'Not available' then 'Public' else line.Offer_target end as offer_visibility,
    case
        when line.Reseller_AWS_Account_ID is null and line.Opportunity_Id is null then 'Not applicable'
        when line.recipient_account_id <> line.manufacturer_account_id and line.Reseller_AWS_Account_ID is null then 'Not applicable'
        else line.Reseller_AWS_Account_ID end as Reseller_AWS_Account_ID,
    line.Reseller_Encrypted_Account_ID,
    case
        when line.Reseller_AWS_Account_ID is not null and line.Reseller_Company_Name is null then 'Not available'
        when line.Reseller_AWS_Account_ID is null and line.opportunity_id is null then 'Not applicable'
        when line.recipient_account_id <> line.manufacturer_account_id and line.Reseller_AWS_Account_ID is null then 'Not applicable'
        else line.Reseller_Company_Name end as Reseller_Company_Name,
    coalesce(line.Agreement_ID, 'Not available') as Agreement_ID,
    -- all agreement related data are surfaced as they were at time of invoice.
    case when line.Agreement_Id is null then 'Not available'
         when line.Agreement_Revision is null or line.Agreement_Revision = '' then 'Not provided'
         else line.Agreement_Revision end as Agreement_Revision,
    case when line.Agreement_Id is null then cast(null as TIMESTAMP WITH TIME ZONE) else line.Agreement_Start_Date end as Agreement_Start_Date,
    case when line.Agreement_Id is null then cast(null as TIMESTAMP WITH TIME ZONE) else line.Agreement_End_Date end as Agreement_End_Date,
    case when line.Agreement_Id is null then cast(null as TIMESTAMP WITH TIME ZONE) else line.Agreement_Acceptance_Date end as Agreement_Acceptance_Date,
    terms.accepted_term_types,

    case when line.Agreement_Id is null then 'Not available'
         when line.wholesale_agreement_id is null then 'Not applicable'
         else line.wholesale_agreement_id end as
    wholesale_agreement_id,
    line.status,
    line.estimated_charges_currency_code,
    line.estimated_charges_net_amount,
    line.purchase_invoice_id,
    coalesce(line.listing_fee_invoice_id, 'Not applicable') as listing_fee_invoice_id,
    coalesce(line.resale_invoice_id, 'Not applicable') as resale_invoice_id,

    line.report_transaction_reference_id as transaction_reference_id,

    line.purchase_invoice_date as Invoice_Date,
    line.listing_fee_invoice_date as Charge_Invoice_Date,
    line.Usage_Period_Start_Date,
    line.Usage_Period_End_Date,

    -- We are rounding the sums using 2 decimal precision
    -- Note that the rounding method might differ between SQL implementations.
    -- The monthly revenue report is using RoundingMode.HALF_UP. This might create tiny discrepancies between this SQL output
    -- and the legacy report
    round(line.gross_revenue_invoiced,2) as Gross_Revenue,
    round(line.gross_refund_invoiced,2) as Gross_Refund,
    line.Payment_Due_Date,
    round(line.aws_rev_share_invoiced,2) as Listing_Fee,
    round(line.aws_refund_share_invoiced,2) as Listing_Fee_Refund,
    case
        when line.gross_revenue_invoiced + line.gross_refund_invoiced != 0 then
            truncate(
              abs((line.aws_rev_share_invoiced + line.aws_refund_share_invoiced)/(line.gross_revenue_invoiced + line.gross_refund_invoiced)), 4)
        else 0
    end as Listing_Fee_Percentage,
    round(line.aws_tax_share_invoiced,2) as AWS_Tax_Share,
    round(line.aws_tax_share_refund_invoiced,2) as AWS_Tax_Share_Refund,
    round(line.aws_tax_share_listing_fee_invoiced,2) as AWS_Tax_Share_Listing_Fee,
    round(line.aws_tax_share_refund_listing_fee_invoiced,2) as AWS_Tax_Share_Refund_Listing_Fee,
    round(line.seller_tax_share_invoiced,2) as Seller_Tax_Share,
    round(line.seller_tax_share_refund_invoiced,2) as Seller_Tax_Share_Refund,
    round(line.infrastructure_netting_invoiced,2) as Infrastructure_Netting,
    round(line.cogs_invoiced,2) as COGS,
    round(line.cogs_refund_invoiced,2) as COGS_Refund,
    round(line.other_seller_tax_share_invoiced,2) as Other_Seller_Tax_Share,
    round(line.other_seller_tax_share_refund_invoiced,2) as Other_Seller_Tax_Share_Refund,

    round(line.wholesale_aws_tax_share_invoiced,2) as wholesale_aws_tax_share,
    round(line.wholesale_aws_tax_share_refund_invoiced,2) as wholesale_aws_tax_share_refund,
    round(line.wholesale_seller_tax_share_invoiced,2) as wholesale_seller_tax_share,
    round(line.wholesale_seller_tax_share_refund_invoiced,2) as wholesale_seller_tax_share_refund,
    round(line.wholesale_other_seller_tax_share_invoiced,2) as wholesale_other_seller_tax_share,
    round(line.wholesale_other_seller_tax_share_refund_invoiced,2) as wholesale_other_seller_tax_share_refund,

    round(line.all_balance_impacting_invoiced,2) as all_balance_impacting_invoiced,

    -- summing rounded amounts to ensure that the calculation is consistent between above figures and this one
    round(line.seller_net_revenue,2) as Seller_Net_Revenue,
    round(line.balance_adjustment_invoiced,2) as Balance_Adjustment,
    -1 * round(line.disbursed_net_revenue,2) as disbursed_net_revenue,
    round(line.undisbursed_net_revenue,2) as undisbursed_net_Revenue,

    ------------------
    -- Disbursement --
    ------------------
    round(line.gross_revenue_disbursed,2) as Gross_Revenue_Disbursed,
    round(line.gross_refund_disbursed,2) as Gross_Refund_Disbursed,
    round(line.aws_rev_share_disbursed,2) as Listing_Fee_Disbursed,
    round(line.aws_refund_share_disbursed,2) as Listing_Fee_Refund_Disbursed,
    round(line.aws_tax_share_disbursed,2) as AWS_Tax_Share_Disbursed,
    round(line.aws_tax_share_refund_disbursed,2) as AWS_Tax_Share_Refund_Disbursed,
    round(line.aws_tax_share_listing_fee_disbursed,2) as AWS_Tax_Share_Listing_Fee_Disbursed,
    round(line.aws_tax_share_refund_listing_fee_disbursed,2) as AWS_Tax_Share_Refund_Listing_Fee_Disbursed,
    round(line.seller_tax_share_invoiced,2) as Seller_Tax_Share_Disbursed,
    round(line.seller_tax_share_refund_invoiced,2) as Seller_Tax_Share_Refund_Disbursed,
    round(line.cogs_disbursed,2) as COGS_Disbursed,
    round(line.cogs_refund_disbursed,2) as COGS_Refund_Disbursed,
    round(line.infrastructure_netting_disbursed,2) as Infrastructure_Netting_Disbursed,
    round(line.balance_adjustment_disbursed,2) as Balance_Adjustment_Disbursed,
    round(line.other_seller_tax_share_invoiced,2) as Other_Seller_Tax_Share_Disbursed,
    round(line.other_seller_tax_share_refund_invoiced,2) as Other_Seller_Tax_Share_Refund_Disbursed,

    round(line.wholesale_aws_tax_share_disbursed,2) as wholesale_aws_tax_share_disbursed,
    round(line.wholesale_aws_tax_share_refund_disbursed,2) as wholesale_aws_tax_share_refund_disbursed,
    round(line.wholesale_seller_tax_share_disbursed,2) as wholesale_seller_tax_share_disbursed,
    round(line.wholesale_seller_tax_share_refund_disbursed,2) as wholesale_seller_tax_share_refund_disbursed,
    round(line.wholesale_other_seller_tax_share_disbursed,2) as wholesale_other_seller_tax_share_disbursed,
    round(line.wholesale_other_seller_tax_share_refund_disbursed,2) as wholesale_other_seller_tax_share_refund_disbursed,

    round(line.all_balance_impacting_disbursed,2) as all_balance_impacting_disbursed,

    line.currency as Currency,
    line.disbursement_currency,
    line.disbursement_flag,
    line.disbursement_flag_status as disbursement_status, 
    line.last_collection_date,
    line.last_disbursement_date,
    line.last_disbursement_id,
    case
        when line.last_disbursement_id is null then 'Not applicable'
        -- NOTE: The above when clause is equivalent to the following when clause:
        --when coalesce(Disbursement_Status, '<null>'), <> 'Not disbursed'` then 'Not applicable'
        when line.last_disburse_bank_trace_id is null then 'Not available'
        else line.last_disburse_bank_trace_id
    end as last_disburse_bank_trace_id,
    case
        when line.last_disbursement_id is null then 'Not applicable'
        when line.last_disburse_amazon_reference_id is null then 'Not available'
        else line.last_disburse_amazon_reference_id
    end as last_disburse_amazon_reference_id,
    line.disbursement_date_list,
    case
        when line.last_disbursement_id is null then 'Not applicable'
        -- NOTE: The above when clause is equivalent to the following when clause:
        --when coalesce(Disbursement_Status, '<null>'), <> 'Not disbursed'` then 'Not applicable'
        when line.disburse_bank_trace_id_list is null then 'Not available'
        else line.disburse_bank_trace_id_list
    end as disburse_bank_trace_id_list,
    case
        when line.last_disbursement_id is null or line.last_disbursement_id = '' then 'Not applicable'
        when line.disburse_amazon_reference_id_list is null or line.disburse_amazon_reference_id_list = '' then 'Not available'
        else line.disburse_amazon_reference_id_list
    end as disburse_amazon_reference_id_list,
    line.broker_id as Broker_ID,
    line.manufacturer_aws_account_id as Manufacturer_AWS_Account_ID,
    line.Manufacturer_Encrypted_Account_ID,
    coalesce(line.Manufacturer_Company_Name, 'Not available') as Manufacturer_Company_Name,
    cast(null as varchar) as AR_Period,

    coalesce(line.seller_issued_invoice_id, 'Not applicable') as seller_issued_invoice_id,            -- For deemed VAT (and future) where AWS is the recipient
    coalesce(line.seller_issued_invoice_variant, 'Not applicable') as seller_issued_invoice_variant,  -- For deemed VAT (and future) where AWS is the recipient

    round(line.all_balance_impacting_collected,2) as collected_net_revenue,
    round(line.seller_net_revenue - line.all_balance_impacting_collected,2) as uncollected_net_revenue,
    -- Disbursed amount is a negative value so we should use plus here.
    round(line.all_balance_impacting_collected + line.disbursed_net_revenue,2) as awaiting_disbursement,

    least(line.min_data_catalog, terms.min_data_catalog) as min_data_catalog,
    greatest(line.max_data_catalog, terms.max_data_catalog) as max_data_catalog,
    line.data_catalog

from
    line_items_with_window_functions_enrich_offer_product_address_name as line
left join agreement_with_all_accepted_term_types as terms on
        line.agreement_id = terms.agreement_id and line.agreement_valid_from = terms.revision_creation_time

where
    line.disbursement_id_or_invoiced = '<invoiced>'
),


-- ============================================================================
-- CTE: billed_revenue_report
-- Renames columns for the final user-facing output and computes payment_term.
-- ============================================================================

billed_revenue_report as (
select
    invoice_date, 
    payment_due_date,
    concat('Net ',
        case when abs(date_diff('day', invoice_date, payment_due_date)) > 180 then '180+'
            else cast(abs(date_diff('day', invoice_date, payment_due_date)) as varchar)
           end,
       ' days'
    ) as payment_term,
    purchase_invoice_id as invoice_id,
    listing_fee_invoice_id,
    resale_invoice_id as wholesale_invoice_id,
    seller_issued_invoice_id,
    seller_issued_invoice_variant,

    subscriber_company_name,
    subscriber_aws_account_id,
    subscriber_encrypted_account_id,
    subscriber_email_domain,
    subscriber_city,
    subscriber_state_or_region,
    subscriber_country,
    subscriber_postal_code,
    subscriber_address_id,

    product_title,
    offer_name,
    offer_id,
    offer_set_id,
    offer_visibility,

    agreement_id,
    agreement_start_date,
    agreement_acceptance_date,
    agreement_end_date,
    accepted_term_types,
    data_catalog as catalog,

    usage_period_start_date,
    usage_period_end_date,
    disbursement_status,
    last_disbursement_date as disbursement_date,
    Disburse_Bank_Trace_Id_List as disburse_bank_trace_id,
    disburse_amazon_reference_id_list as disbursement_reference_number,

    gross_revenue,
    gross_refund,
    listing_fee,
    listing_fee_refund,
    listing_fee_percentage,
    seller_tax_share,
    seller_tax_share_refund,
    aws_tax_share,
    aws_tax_share_refund,
    aws_tax_share_listing_fee,
    aws_tax_share_refund_listing_fee,
    cogs as wholesale_cost,
    cogs_refund as wholesale_cost_refund,
    wholesale_seller_tax_share,
    wholesale_seller_tax_share_refund,
    wholesale_other_seller_tax_share,
    wholesale_other_seller_tax_share_refund,
    wholesale_aws_tax_share,
    wholesale_aws_tax_share_refund,
    seller_net_revenue,
    collected_net_revenue,
    uncollected_net_revenue,
    last_collection_date,
    currency,

    transaction_reference_id,
    broker_id as aws_seller_of_record,
    opportunity_id as resale_authorization_id,
    offer_opportunity_name as resale_authorization_name,
    offer_opportunity_description as resale_authorization_description,
    reseller_company_name,
    reseller_aws_account_id,

    payer_company_name,
    payer_aws_account_id,
    payer_encrypted_account_id,
    payer_email_domain,
    payer_city,
    payer_state,
    payer_country,
    payer_postal_code,
    payer_address_id,

    manufacturer_aws_account_id as isv_account_id,
    manufacturer_company_name as isv_company_name,
    product_id,

    disbursed_net_revenue,
    undisbursed_net_revenue,
    internal_buyer_invoice_line_item_surrogate_id_with_currency as silent_internal_report_transaction
from revenue_recognition_at_invoice_time_refactor
)

select invoice_id,
    subscriber_aws_account_id,
    payer_aws_account_id,
    gross_revenue,
    gross_refund,
    last_collection_date
from billed_revenue_report
where invoice_date >= date_add('DAY', -90, current_date)
--where invoice_date between cast('2025-01-01' as TIMESTAMP WITH TIME ZONE) and cast('2026-03-01' as TIMESTAMP WITH TIME ZONE)
