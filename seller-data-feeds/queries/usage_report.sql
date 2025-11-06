-- Usage report

-- General note: When executing this query we are assuming that the data ingested in the database is using
-- two time axes (the valid_from column and the update_date column).
-- See documentation for more details: https://docs.aws.amazon.com/marketplace/latest/userguide/data-feed.html#data-feed-details

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
        tax_registration_number
    from
        (
            select
                account_id,
                aws_account_id,
                encrypted_account_id,
                mailing_address_id,
                tax_address_id,
                tax_legal_name,
                valid_from,
                delete_date,
                tax_registration_number,
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

accounts_with_history as (
    with accounts_with_history_with_extended_valid_from as (
        select
            account_id,
            -- sometimes, this columns gets imported as a "bigint" and loses heading 0s -> casting to a char and re-adding heading 0s (if need be)
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
                then cast('1970-01-01 00:00:00' as timestamp)
                else valid_from
            end as valid_from
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
            cast('2999-01-01 00:00:00' as timestamp)
        ) as valid_to
    from
        accounts_with_history_with_extended_valid_from
),

-- An address_id has several valid_from dates (each representing a separate revision of the data)
-- but because of bi-temporality, an account_id + valid_from tuple can appear multiple times with a different update_date.
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
        --Postal codes are sometimes imported as numbers, make sure here they are all stored as string
        cast(postal_code as varchar) as postal_code,
        row_num
    from
        (
            select
                valid_from,
                update_date,
                delete_date,
                address_id,
                company_name,
                email_domain,
                country_code,
                state_or_region,
                city,
                postal_code,
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

-- We are only interested in the most recent tuple (BTW: a given address is not supposed to change over time but when bugs ;-) so this query mainly does nothing)
address_with_latest_revision as (
    select
        valid_from,
        address_id,
        company_name,
        email_domain,
        country_code,
        state_or_region,
        city,
        postal_code,
        row_num_latest_revision
    from
        (
            select
                valid_from,
                address_id,
                company_name,
                email_domain,
                country_code,
                state_or_region,
                city,
                postal_code,
                row_number() over (partition by address_id order by valid_from desc) as row_num_latest_revision
            from
                address_with_uni_temporal_data
        )
   where
       row_num_latest_revision = 1
),

accounts_with_history_with_company_name as (
    select
        awh.account_id,
        awh.aws_account_id,
        awh.encrypted_account_id,
        awh.mailing_address_id,
        awh.tax_address_id,
        awh.tax_legal_name,
        coalesce(
            --empty value in Athena shows as '', change all '' value to null
            case when address.company_name = '' then null else address.company_name end,
            awh.tax_legal_name) as mailing_company_name,
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
            -- 212 is the longest delay between acceptance_date of the agreement and the account start_Date
            else awh.valid_from
        end as valid_from_adjusted,
        awh.valid_to
from accounts_with_history as awh
    left join address_with_latest_revision as address on
        awh.mailing_address_id = address.address_id and awh.mailing_address_id is not null
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
        from_iso8601_timestamp(start_time) as start_date,
        from_iso8601_timestamp(end_time) as end_date,
        from_iso8601_timestamp(acceptance_time) as acceptance_date,
        intent,
        preceding_agreement_id,
        status,
        status_reason_code,
        cast(estimated_agreement_value as double) as estimated_agreement_value,
        currency_code
    from
    (
        select
            --empty value in Athena shows as '', change all '' value to null
            case when agreement_id = '' then null else agreement_id end as agreement_id,
            offer_id,
            proposer_account_id,
            acceptor_account_id,
            valid_from,
            start_time,
            end_time,
            acceptance_time,
            delete_date,
            intent,
            case when preceding_agreement_id = '' then null else preceding_agreement_id end as preceding_agreement_id,
            status,
            status_reason_code,
            case when estimated_agreement_value = '' then null else estimated_agreement_value end as estimated_agreement_value,
            currency_code,
            row_number() over (partition by agreement_id, valid_from order by from_iso8601_timestamp(update_date) desc) as row_num
        from
            -- TODO change to agreementfeed_v1 when Agreement Feed is GA'ed
            agreementfeed_v1
    )
    where
        -- keep latest ...
        row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
),

agreements_revisions_with_history as (
    with agreements_with_window_functions as (
        select
            agreement_id,
            offer_id,
            proposer_account_id,
            acceptor_account_id,
            start_date,
            end_date,
            acceptance_date,
            -- The start time of agreement valid_from is extended to '1970-01-01 00:00:00', because:
            -- ... in usage report transformations, some usage line items with usage_date cannot
            -- ... fall into the default valid time range of the associated agreement
            case
                when lag(valid_from) over (PARTITION BY agreement_id order by valid_from asc) is null
                then timestamp '1970-01-01 00:00:00'
                else valid_from
            end as valid_from,
            coalesce(
                lead(valid_from) over (partition by agreement_id order by valid_from asc),
                timestamp '2999-01-01 00:00:00'
            ) as valid_to,
            rank() over (partition by agreement_id order by valid_from asc) version,
            preceding_agreement_id as origin_agreement_id,
            intent as origin_intent,
            currency_code estimated_charges_currency_code,
            status,
            status_reason_code,
            estimated_agreement_value estimated_charges_net_amount,
            '' as agreement_revision
        from
            agreements_with_uni_temporal_data
    )
    select
        agreement_id,
        offer_id,
        proposer_account_id,
        acceptor_account_id,
        start_date,
        end_date,
        acceptance_date,
        valid_from,
        case
            -- The following 60 minute adjustment is to handle special case where When Renewal happens for a contract
            when version=1 then date_add('Minute',-60,valid_from)
            else valid_from
        end as valid_from_adjusted,
        valid_to,
        origin_agreement_id,
        origin_intent,
        estimated_charges_currency_code,
        status,
        status_reason_code,
        estimated_charges_net_amount,
        agreement_revision
    from
        agreements_with_window_functions
),

-- An agreement_id has several valid_from dates (each representing an agreement revision)
-- but because of bi-temporality, an agreement_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
agreements_terms_with_uni_temporal_data as (
    select
        agreement_id,
        term_id,
        term_type,
        term_configuration,
        from_iso8601_timestamp(valid_from) as revision_creation_time,
        from_iso8601_timestamp(update_date) as update_date,
        from_iso8601_timestamp(insert_date) as insert_date,
        from_iso8601_timestamp(delete_date) as delete_date
    from
    (
        select
            --empty value in Athena shows as '', change all '' value to null
            case when agreement_id = '' then null else agreement_id end as agreement_id,
            term_id,
            term_type,
            term_configuration,
            valid_from,
            update_date,
            insert_date,
            case when delete_date = '' then null else delete_date end as delete_date,
            row_number() over (partition by term_id, valid_from order by from_iso8601_timestamp(update_date) desc) as row_num
        from
            -- TODO change to agreementfeed_v1 when Agreement Feed is GA'ed
            agreementtermfeed_v1
    )
    where
        -- keep latest ...
        row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
),

agreement_with_all_accepted_term_types as (
    select
        agreement_id,
        revision_creation_time,
        listagg(term_type, ',') within group (order by term_type) accepted_term_types
    from
        agreements_terms_with_uni_temporal_data
    GROUP BY
        agreement_id,
        revision_creation_time
),

-- An offer_id has several valid_from dates (each representing an offer revision)
-- but because of bi-temporality, an offer_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
offers_with_uni_temporal_data as (
    select
        from_iso8601_timestamp(valid_from) as valid_from,
        from_iso8601_timestamp(update_date) as update_date,
        from_iso8601_timestamp(delete_date) as delete_date,
        offer_id,
        offer_revision,
        name,
        expiration_date,
        opportunity_id,
        opportunity_name,
        opportunity_description,
        seller_account_id
    from
    (
        select
            valid_from,
            update_date,
            delete_date,
            offer_id,
            offer_revision,
            name,
            expiration_date,
            case when opportunity_id = '' then null else opportunity_id end as opportunity_id,
            case when opportunity_name = '' then null else opportunity_name end as opportunity_name,
            case when opportunity_description = '' then null else opportunity_description end as opportunity_description,
            seller_account_id,
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

-- Here, we build the validity time range (adding valid_to on top of valid_from) of each offer revision.
-- We will use it to get Offer name at invoice time.
-- NB: If you'd rather get "current" offer name, un-comment "offers_with_latest_revision"
offers_with_history as (
    select
        offer.offer_id,
        offer.offer_revision,
        offer.name,
        offer.opportunity_id,
        offer.opportunity_name,
        offer.opportunity_description,
        accounts.aws_account_id as seller_aws_account_id,
        offer.seller_account_id,
        (accounts.aws_account_id is not null and accounts.aws_account_id <> '276720623826') as is_cppo_offer,
        case when (accounts.aws_account_id is not null and accounts.aws_account_id <> '276720623826') then 'Y' else 'N' end as cppo_flag,
        offer.valid_from,
        -- When we try to look up an offer revision as at the acceptance date of a BYOL agreement, we run into a problem.
        -- For BYOL, the agreement might be accepted (using some external non-AWS system or manual process) days before
        -- that BYOL agreement is entered into AWS Marketplace by the buyer. Therefore, the buyer is permitted to manually
        -- enter a backdated acceptance date, which might predate the point in time when the first revision of the offer
        -- was created. To work around this, we need to adjust the valid_from on the first revision of the offer to be
        -- earlier than the earliest possible backdated BYOL agreement acceptance date.
        case
            when lag(offer.valid_from) over (partition by offer.offer_id order by offer.valid_from asc) is null and offer.valid_from < cast('2021-04-01' as timestamp)
            then date_add('Day', -3857, offer.valid_from)
            -- 3857 is the longest delay between acceptance_date of an agreement and the first revision of the offer
            when lag(offer.valid_from) over (partition by offer.offer_id order by offer.valid_from asc) is null and offer.valid_from >= cast('2021-04-01' as timestamp)
            then date_add('Day', -1460, offer.valid_from)
            --after 2021 for the two offers we need to adjust for 2 more years
            else offer.valid_from
        end as valid_from_adjusted,
        coalesce(
            lead(offer.valid_from) over (partition by offer.offer_id order by offer.valid_from asc),
            cast('2999-01-01 00:00:00' as timestamp))
        as valid_to
    from offers_with_uni_temporal_data offer
    left join accounts_with_history as accounts on
        offer.seller_account_id = accounts.account_id and
        offer.valid_from >= accounts.valid_from and
        offer.valid_from < accounts.valid_to
),

-- An offer_target_id has several valid_from dates (each representing an offer revision)
-- but because of bi-temporality, an offer_target_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
offer_targets_with_uni_temporal_data as (
    select
        from_iso8601_timestamp(valid_from) as valid_from,
        from_iso8601_timestamp(update_date) as update_date,
        from_iso8601_timestamp(delete_date) as delete_date,
        offer_target_id,
        offer_id,
        offer_revision,
        target_type,
        polarity,
        value
    from
    (
        select
            valid_from,
            update_date,
            delete_date,
            offer_target_id,
            offer_id,
            offer_revision,
            target_type,
            polarity,
            value,
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

offer_target_type as (
select
    offer_id,
    offer_revision,
    -- TODO: Remove below for customer facing version
    substring(
        -- The first character indicates the priority (lower value means higher precedence):
        min(
            case
                when offer_target.target_type='BuyerAccounts' then '1Private'
                when offer_target.target_type='ParticipatingPrograms' then '2Program:'||cast(offer_target.value as varchar)
                when offer_target.target_type='CountryCodes' then '3GeoTargeted'
                -- well, there is no other case today, but rather be safe...
                else '4Other Targeting'
            end
        ),
        -- Remove the first character that was only used for th priority in the "min" aggregate function:
        2
    ) as offer_target
from
    offer_targets_with_uni_temporal_data as offer_target
group by
    offer_id,
    offer_revision
),

offers_with_history_with_target_type as (
    select
        offer.offer_id,
        offer.offer_revision,
        cast(max(cast(offer.is_cppo_offer as int)) as boolean) as is_cppo_offer,
        max(offer.cppo_flag) as cppo_flag,
        -- even though today it is not possible to combine several types of targeting in a single offer, let's ensure the query is still predictable if this gets possible in the future
        max(
            case
                when offer.is_cppo_offer then 'Private'
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
        offer.seller_account_id
    from
        offers_with_history as offer
    left join offer_target_type as off_tgt on
        offer.offer_id = off_tgt.offer_id
        and offer.offer_revision = off_tgt.offer_revision
    group by
        offer.offer_id,
        offer.offer_revision,
        offer.valid_from,
        offer.valid_from_adjusted,
        offer.valid_to,
        offer.opportunity_id,
        offer.seller_account_id
),

legacy_products as (
    select
        legacy_id,
        new_id
    from
        legacyidmappingfeed_v1
    where
        mapping_type='PRODUCT'
    group by
        legacy_id,
        new_id
),

-- A product_id has several valid_from dates (each representing a product revision),
-- but because of bi-temporality, each product_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
products_with_uni_temporal_data as (
    select
        from_iso8601_timestamp(valid_from) as valid_from,
        from_iso8601_timestamp(update_date) as update_date,
        from_iso8601_timestamp(delete_date) as delete_date,
        product_id,
        manufacturer_account_id,
        product_code,
        title
    from
    (
        select
            valid_from,
            update_date,
            delete_date,
            product_id,
            manufacturer_account_id,
            product_code,
            title,
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

products_with_history as (
    select
        products.product_id,
        products.title,
        products.valid_from,
        -- Offerv2 can have upto 50 years and Offerv3 is upto 5 years of past date
        case
            when lag(products.valid_from) over (partition by products.product_id order by products.valid_from asc) is null and products.valid_from < cast('2021-04-01' as timestamp)
                then date_add('Day', -3857, products.valid_from)
            -- 3827 is the longest delay between acceptance_date of an agreement and the product
            -- we are keeping 3857 as a consistency between the offers and products
            when lag(products.valid_from) over (partition by products.product_id order by products.valid_from asc) is null and products.valid_from >= cast('2021-04-01' as timestamp)
                then date_add('Day', -2190, products.valid_from)
            --after 2021 for the two offers we need to adjust for 2 more years
            else products.valid_from end as valid_from_adjusted,
        coalesce(
                lead(products.valid_from) over (partition by products.product_id order by products.valid_from asc),
                cast('2999-01-01 00:00:00' as timestamp)
            ) as valid_to,
        products.product_code,
        products.manufacturer_account_id,
        accounts.aws_account_id manufacturer_aws_account_id,
        accounts.encrypted_account_id as manufacturer_encrypted_account_id

    from
        products_with_uni_temporal_data products
        left join accounts_with_history as accounts on
            products.manufacturer_account_id = accounts.account_id and
            products.valid_from >= accounts.valid_from and
            products.valid_from < accounts.valid_to
),

-- A usage_feed_id has several valid_from dates (each representing a usage revision),
-- but because of bi-temporality, each usage_feed_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
usage_with_uni_temporal_data as (
    select
        cast(valid_from as timestamp) as valid_from,
        from_iso8601_timestamp(update_date) as update_date,
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
        cast(estimated_revenue_in_pricing_currency as double) as estimated_revenue
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
            case when estimated_revenue_in_pricing_currency = '' then null else estimated_revenue_in_pricing_currency end as estimated_revenue_in_pricing_currency,
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

usage_line_items as (
    select
        usage_feed_id,
        valid_from,
        coalesce(
            lead(valid_from) over (partition by usage_feed_id order by valid_from asc),
            cast('2999-01-01 00:00:00' as timestamp))
        as valid_to,
        end_user_account_id,
        payer_account_id,
        usage_date,
        resource_type,
        usage_unit_types,
        currency,
        usage_units,
        estimated_revenue,
        region,
        agreement_id,
        product_id
    FROM
        usage_with_uni_temporal_data
),

usage_line_items_with_agreement as (
    SELECT
        usage_lines.usage_feed_id,
        agreement.valid_from as agreement_valid_from,
        usage_lines.valid_from,
        usage_lines.end_user_account_id,
        usage_lines.payer_account_id,
        agreement.acceptor_account_id AS subscriber_account_id,
        usage_lines.usage_date,
        usage_lines.resource_type,
        usage_lines.usage_unit_types,
        usage_lines.currency,
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
        agreement.offer_id,
        usage_lines.product_id
    from
        usage_line_items as usage_lines
        left join agreements_revisions_with_history as agreement on
            usage_lines.agreement_id = agreement.agreement_id and
            usage_lines.usage_date >= agreement.valid_from_adjusted and
            usage_lines.usage_date < agreement.valid_to
),

usage_line_items_with_agreement_offer as (
    select
        usage_lines.usage_feed_id,
        usage_lines.agreement_valid_from,
        usage_lines.valid_from,
        usage_lines.end_user_account_id,
        usage_lines.payer_account_id,
        usage_lines.subscriber_account_id,
        usage_lines.usage_date,
        usage_lines.resource_type,
        usage_lines.usage_unit_types,
        usage_lines.currency,
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
        offer.offer_target_with_private as offer_visibility,
        offer.cppo_flag,
        offer.name as offer_name,
        offer.opportunity_id AS resale_authorization_id,
        offer.opportunity_name AS resale_authorization_name,
        offer.opportunity_description AS resale_authorization_description,
        usage_lines.product_id
    from
        usage_line_items_with_agreement as usage_lines
        left join offers_with_history_with_target_type as offer on
            usage_lines.offer_id = offer.offer_id and
            usage_lines.usage_date >= offer.valid_from_adjusted and
            usage_lines.usage_date < offer.valid_to
),

usage_line_items_with_product as (
    select
        usage_lines.usage_feed_id,
        usage_lines.agreement_valid_from,
        usage_lines.valid_from,
        usage_lines.end_user_account_id,
        usage_lines.payer_account_id,
        usage_lines.subscriber_account_id,
        usage_lines.usage_date,
        usage_lines.resource_type,
        usage_lines.usage_unit_types,
        usage_lines.currency,
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
        products.manufacturer_aws_account_id,
        products.manufacturer_encrypted_account_id
    from
        usage_line_items_with_agreement_offer as usage_lines
        left join products_with_history as products on
            usage_lines.product_id = products.product_id and
            usage_lines.usage_date >= products.valid_from_adjusted and
            usage_lines.usage_date < products.valid_to
        left join legacy_products as legacy_products on
            usage_lines.product_id = legacy_products.new_id
),

usage_line_items_with_payer_account as (
    select
        usage_lines.usage_feed_id,
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
        usage_lines.resource_type,
        usage_lines.usage_unit_types,
        usage_lines.currency,
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
        usage_lines.manufacturer_encrypted_account_id
    from
        usage_line_items_with_product as usage_lines
        left join accounts_with_history_with_company_name as payer_account on
            usage_lines.payer_account_id = payer_account.account_id and
            usage_lines.usage_date >= payer_account.valid_from_adjusted and
            usage_lines.usage_date < payer_account.valid_to
),

usage_line_items_with_end_user_account as (
    select
        usage_lines.usage_feed_id,
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
        usage_lines.resource_type,
        usage_lines.usage_unit_types,
        usage_lines.currency,
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
        usage_lines.manufacturer_encrypted_account_id
    from
        usage_line_items_with_payer_account as usage_lines
        left join accounts_with_history_with_company_name as end_user_account on
            usage_lines.end_user_account_id = end_user_account.account_id and
            usage_lines.usage_date >= end_user_account.valid_from_adjusted and
            usage_lines.usage_date < end_user_account.valid_to
),

usage_line_items_with_subscriber_account as (
    select
        usage_lines.usage_feed_id,
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
        usage_lines.resource_type,
        usage_lines.usage_unit_types,
        usage_lines.currency,
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
        usage_lines.manufacturer_encrypted_account_id
    from
        usage_line_items_with_end_user_account as usage_lines
        left join accounts_with_history_with_company_name as subscriber_account on
            usage_lines.subscriber_account_id = subscriber_account.account_id and
            usage_lines.usage_date >= subscriber_account.valid_from_adjusted and
            usage_lines.usage_date < subscriber_account.valid_to
),

usage_line_items_enriched_with_address as (
    select
        usage_lines.usage_feed_id,
        usage_lines.agreement_valid_from,

        usage_lines.end_user_account_id,
        usage_lines.end_user_aws_account_id,
        usage_lines.end_user_encrypted_account_id,
        usage_lines.end_user_address_id,
        end_user_address.company_name AS end_user_company_name,
        end_user_address.email_domain AS end_user_email_domain,
        end_user_address.city AS end_user_city,
        end_user_address.state_or_region AS end_user_state_or_region,
        end_user_address.country_code AS end_user_country,
        end_user_address.postal_code AS end_user_postal_code,

        usage_lines.payer_account_id,
        usage_lines.payer_aws_account_id,
        usage_lines.payer_encrypted_account_id,
        usage_lines.payer_address_id,
        payer_address.company_name AS payer_company_name,
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
        subscriber_address.company_name AS subscriber_company_name,
        subscriber_address.email_domain AS subscriber_email_domain,
        subscriber_address.city AS subscriber_city,
        subscriber_address.state_or_region AS subscriber_state_or_region,
        subscriber_address.country_code AS subscriber_country,
        subscriber_address.postal_code AS subscriber_postal_code,

        usage_lines.usage_date,
        usage_lines.resource_type,
        usage_lines.usage_unit_types,
        usage_lines.currency,
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
        usage_lines.offer_id,
        usage_lines.offer_visibility,
        usage_lines.cppo_flag,
        usage_lines.offer_name,
        usage_lines.resale_authorization_id,
        usage_lines.resale_authorization_name,
        usage_lines.resale_authorization_description,
        CASE
            WHEN usage_lines.proposer_account_id = usage_lines.manufacturer_account_id
             THEN NULL
            ELSE proposer_account.aws_account_id
        END AS reseller_aws_account_id,
        CASE
            WHEN usage_lines.proposer_account_id = usage_lines.manufacturer_account_id
                THEN NULL
            ELSE proposer_account.encrypted_account_id
            END AS reseller_encrypted_account_id,
        CASE
            WHEN usage_lines.proposer_account_id = usage_lines.manufacturer_account_id
                THEN NULL
            ELSE proposer_account.mailing_company_name
        END AS reseller_company_name,
        usage_lines.product_code,
        usage_lines.manufacturer_account_id,
        usage_lines.manufacturer_aws_account_id,
        usage_lines.manufacturer_encrypted_account_id,
        manufacturer_account.mailing_company_name AS manufacturer_company_name
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
        left join address_with_latest_revision as subscriber_tax_address on
            usage_lines.subscriber_tax_address_id = subscriber_tax_address.address_id
        left join address_with_latest_revision as subscriber_mailing_address on
            usage_lines.subscriber_mailing_address_id = subscriber_mailing_address.address_id
        left join address_with_latest_revision as subscriber_address on
            usage_lines.subscriber_address_id = subscriber_address.address_id
        left join address_with_latest_revision as end_user_tax_address on
            usage_lines.end_user_tax_address_id = end_user_tax_address.address_id
        left join address_with_latest_revision as end_user_mailing_address on
            usage_lines.end_user_mailing_address_id = end_user_mailing_address.address_id
),

usage_report as (
    select
        usage_lines.usage_date,
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
            WHEN usage_lines.agreement_id IS NULL THEN 'Not available'
            ELSE usage_lines.offer_id
        END AS offer_id,
        CASE
            WHEN usage_lines.agreement_id IS NULL THEN 'Not available'
            WHEN usage_lines.offer_name IS NULL AND usage_lines.offer_visibility = 'Public' THEN 'Not applicable'
            ELSE usage_lines.offer_name
        END AS offer_name,
        CASE
            WHEN usage_lines.agreement_id IS NULL THEN 'Not available'
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
        -----------
        -- Usage --
        -----------
        usage_lines.resource_type as dimension_key,
        usage_lines.region,
        usage_lines.usage_units as estimated_usage_units,
        usage_lines.usage_unit_types,
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
            WHEN '276720623826' <> usage_lines.manufacturer_aws_account_id AND usage_lines.reseller_aws_account_id IS NULL THEN 'Not applicable'
            ELSE usage_lines.reseller_company_name
        END AS reseller_company_name,
        CASE
            WHEN usage_lines.reseller_aws_account_id IS NULL AND usage_lines.resale_authorization_id IS NULL THEN 'Not applicable'
            WHEN '276720623826' <> usage_lines.manufacturer_aws_account_id AND usage_lines.reseller_aws_account_id IS NULL THEN 'Not applicable'
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

        coalesce(usage_lines.manufacturer_company_name, 'Not available') AS isv_company_name,
        usage_lines.manufacturer_aws_account_id AS isv_aws_account_id
    from
        usage_line_items_enriched_with_address as usage_lines
        left join agreement_with_all_accepted_term_types as terms on
            usage_lines.agreement_id = terms.agreement_id and
            usage_lines.agreement_valid_from = terms.revision_creation_time
)

select *
from usage_report
where usage_date >= date_add('DAY', -90, current_date)
