-- Agreement report

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

address_with_uni_temporal_data as (
    select
        from_iso8601_timestamp(valid_from) as valid_from,
        address_id,
        company_name,
        email_domain,
        country_code,
        state_or_region,
        city,
        postal_code,
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
            -- 212 is the longest delay between acceptance_date of the agreement and the account start_Date in problem we identified as of Aug 04,2023
            else awh.valid_from
        end as valid_from_adjusted,
        awh.valid_to
    from accounts_with_history as awh
    left join address_with_latest_revision as address on
        awh.mailing_address_id = address.address_id and awh.mailing_address_id is not null
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
        product_id,
        title,
        valid_from,
        -- Offerv2 can have upto 50 years and Offerv3 is upto 5 years of past date
        case
            when lag(valid_from) over (partition by product_id order by valid_from asc) is null and valid_from < cast('2021-04-01' as timestamp)
                then date_add('Day', -3857, valid_from)
            -- 3827 is the longest delay between acceptance_date of an agreement and the product
            -- we are keeping 3857 as a consistency between the offers and products
            when lag(valid_from) over (partition by product_id order by valid_from asc) is null and valid_from >= cast('2021-04-01' as timestamp)
                then date_add('Day', -2190, valid_from)
            --after 2021 for the two offers we need to adjust for 2 more years
            else valid_from end as valid_from_adjusted,
        coalesce(
                lead(valid_from) over (partition by product_id order by valid_from asc),
                cast('2999-01-01 00:00:00' as timestamp)
            ) as valid_to,
        product_code,
        manufacturer_account_id
    from
        products_with_uni_temporal_data
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
        from_iso8601_timestamp(start_time) as start_time,
        from_iso8601_timestamp(end_time) as end_time,
        from_iso8601_timestamp(acceptance_time) as acceptance_time,
        intent,
        preceding_agreement_id,
        status,
        status_reason_code,
        estimated_agreement_value,
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
            cast(estimated_agreement_value as double) as estimated_agreement_value,
            currency_code,
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

agreements_revisions_with_current_revision as (
    with agreement_with_status_group as (
        select
        proposer_account_id,
        acceptor_account_id,
        agreement_id,
        offer_id,
        status,
        status_reason_code,
        start_time,
        end_time,
        valid_from,
        acceptance_time,
        preceding_agreement_id,
        intent,
        currency_code,
        cast(estimated_agreement_value as double) estimated_agreement_value,
        case
            when status = 'SUPERSEDED' then 'SUPERSEDED'   -- alphabetic max
            when status = 'ROLLED_BACK' then 'ROLLED_BACK'
            else 'EFFECTIVE'                               -- alphabetic min
        end as status_group_alt,
        max (case when status = 'SUPERSEDED' then 'SUPERSEDED' when status = 'ROLLED_BACK' then 'ROLLED_BACK' else 'EFFECTIVE' end) over(partition by agreement_id) as status_group_max,
        min (case when status = 'SUPERSEDED' then 'SUPERSEDED' when status = 'ROLLED_BACK' then 'ROLLED_BACK' else 'EFFECTIVE' end) over(partition by agreement_id) as status_group_min,
        rank() over(partition by agreement_id, (case when status = 'SUPERSEDED' then 'SUPERSEDED' when status = 'ROLLED_BACK' then 'ROLLED_BACK' else 'EFFECTIVE' end) order by valid_from desc) rnk_by_status_group
        from
        agreements_with_uni_temporal_data
)
select
     proposer_account_id,
     acceptor_account_id,
     agreement_id,
     offer_id,
     status,
     status_reason_code,
     start_time,
     end_time,
     valid_from,
     acceptance_time,
     preceding_agreement_id,
     intent,
     currency_code,
     estimated_agreement_value
 from agreement_with_status_group
 where
    -- If all statuses are ROLLED_BACK, then choose the most recent revision (by revision_creation_time):
     (
             status_group_max = 'ROLLED_BACK' and status_group_min = 'ROLLED_BACK' and rnk_by_status_group = 1
         )
    -- If all statuses are SUPERSEDED, then choose the most recent revision (by revision_creation_time):
    or (
             status_group_max = 'SUPERSEDED' and status_group_min = 'SUPERSEDED' and rnk_by_status_group = 1
     )
    -- If all statuses are either SUPERSEDED or ROLLED_BACK, then choose the most recent ROLLED_BACK revision (by revision_creation_time):
    or (
             status_group_max = 'SUPERSEDED' and status_group_min = 'ROLLED_BACK' and status = 'ROLLED_BACK' and rnk_by_status_group = 1
     )
    or (
             status_group_min = 'EFFECTIVE' and status not in ('ROLLED_BACK','SUPERSEDED') and rnk_by_status_group = 1
     )
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
        offer_id,
        offer_revision,
        name,
        opportunity_id,
        opportunity_name,
        opportunity_description,
        seller_account_id,
        valid_from,
        -- When we try to look up an offer revision as at the acceptance date of a BYOL agreement, we run into a problem.
        -- For BYOL, the agreement might be accepted (using some external non-AWS system or manual process) days before
        -- that BYOL agreement is entered into AWS Marketplace by the buyer. Therefore, the buyer is permitted to manually
        -- enter a backdated acceptance date, which might predate the point in time when the first revision of the offer
        -- was created. To work around this, we need to adjust the valid_from on the first revision of the offer to be
        -- earlier than the earliest possible backdated BYOL agreement acceptance date.
        case
            when lag(valid_from) over (partition by offer_id order by valid_from asc) is null and valid_from < cast('2021-04-01' as timestamp)
            then date_add('Day', -3857, valid_from)
            -- 3857 is the longest delay between acceptance_date of an agreement and the first revision of the offer
            when lag(valid_from) over (partition by offer_id order by valid_from asc) is null and valid_from >= cast('2021-04-01' as timestamp)
            then date_add('Day', -1460, valid_from)
            --after 2021 for the two offers we need to adjust for 2 more years
            else valid_from
        end as valid_from_adjusted,
        coalesce(
            lead(valid_from) over (partition by offer_id order by valid_from asc),
            cast('2999-01-01 00:00:00' as timestamp))
        as valid_to
    from offers_with_uni_temporal_data
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
        -- even though today it is not possible to combine several types of targeting in a single offer, let's ensure the query is still predictable if this gets possible in the future
        max(
            case
                when off_tgt.offer_target is null then 'Public'
                else off_tgt.offer_target
            end
        ) as offer_target,
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

agreements_revisions_offer as (

with data as (
select
    al.valid_from,
    al.agreement_id,
    al.offer_id,
    al.proposer_account_id,
    al.acceptor_account_id,
    al.status,
    al.status_reason_code,
    al.start_time,
    al.end_time,
    al.acceptance_time,
    al.preceding_agreement_id,
    al.intent,
    ao.offer_id as previous_offer_id,
    offer.offer_revision,
    offer.offer_target,
    case when offer.name = '' then null else offer.name end as offer_name,
    offer.opportunity_id,
    offer.opportunity_name,
    offer.opportunity_description,
    offer.seller_account_id,
    ar.agreement_id as next_agreement_id,
    ar.offer_id as next_offer_id,
    ar.acceptance_time as next_agreement_acceptance_time,
    al.estimated_agreement_value as total_contract_value,
    al.currency_code

from agreements_revisions_with_current_revision as al
left join agreements_revisions_with_current_revision as ar on ar.preceding_agreement_id = al.agreement_id
left join agreements_revisions_with_current_revision as ao on al.preceding_agreement_id = ao.agreement_id
left join offers_with_history_with_target_type as offer on al.offer_id = offer.offer_id  and al.acceptance_time >= offer.valid_from_adjusted and al.acceptance_time < offer.valid_to

)
select
    valid_from,
    agreement_id,
    offer_id,
    proposer_account_id,
    acceptor_account_id,
    status,
    status_reason_code,
    start_time,
    end_time,
    acceptance_time,
    preceding_agreement_id,
    intent,
    previous_offer_id,
    offer_revision,
    offer_target,
    offer_name,
    opportunity_id,
    opportunity_name,
    opportunity_description,
    seller_account_id,
    listagg(next_agreement_id,',') within group (order by acceptance_time ) as next_agreement_id,
    listagg(next_offer_id,',') within group (order by acceptance_time ) as next_offer_id,
    listagg(cast(next_agreement_acceptance_time as VARCHAR),',') within group (order by acceptance_time) as next_agreement_acceptance_time,
    total_contract_value,
    currency_code
from
    data
group by
    valid_from,
    agreement_id,
    offer_id,
    proposer_account_id,
    acceptor_account_id,
    status,
    status_reason_code,
    start_time,
    end_time,
    acceptance_time,
    preceding_agreement_id,
    intent,
    previous_offer_id,
    offer_revision,
    offer_target,
    offer_name,
    opportunity_id,
    opportunity_name,
    opportunity_description,
    seller_account_id,
    total_contract_value,
    currency_code
),

offer_product_with_uni_temporal_data as (
    select
        from_iso8601_timestamp(valid_from) as valid_from,
        from_iso8601_timestamp(update_date) as update_date,
        from_iso8601_timestamp(delete_date) as delete_date,
        product_id,
        offer_id,
        offer_revision
    from
    (
        select
            valid_from,
            update_date,
            delete_date,
            product_id,
            offer_id,
            offer_revision,
            row_number() over (partition by offer_id, valid_from order by from_iso8601_timestamp(update_date) desc) as row_num
        from
            offerproductfeed_v1
    )
    where
        -- keep latest ...
        row_num = 1
        -- ... and remove the soft-deleted one.
        and (delete_date is null or delete_date = '')
),

offer_product_with_history as (

select
    offer_id,
    offer_revision,
    product_id,
    valid_from,
    coalesce(lead(valid_from) over (partition by offer_id,offer_revision order by valid_from asc),timestamp '2999-01-01 00:00:00') as valid_to
from
       offer_product_with_uni_temporal_data as offer_product

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

agreements_revisions_offer_product_with_history as (

select
    agreements_offer.status,
    agreements_offer.status_reason_code,
    agmt_offer_prod.product_id,
    legacy_prod.legacy_id as legacy_product_id,
    agreements_offer.offer_name,
    agreements_offer.offer_id,
    agreements_offer.offer_target as offer_visibility,
    agreements_offer.agreement_id,
    agreements_offer.valid_from,
    agreements_offer.start_time as agreement_start_date,
    agreements_offer.acceptance_time as agreement_acceptance_date,
    agreements_offer.end_time as agreement_end_date,
    agreements_offer.intent as agreement_intent,
    agreements_offer.preceding_agreement_id as previous_agreement_id,
    agreements_offer.next_agreement_id,
    agreements_offer.next_agreement_acceptance_time as next_agreement_acceptance_date,
    agreements_offer.previous_offer_id,
    agreements_offer.next_offer_id,
    agreements_offer.opportunity_id as resale_authorization_id,
    agreements_offer.opportunity_name as resale_authorization_name,
    agreements_offer.opportunity_description as resale_authorization_description,
    agreements_offer.total_contract_value,
    agreements_offer.currency_code,
    agreements_offer.acceptor_account_id,
    agreements_offer.proposer_account_id,
    agreements_offer.offer_revision
from agreements_revisions_offer as agreements_offer
left join offer_product_with_history as agmt_offer_prod on
       agreements_offer.offer_id = agmt_offer_prod.offer_id
       and agreements_offer.offer_revision = agmt_offer_prod.offer_revision
left join legacy_products as legacy_prod on
       agmt_offer_prod.product_id = legacy_prod.new_id

),
agreements_revisions_offer_product as (

select
    agmt_offer_pwh.valid_from,
    coalesce(agmt_offer_pwh.legacy_product_id, agmt_offer_pwh.product_id) as product_id,
    product_with_hist.product_code,
    product_with_hist.title as product_title,
    agmt_offer_pwh.legacy_product_id,
    agmt_offer_pwh.offer_name,
    agmt_offer_pwh.offer_id,
    case when agmt_offer_pwh.proposer_account_id <> product_with_hist.manufacturer_account_id then 'Private' else agmt_offer_pwh.offer_visibility end as offer_visibility,
    agmt_offer_pwh.agreement_id,
    agmt_offer_pwh.agreement_start_date,
    agmt_offer_pwh.agreement_acceptance_date,
    agmt_offer_pwh.agreement_end_date,
    agmt_offer_pwh.agreement_intent,
    agmt_offer_pwh.previous_agreement_id,
    agmt_offer_pwh.next_agreement_id,
    agmt_offer_pwh.next_agreement_acceptance_date,
    agmt_offer_pwh.previous_offer_id,
    agmt_offer_pwh.next_offer_id,
    agmt_offer_pwh.resale_authorization_id,
    agmt_offer_pwh.resale_authorization_name,
    agmt_offer_pwh.resale_authorization_description,
    agmt_offer_pwh.acceptor_account_id,
    agmt_offer_pwh.proposer_account_id,
    agmt_offer_pwh.offer_revision,
    agmt_offer_pwh.status,
    agmt_offer_pwh.status_reason_code,
    agmt_offer_pwh.total_contract_value,
    agmt_offer_pwh.currency_code,
    acc_reseller.aws_account_id as proposer_aws_account_id,
    acc_reseller.encrypted_account_id as proposer_encrypted_account_id,
    case when acc_reseller.mailing_company_name = '' then null else acc_reseller.mailing_company_name end as proposer_company_name,
    acc_acceptor.aws_account_id as subscriber_aws_account_id,
    acc_acceptor.encrypted_account_id as subscriber_encrypted_account_id,
    case when acc_acceptor.mailing_company_name = '' then null else acc_acceptor.mailing_company_name end as subscriber_company_name,
    coalesce(acc_acceptor.tax_address_id, acc_acceptor.mailing_address_id) subscriber_address_id,
    case when agmt_offer_pwh.proposer_account_id <> product_with_hist.manufacturer_account_id then acc_reseller.mailing_company_name else null end as reseller_company_name,
    case when agmt_offer_pwh.proposer_account_id <> product_with_hist.manufacturer_account_id then acc_reseller.aws_account_id else null end as reseller_aws_account_id,
    case when agmt_offer_pwh.proposer_account_id <> product_with_hist.manufacturer_account_id then acc_reseller.encrypted_account_id else null end as reseller_encrypted_account_id,
    product_with_hist.manufacturer_account_id,
    acc_products.aws_account_id as isv_aws_account_id,
    null as isv_encrypted_account_id,
    null as isv_company_name
from agreements_revisions_offer_product_with_history as agmt_offer_pwh
left join products_with_history as product_with_hist on
    agmt_offer_pwh.product_id = product_with_hist.product_id  and agmt_offer_pwh.agreement_acceptance_date >= product_with_hist.valid_from_adjusted and agmt_offer_pwh.agreement_acceptance_date < product_with_hist.valid_to
left join accounts_with_history_with_company_name as acc_acceptor on
    agmt_offer_pwh.acceptor_account_id = acc_acceptor.account_id and agmt_offer_pwh.agreement_acceptance_date >= acc_acceptor.valid_from_adjusted and agmt_offer_pwh.agreement_acceptance_date < acc_acceptor.valid_to
left join accounts_with_history_with_company_name as acc_reseller on
    agmt_offer_pwh.proposer_account_id = acc_reseller.account_id and agmt_offer_pwh.agreement_acceptance_date >= acc_reseller.valid_from_adjusted and agmt_offer_pwh.agreement_acceptance_date < acc_reseller.valid_to
left join accounts_with_history_with_company_name as acc_products on
    product_with_hist.manufacturer_account_id = acc_products.account_id and agmt_offer_pwh.agreement_acceptance_date >= acc_products.valid_from and agmt_offer_pwh.agreement_acceptance_date < acc_products.valid_to

),
agreement_revision_final_report as (

with main_query as (
select
    agg.valid_from,
    agg.status,
    agg.status_reason_code,
    agg.product_title,
    agg.product_id,
    agg.product_code,
    agg.legacy_product_id,
    agg.offer_name,
    agg.offer_id,
    agg.offer_revision,
    agg.offer_visibility,
    agg.agreement_id,
    agg.agreement_start_date,
    agg.agreement_acceptance_date,
    agg.agreement_end_date,
    agg.agreement_intent,
    agg.previous_agreement_id,
    agg.next_agreement_id,
    agg.previous_offer_id,
    agg.next_offer_id,
    agg.next_agreement_acceptance_date,
    agg.proposer_aws_account_id,
    agg.proposer_encrypted_account_id,
    agg.proposer_company_name,
    agg.subscriber_aws_account_id,
    agg.subscriber_encrypted_account_id,
    agg.subscriber_company_name,
    acc.email_domain          as subscriber_email_domain,
    acc.city                  as subscriber_city,
    acc.state_or_region       as subscriber_state_or_region,
    acc.country_code          as subscriber_country,
    acc.postal_code as subscriber_postal_code,
    agg.resale_authorization_id,
    agg.resale_authorization_name,
    agg.resale_authorization_description,
    agg.reseller_company_name,
    agg.reseller_aws_account_id,
    agg.reseller_encrypted_account_id,
    acc_isv.mailing_company_name as isv_company_name,
    agg.isv_aws_account_id,
    agg.isv_encrypted_account_id,
    agg.total_contract_value,
    agg.currency_code
from agreements_revisions_offer_product as agg
left join address_with_latest_revision as acc on agg.subscriber_address_id = acc.address_id
left join accounts_with_history_with_company_name as acc_isv on
 agg.isv_aws_account_id = acc_isv.aws_account_id and agg.agreement_acceptance_date >= acc_isv.valid_from_adjusted and agg.agreement_acceptance_date < acc_isv.valid_to

)
select
    --Product information

    coalesce(product_title, 'Not provided') as product_title,
    coalesce(product_id, 'Not provided') as product_id,
    coalesce(product_code, 'Not provided') as product_code,
    legacy_product_id,
    null as product_type,
    --Procurement information

    case
        when offer_name is null and offer_visibility = 'Public' then 'Not applicable'
        else offer_name
        end as offer_name,
    offer_id,
    case
        when offer_visibility is null then 'Public'
        else offer_visibility
        end as offer_visibility,
    offer_revision,
    --Agreement information

    agreement_id,
    status,
    status_reason_code,
    agreement_start_date,
    agreement_acceptance_date,
    agreement_end_date,
    valid_from as agreement_updated_date,
    agreement_intent,
    coalesce(previous_agreement_id, case when agreement_intent is null or agreement_intent = 'NEW' then 'Not applicable' else 'Not available' end) as previous_agreement_id,
    coalesce(next_agreement_id, case when agreement_intent is null or agreement_intent = 'NEW' then 'Not applicable' else 'Not available' end) as next_agreement_id,
    coalesce(previous_offer_id, case when agreement_intent is null or agreement_intent = 'NEW' then 'Not applicable' else 'Not available' end) as previous_offer_id,
    coalesce(next_offer_id, case when agreement_intent is null or agreement_intent = 'NEW' then 'Not applicable' else 'Not available' end) as next_offer_id,
    coalesce(next_agreement_acceptance_date, case when agreement_intent is null or agreement_intent = 'NEW' then 'Not applicable' else 'Not available' end) as next_agreement_acceptance_date,
    'PurchaseAgreement' as agreement_type,
    proposer_aws_account_id,
    proposer_encrypted_account_id,
    -- replace null proposer_company_name with Not available
    coalesce(proposer_company_name, 'Not available') as proposer_company_name,

    --Subscriber Information

    subscriber_aws_account_id,
    subscriber_encrypted_account_id,
    case
        when subscriber_company_name is null then 'Not provided'
        else subscriber_company_name
        end as subscriber_company_name,
    case
        when subscriber_email_domain is null then 'Not provided'
        else subscriber_email_domain
        end as subscriber_email_domain,
    case
        when subscriber_city is null then 'Not provided'
        else subscriber_city
        end as subscriber_city,
    case
        when subscriber_state_or_region is null then 'Not provided'
        else subscriber_state_or_region
        end as subscriber_state_or_region,
    case
        when subscriber_country is null then 'Not provided'
        else subscriber_country
        end as subscriber_country,
    case
        when subscriber_postal_code is null then 'Not provided'
        else subscriber_postal_code
        end as subscriber_postal_code,

    --Resale information

    case
        when resale_authorization_id is null then
            case
                when offer_visibility = 'Public' then 'Not applicable'
                when offer_visibility is null then 'Not applicable'
                else null
                end
        else resale_authorization_id
        end as resale_authorization_id,
    case
        when resale_authorization_name is null then
            case
                when offer_visibility = 'Public' then 'Not applicable'
                when offer_visibility is null then 'Not applicable'
                else null
                end
        else resale_authorization_name
        end as resale_authorization_name,
    case
        when resale_authorization_description is null then
            case
                when offer_visibility = 'Public' then 'Not applicable'
                when offer_visibility is null then 'Not applicable'
                else null
                end
        else resale_authorization_description
        end as resale_authorization_description,
    case
        when reseller_aws_account_id is not null and reseller_company_name is null then 'Not available'
        when reseller_aws_account_id is null and resale_authorization_id is null then 'Not applicable'
        when reseller_aws_account_id is null then 'Not applicable'
        else reseller_company_name
        end as reseller_company_name,
    case
        when reseller_aws_account_id is null and resale_authorization_id is null then 'Not applicable'
        when reseller_aws_account_id is null then 'Not applicable'
        else reseller_aws_account_id
        end as reseller_aws_account_id,
    reseller_encrypted_account_id,
    coalesce(case when isv_company_name = '' then null else isv_company_name end, 'Not available') as isv_company_name,
    isv_aws_account_id,
    isv_encrypted_account_id,
    case
        when resale_authorization_id is null and isv_aws_account_id != proposer_aws_account_id then 'Y'
        when resale_authorization_id is not null then 'Y'
        else 'N'
        end as cppo_flag,
    total_contract_value,
    currency_code
from
    main_query
)
select
      agreement_id
     ,next_agreement_acceptance_date
     ,previous_offer_id
     ,subscriber_email_domain
     ,reseller_company_name
     ,reseller_encrypted_account_id
     ,isv_aws_account_id
     ,product_code
     ,subscriber_country
     ,currency_code
     ,previous_agreement_id
     ,agreement_type
     ,resale_authorization_description
     ,cppo_flag
     ,product_id
     ,proposer_encrypted_account_id
     ,isv_company_name
     ,resale_authorization_id
     ,next_offer_id
     ,total_contract_value
     ,legacy_product_id
     ,agreement_acceptance_date
     ,isv_encrypted_account_id
     ,subscriber_aws_account_id
     ,reseller_aws_account_id
     ,product_title
     ,offer_name
     ,offer_id
     ,agreement_end_date
     ,resale_authorization_name
     ,proposer_aws_account_id
     ,agreement_start_date
     ,agreement_intent
     ,product_type
     ,proposer_company_name
     ,subscriber_postal_code
     ,subscriber_company_name
     ,subscriber_city
     ,next_agreement_id
     ,subscriber_state_or_region
     ,subscriber_encrypted_account_id
     ,offer_visibility
     ,status
from agreement_revision_final_report
where agreement_start_date >= date_add('DAY', -90, current_date)
