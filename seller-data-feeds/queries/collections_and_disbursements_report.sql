-- Collections and disbursements report

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
    origin_offer_id,
    proposer_account_id,
    acceptor_account_id,
    agreement_revision,
    from_iso8601_timestamp(valid_from) as valid_from,
    from_iso8601_timestamp(start_date) as start_date,
    from_iso8601_timestamp(end_date) as end_date,
    from_iso8601_timestamp(acceptance_date) as acceptance_date,
    agreement_type,
    previous_agreement_id,
    agreement_intent
  from
  (
    select
      --empty value in Athena shows as '', change all '' value to null
      case when agreement_id = '' then null else agreement_id end as agreement_id,
      origin_offer_id,
      proposer_account_id,
      acceptor_account_id,
      agreement_revision,
      valid_from,
      delete_date,
      start_date,
      end_date,
      acceptance_date,
      agreement_type,
      previous_agreement_id,
      agreement_intent,
      row_number() over (partition by agreement_id, valid_from order by from_iso8601_timestamp(update_date) desc) as row_num
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

agreements_with_history as (
  with agreements_with_window_functions as (
    select
      agreement_id,
      origin_offer_id as offer_id,
      proposer_account_id,
      acceptor_account_id,
      agreement_revision,
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
      agreement_type,
      previous_agreement_id,
      agreement_intent
    from
      agreements_with_uni_temporal_data
  )
  select
    agreement_id,
    offer_id,
    proposer_account_id,
    acceptor_account_id,
    agreement_revision,
    start_date,
    end_date,
    acceptance_date,
    valid_from,
    case
        when version=1 and valid_from < timestamp '2023-03-03 06:16:08.743' then timestamp '1970-01-01'
        -- The following 60 minute adjustment is to handle special case where When Renewal happens for a contract
        when version=1 then date_add('minute',-60,valid_from)
        else valid_from
    end as valid_from_adjusted,
    valid_to,
    agreement_type,
    previous_agreement_id,
    agreement_intent
  from
    agreements_with_window_functions
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
      opportunity_id,
      opportunity_name,
      opportunity_description,
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
-- provided for reference only if you are interested into get "current" offer name
-- (ie. not used afterwards)
offers_with_latest_revision as (
  select
    offer_id,
    offer_revision,
    name,
    opportunity_name,
    opportunity_description,
    valid_from,
    null valid_to
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
      row_number() over (partition by offer_id order by valid_from desc) as row_num_latest_revision
    from
      offers_with_uni_temporal_data
  )
  where
    row_num_latest_revision = 1
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
    offer.opportunity_id
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
    offer.opportunity_id
),

-- provided for reference only if you are interested into get "current" offer targets
-- (ie. not used afterwards)
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
    left join offer_targets_with_uni_temporal_data off_tgt on
      offer.offer_id=off_tgt.offer_id and offer.offer_revision=off_tgt.offer_revision
  group by
    offer.offer_id,
    offer.offer_revision,
    -- redundant with offer_revision, as each revision has a dedicated valid_from (but cleaner in the group by)
    offer.valid_from,
    offer.valid_to
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
      else valid_from 
    end as valid_from_adjusted,
    coalesce(
      lead(valid_from) over (partition by product_id order by valid_from asc),
      cast('2999-01-01 00:00:00' as timestamp)
    ) as valid_to,
    product_code,
    manufacturer_account_id
  from
    products_with_uni_temporal_data
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

-- A given billing_event_id represents an accounting event and thus has only one valid_from date,
-- but because of bi-temporality, a billing_event_id (+ its valid_from) can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
billing_events_with_uni_temporal_data as (
  select
    billing_event_id,
    valid_from,
    update_date,
    delete_date,
    invoice_date,
    transaction_type,
    transaction_reference_id,
    parent_billing_event_id,
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
    --empty value in Athena shows as '', change all '' value to null
    case when agreement_id = '' then null else agreement_id end as agreement_id,
    invoice_id,
    payment_due_date,
    usage_period_start_date,
    usage_period_end_date,
    buyer_transaction_reference_id,
    row_num
  from
  (
    select
      billing_event_id,
      from_iso8601_timestamp(valid_from) as valid_from,
      from_iso8601_timestamp(update_date) as update_date,
      delete_date,
      from_iso8601_timestamp(invoice_date) as invoice_date,
      transaction_type,
      transaction_reference_id,
      parent_billing_event_id,
      -- casting in case data was imported as number
      cast(bank_trace_id as varchar) as bank_trace_id,
      broker_id,
      product_id,
      disbursement_billing_event_id,
      action,
      from_account_id,
      to_account_id,
      end_user_account_id,
      billing_address_id,
      -- casting in case data was imported as varchar
      cast(amount as decimal(38,6)) as amount,
      currency,
      balance_impacting,
      agreement_id,
      invoice_id,
      case when payment_due_date is null or payment_due_date = '' then null else from_iso8601_timestamp(payment_due_date) end as payment_due_date,
      from_iso8601_timestamp(usage_period_start_date) as usage_period_start_date,
      from_iso8601_timestamp(usage_period_end_date) as usage_period_end_date,
      buyer_transaction_reference_id,
      row_number() over (partition by billing_event_id, valid_from order by from_iso8601_timestamp(update_date) desc) as row_num
    from
      billingeventfeed_v1
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
    billing_events_with_uni_temporal_data bill
  where
    -- Assumption here is only seller will pay listing fee. As of 12/21/2021, there are cases that Channel partner have 0 listing fee for CPPO, so the amount could be 0.
    bill.transaction_type like 'AWS_REV_SHARE' and amount <= 0 and action = 'INVOICED'
  group by
    -- from_account_id is always the same for all those "listing fee" transactions == the seller of record himself.
    -- If this view returns more than 1 record, the overall query will fail (on purpose). Please contact AWS Marketplace if this happens.
    from_account_id
),

billing_event_with_business_flags as (
  select
    bl.billing_event_id,
    bl.end_user_account_id,
    bl.agreement_id,
    aggrement.proposer_account_id,
    aggrement.offer_id,
    aggrement.acceptor_account_id,
    case
      -- For AWS and BALANCE_ADJUSTMENT, the billing event feed will show the "AWS Marketplace" account as the
      -- receiver of the funds and the seller as the payer. We are not interested in this information here.
      -- Null values will be ignored by the `max` aggregation function.
      when bl.transaction_type like 'AWS%' then null
      -- For BALANCE_ADJUSTMENT, payer is seller themselves
      when bl.invoice_id is null then bl.to_account_id
      -- We get the payer of the invoice from *any* transaction type that is not AWS and not BALANCE_ADJUSTMENT (because they are the same for a given end user + agreement + product).
      else bl.from_account_id
    end as payer_account_id,
    bl.product_id,
    bl.action,
    bl.transaction_type,
    bl.parent_billing_event_id,
    bl.disbursement_billing_event_id,
    bl.amount,
    bl.currency,
    bl.balance_impacting,
    bl.invoice_date,
    bl.payment_due_date,
    bl.usage_period_start_date,
    bl.usage_period_end_date,
    bl.invoice_id,
    bl.billing_address_id,
    bl.transaction_reference_id,
    bl.buyer_transaction_reference_id,
    case when disbursement.bank_trace_id = 'EMEA_MP_TEST_TRACE_ID' then null else disbursement.bank_trace_id end as bank_trace_id,
    case when disbursement.bank_trace_id = 'EMEA_MP_TEST_TRACE_ID' then null else disbursement.invoice_date end as disbursement_date,
    disbursement.billing_event_id as disbursement_id,
    -- We will use disbursement_id_or_invoiced as part of the PK, so it cannot be null:
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when disbursement.billing_event_id = '' then null else disbursement.billing_event_id end,
      '<invoiced>') as disbursement_id_or_invoiced,
    bl.broker_id,
    case
      when bl.invoice_id is null /* transaction_type = 'BALANCE_ADJUSTMENT' */
        then (select seller_account_id from seller_account) ||':'|| cast(bl.invoice_date as varchar)
      else bl.buyer_transaction_reference_id
        ||'-'|| case when bl.agreement_id is null or bl.agreement_id = '' then ' ' else bl.agreement_id end
        ||'-'|| case when bl.end_user_account_id is null or bl.end_user_account_id = '' then ' ' else bl.end_user_account_id end
        ||'-'|| coalesce(cast(bl.usage_period_start_date as varchar),' ')
        ||'-'|| coalesce(cast(bl.usage_period_end_date as varchar),' ')
    end as internal_buyer_line_item_id,
    bl.buyer_transaction_reference_id <> bl.transaction_reference_id as is_seller_invoice,
    case when bl.transaction_type = 'SELLER_REV_SHARE' and (select seller_account_id from seller_account) <> bl.to_account_id then true else false end as is_cog,
    case when bl.transaction_type in('SELLER_REV_SHARE_CREDIT', 'SELLER_REV_SHARE_REFUND') and (select seller_account_id from seller_account) <> bl.to_account_id then true else false end as is_cog_refund,
    --TODO: replace below logic once we can create a logic the identify reseller/manufacturer without depending on agreement feed
    case when aggrement.proposer_account_id <> (select seller_account_id from seller_account) then true else false end as is_manufacturer_view_of_reseller
  from
    billing_events_with_uni_temporal_data as bl
    left join billing_events_with_uni_temporal_data as disbursement on
      disbursement.transaction_type like 'DISBURSEMENT%'
        and disbursement.action = 'DISBURSED'
        and disbursement.transaction_type IN ('DISBURSEMENT', 'DISBURSEMENT_FAILURE')
        and bl.disbursement_billing_event_id = disbursement.billing_event_id
    left join agreements_with_history as aggrement on
      bl.agreement_id = aggrement.agreement_id
        and bl.invoice_date >= aggrement.valid_from_adjusted
        and bl.invoice_date < aggrement.valid_to
    left join accounts_with_history awh on
      bl.to_account_id = awh.account_id
        and bl.invoice_date >= awh.valid_from
        and bl.invoice_date < awh.valid_to
  where
    bl.transaction_type not like 'DISBURSEMENT%' and
      (bl.agreement_id is null or bl.agreement_id = ''
      or aggrement.agreement_id is not null)
),

-- listagg function in athena does not support partitioning, grouping here and then joining to the main query
seller_invoice_list as (
  select
    internal_buyer_line_item_id,
    listagg(case when not is_seller_invoice then null else invoice_id end,',') within group (order by case when not is_seller_invoice then null else cast(invoice_date as varchar) end) as seller_invoice_id_or_null,
    listagg(case when not is_seller_invoice then null else cast(invoice_date as varchar) end,',') within group (order by case when not is_seller_invoice then null else cast(invoice_date as varchar) end) as seller_invoice_date_or_null
  from
    (
    -- listagg function in athena does not support ordering by another field when distinct is used,
    -- here we first select distinct invoices and then do the listagg order by invoice_date
    select distinct internal_buyer_line_item_id, is_seller_invoice, invoice_id, invoice_date
    from billing_event_with_business_flags) distinct_invoices
  group by internal_buyer_line_item_id
  order by internal_buyer_line_item_id
),

billing_event_with_categorized_transaction as (
-- Use the flags that were created in the previous transformation in more calculated columns:
-- NOTE: This transformation has no joins and no window functions
  select
    billing_event_id,
    end_user_account_id,
    agreement_id,
    proposer_account_id,
    offer_id,
    acceptor_account_id,
    case when is_cog or is_cog_refund then null else payer_account_id end as payer_account_id,
    product_id,
    action,
    transaction_type,
    parent_billing_event_id,
    disbursement_billing_event_id,
    amount,
    currency,
    balance_impacting,
    invoice_date,
    payment_due_date,
    usage_period_start_date,
    usage_period_end_date,
    invoice_id,
    billing_address_id,
    transaction_reference_id,
    buyer_transaction_reference_id,
    bank_trace_id,
    disbursement_date,
    disbursement_id,
    disbursement_id_or_invoiced,
    broker_id,
    bl.internal_buyer_line_item_id,
    is_seller_invoice,
    is_cog,
    is_cog_refund,
    is_manufacturer_view_of_reseller,

    -- Buyer/seller columns:
    case when is_seller_invoice then null else invoice_id end as buyer_invoice_id_or_null,
    seller_invoices.seller_invoice_id_or_null,
    case when is_seller_invoice then null else invoice_date end as buyer_invoice_date_or_null,
    seller_invoices.seller_invoice_date_or_null,

    -- Categorized amounts by transaction type:
    case when transaction_type =   'SELLER_REV_SHARE' and not is_cog then amount else 0 end as gross_revenue,
    case when transaction_type in ('SELLER_REV_SHARE_REFUND','SELLER_REV_SHARE_CREDIT') and not is_cog_refund then amount else 0 end as gross_refund,
    case when transaction_type =   'SELLER_REV_SHARE' and     is_cog then amount else 0 end as cogs,
    case when transaction_type in ('SELLER_REV_SHARE_REFUND','SELLER_REV_SHARE_CREDIT') and     is_cog_refund then amount else 0 end as cogs_refund,
    case when transaction_type =   'AWS_REV_SHARE' then amount else 0 end as aws_rev_share,
    case when transaction_type in ('AWS_REV_SHARE_REFUND','AWS_REV_SHARE_CREDIT') then amount else 0 end as aws_refund_share,
    case when transaction_type =   'AWS_TAX_SHARE' and not is_seller_invoice then amount else 0 end as aws_tax_share,             -- AWS tax share from _buyer_  invoice
    case when transaction_type =   'AWS_TAX_SHARE' and     is_seller_invoice then amount else 0 end as aws_tax_share_listing_fee, -- AWS tax share from _seller_ invoice
    case when transaction_type =   'AWS_TAX_SHARE_REFUND' and not is_seller_invoice then amount else 0 end as aws_tax_share_refund,
    case when transaction_type =   'AWS_TAX_SHARE_REFUND' and     is_seller_invoice then amount else 0 end as aws_tax_share_refund_listing_fee,
    case when transaction_type =   'SELLER_TAX_SHARE' then amount else 0 end as seller_tax_share,
    case when transaction_type =   'SELLER_TAX_SHARE_REFUND' then amount else 0 end as seller_tax_share_refund,
    case when transaction_type =   'BALANCE_ADJUSTMENT' then amount else 0 end as balance_adjustment,
    case when transaction_type =   'SELLER_REV_SHARE_CREDIT' then amount else 0 end as seller_rev_credit,
    case when transaction_type =   'AWS_REV_SHARE_CREDIT' then amount else 0 end as aws_ref_fee_credit
  from
    billing_event_with_business_flags as bl
    left join seller_invoice_list as seller_invoices
      on bl.internal_buyer_line_item_id = seller_invoices.internal_buyer_line_item_id
    ),

line_items_aggregated as (
-- This transformation has the only "group by" in all of these transformations.
-- NOTE: This transformation has no joins and no window functions
  select
    internal_buyer_line_item_id,
    disbursement_id,
    disbursement_id_or_invoiced,
    product_id,
    broker_id,
    currency,
    agreement_id,
    proposer_account_id,
    acceptor_account_id,
    max(payer_account_id) as payer_account_id,
    offer_id,
    end_user_account_id,
    usage_period_start_date,
    usage_period_end_date,
    max(payment_due_date) payment_due_date,
    buyer_transaction_reference_id,
    bank_trace_id,
    disbursement_date,
    max(billing_address_id) as billing_address_id,

    -- Buyer/seller columns:
    max(buyer_invoice_id_or_null) as buyer_invoice_id,
    max(seller_invoice_id_or_null) as seller_invoice_id,
    max(buyer_invoice_date_or_null) as buyer_invoice_date,
    max(seller_invoice_date_or_null) as seller_invoice_date,
  
    -- Categorized amounts by transaction type:
    -- When disbursement_id_or_invoiced = '<invoiced>',    these are invoiced amounts
    -- When disbursement_id_or_invoiced <> '<invoiced>' these are disbursed amounts for _this_ specific disbursement_id
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
    sum(balance_adjustment) as balance_adjustment_this_disbursement_id_or_invoiced,
    sum(seller_rev_credit) as seller_rev_credit_this_disbursement_id_or_invoiced,
    sum(aws_ref_fee_credit) as aws_ref_fee_credit_this_disbursement_id_or_invoiced
  from
    billing_event_with_categorized_transaction as billing_categorized
  group by
    internal_buyer_line_item_id,
    disbursement_id,
    disbursement_id_or_invoiced,
    broker_id,
    -- The following columns are included the in group by but they are intentionally omitted from the PK.
    -- These columns should have the _same_ values for each record in the PK.
    product_id,
    currency,
    agreement_id,
    proposer_account_id,
    acceptor_account_id,
    offer_id,
    end_user_account_id,
    usage_period_start_date,
    usage_period_end_date,
    buyer_transaction_reference_id,
    bank_trace_id,
    disbursement_date
),

-- listagg function in athena does not support partitioning, grouping here and then joining to the main query
disbursement_list as (
  select
    internal_buyer_line_item_id,
    listagg(cast(disbursement_date as varchar),',') within group (order by cast(disbursement_date as varchar)) as disbursement_date_list,
    listagg(bank_trace_id,',') within group (order by cast(disbursement_date as varchar)) as disburse_bank_trace_id_list
    from (
    -- listagg function in athena does not support ordering by another field when distinct is used,
    -- here we first select distinct bank_trace_ids and then do the listagg order by disbursement_date
    select distinct internal_buyer_line_item_id, disbursement_date, bank_trace_id
  from billing_event_with_business_flags) distinct_disbursements
  group by internal_buyer_line_item_id
  order by internal_buyer_line_item_id
),

line_items_with_window_functions as (
--add flag next step compare gross_revenue and gross_revenue_disbursed or gross_refund and gross_refund_disbursed
  select
    line_item.internal_buyer_line_item_id,
    disbursement_id,
    disbursement_id_or_invoiced,
    product_id,
    broker_id,
    currency,
    agreement_id,
    proposer_account_id,
    acceptor_account_id,
    -- when there's aws_rev_Share adjustment/refund to a seller_rev_share invoice, it can happen that for the same aws_rev_share invoice_id, there are multiple disbursement events,
    -- using windows function to map payer_account_id of seller_rev_share to all corresponding aws_rev_Share
    max(payer_account_id) over (partition by line_item.internal_buyer_line_item_id) as payer_account_id,
    offer_id,
    end_user_account_id,
    usage_period_start_date,
    usage_period_end_date,
    payment_due_date,
    bank_trace_id,
    disbursement_date,
    billing_address_id,

    -- Buyer/seller columns:
    max(buyer_invoice_id) over (partition by line_item.internal_buyer_line_item_id) as buyer_invoice_id,
    seller_invoice_id,
    max(buyer_invoice_date) over (partition by line_item.internal_buyer_line_item_id) as buyer_invoice_date,
    seller_invoice_date,

    -- When disbursement_id_or_invoiced = '<invoiced>', these are actually invoiced amounts
    -- When disbursement_id_or_invoiced <> '<invoiced>' these are disbursed amounts for _this_ specific disbursement_id
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
    balance_adjustment_this_disbursement_id_or_invoiced,
    seller_rev_credit_this_disbursement_id_or_invoiced,
    aws_ref_fee_credit_this_disbursement_id_or_invoiced,
    -- IMPORTANT: All window functions partitioned by internal_buyer_line_item_id:

    -- Invoiced amounts, categorized by transaction type:
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then gross_revenue_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end)over (partition by line_item.internal_buyer_line_item_id) as gross_revenue_invoiced,
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then gross_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as gross_refund_invoiced,
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then cogs_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as cogs_invoiced,
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then cogs_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as cogs_refund_invoiced,
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then aws_rev_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as aws_rev_share_invoiced,
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then aws_refund_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as aws_refund_share_invoiced,
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then aws_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as aws_tax_share_invoiced,
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then aws_tax_share_listing_fee_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as aws_tax_share_listing_fee_invoiced,
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then aws_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as aws_tax_share_refund_invoiced,
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as aws_tax_share_refund_listing_fee_invoiced,
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as seller_tax_share_invoiced,
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as seller_tax_share_refund_invoiced,
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then balance_adjustment_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as balance_adjustment_invoiced,
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then seller_rev_credit_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as seller_rev_credit_invoiced,
    sum(case when disbursement_id_or_invoiced = '<invoiced>' then aws_ref_fee_credit_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as aws_ref_fee_credit_invoiced,

    -- Total disbursed amounts (for all disbursement_id values), categorized by transaction type:
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then gross_revenue_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as gross_revenue_disbursed,
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then gross_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as gross_refund_disbursed,
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then cogs_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as cogs_disbursed,
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then cogs_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as cogs_refund_disbursed,
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then aws_rev_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as aws_rev_share_disbursed,
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then aws_refund_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as aws_refund_share_disbursed,
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then aws_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as aws_tax_share_disbursed,
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then aws_tax_share_listing_fee_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as aws_tax_share_listing_fee_disbursed,
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then aws_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as aws_tax_share_refund_disbursed,
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as aws_tax_share_refund_listing_fee_disbursed,
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then seller_tax_share_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as seller_tax_share_disbursed,
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then seller_tax_share_refund_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as seller_tax_share_refund_disbursed,
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then balance_adjustment_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as balance_adjustment_disbursed,
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then seller_rev_credit_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as seller_rev_credit_disbursed,
    sum(case when disbursement_id_or_invoiced <> '<invoiced>' then aws_ref_fee_credit_this_disbursement_id_or_invoiced else cast(0 as decimal(38,6)) end) over (partition by line_item.internal_buyer_line_item_id) as aws_ref_fee_credit_disbursed,

    -- aggregate multiple disbursement
    max(disbursement_date) over (partition by line_item.internal_buyer_line_item_id) as last_disbursement_date,
    first_value(case when disbursement_id_or_invoiced = '<invoiced>' then null else disbursement_id_or_invoiced end) over(partition by line_item.internal_buyer_line_item_id order by coalesce(disbursement_date,cast('1900-01-01' as timestamp)) desc rows between unbounded preceding and unbounded following) as last_disbursement_id,
    first_value(bank_trace_id) over (partition by line_item.internal_buyer_line_item_id order by coalesce(disbursement_date,cast('1900-01-01' as timestamp)) desc rows between unbounded preceding and unbounded following) as last_disburse_bank_trace_id,
    disb_list.disbursement_date_list,
    disb_list.disburse_bank_trace_id_list
  from
    line_items_aggregated as line_item
    left join disbursement_list disb_list
      on line_item.internal_buyer_line_item_id = disb_list.internal_buyer_line_item_id
),

cppo_offer_id as (
  select
    -- Channel partner offers do not exist in offertargetfeed_v1 table (as per legal requirement), causing cppo offer be defined as 'Public' in previous step, we will convert them back to 'Private' in next step
    offer_id
  from
    offers_with_uni_temporal_data
  where
    -- seller_account_id is null means the ISV owns the offer
    seller_account_id is not null
    and seller_account_id <>  (select seller_account_id from seller_account)
  group by
    offer_id
),

line_items_with_window_functions_enrich_offer_product_address as (
  select
    internal_buyer_line_item_id,
    disbursement_id,
    disbursement_id_or_invoiced,
    line.product_id,
    legacy_product.legacy_id as legacy_product_id,
    products.title as product_title,
    line.broker_id,
    line.currency,
    line.end_user_account_id,
    acc_enduser.encrypted_account_id as end_user_encrypted_account_id,
    acc_enduser.aws_account_id as end_user_aws_account_id,
    acc_payer.aws_account_id as payer_aws_account_id,
    acc_payer.encrypted_account_id payer_encrypted_account_id,
    line.agreement_id,
    agreement.agreement_revision,
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
    case
      when offer.offer_id in (
        select distinct offer_id
        from cppo_offer_id)
        then 'Private'
      else offer.offer_target
    end as offer_target,
    offer.name offer_name,
    offer.opportunity_name offer_opportunity_name,
    offer.opportunity_description offer_opportunity_description,
    offer.opportunity_id,
    payment_due_date,
    line.bank_trace_id,
    disbursement_date,
    billing_address_id,
    buyer_invoice_id,
    seller_invoice_id,
    buyer_invoice_date,
    seller_invoice_date,
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
    balance_adjustment_this_disbursement_id_or_invoiced,
    seller_rev_credit_this_disbursement_id_or_invoiced,
    aws_ref_fee_credit_this_disbursement_id_or_invoiced,
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
    balance_adjustment_invoiced,
    seller_rev_credit_invoiced,
    aws_ref_fee_credit_invoiced,
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
    balance_adjustment_disbursed,
    seller_rev_credit_disbursed,
    aws_ref_fee_credit_disbursed,
    last_disbursement_date,
    last_disbursement_id,
    last_disburse_bank_trace_id,
    disbursement_date_list,
    disburse_bank_trace_id_list,
    products.product_code,
    acc_products.aws_account_id as manufacturer_aws_account_id,
    products.manufacturer_account_id,
    --add subscriber and payer addressID, payer address preference order: tax address > billing address > mailing address,  subscriber address preference order: tax address >  mailing address
    coalesce (
      --empty value in Athena shows as '', change all '' value to null in order to follow the preference order logic above
      case when acc_subscriber.tax_address_id ='' then null else acc_subscriber.tax_address_id end,
      case when acc_subscriber.mailing_address_id = '' then null else acc_subscriber.mailing_address_id end) as subscriber_address_id,
    coalesce (
      case when acc_payer.tax_address_id = '' then null else acc_payer.tax_address_id end,
      case when line.billing_address_id = '' then null else line.billing_address_id end,
      case when acc_payer.mailing_address_id = '' then null else acc_payer.mailing_address_id end) as payer_address_id,
    coalesce (
      case when acc_enduser.tax_address_id = '' then null else acc_enduser.tax_address_id end,
      case when line.billing_address_id = '' then null else line.billing_address_id end,
      case when acc_enduser.mailing_address_id = '' then null else acc_enduser.mailing_address_id end) as end_user_address_id
  from
    line_items_with_window_functions as line
  left join agreements_with_history as agreement on
      line.agreement_id = agreement.agreement_id and line.buyer_invoice_date >= agreement.valid_from_adjusted and line.buyer_invoice_date < agreement.valid_to
  left join offers_with_history_with_target_type as offer on
        line.offer_id = offer.offer_id and line.buyer_invoice_date >= offer.valid_from and line.buyer_invoice_date < offer.valid_to
  left join products_with_history as products on
        line.product_id = products.product_id and line.buyer_invoice_date >= products.valid_from_adjusted and line.buyer_invoice_date < products.valid_to
  left join legacy_products as legacy_product on
        line.product_id = legacy_product.new_id
  left join accounts_with_history_with_company_name as acc_payer on
        line.payer_account_id = acc_payer.account_id and line.buyer_invoice_date >= acc_payer.valid_from and line.buyer_invoice_date < acc_payer.valid_to
  left join accounts_with_history_with_company_name as acc_enduser on
        line.end_user_account_id = acc_enduser.account_id and line.buyer_invoice_date >= acc_enduser.valid_from and line.buyer_invoice_date < acc_enduser.valid_to
  left join accounts_with_history_with_company_name as acc_subscriber on
        line.acceptor_account_id = acc_subscriber.account_id and line.buyer_invoice_date >= acc_subscriber.valid_from and line.buyer_invoice_date < acc_subscriber.valid_to
  left join accounts_with_history_with_company_name as acc_products on
        products.manufacturer_account_id = acc_products.account_id and line.buyer_invoice_date >= acc_products.valid_from and line.buyer_invoice_date < acc_products.valid_to

),

line_items_with_window_functions_enrich_offer_product_address_name as (
  select
    line.internal_buyer_line_item_id,
    disbursement_id,
    disbursement_id_or_invoiced,
    product_id,
    legacy_product_id,
    product_title,
    broker_id,
    currency,
    end_user_address_id,
    end_user_account_id,
    end_user_encrypted_account_id,
    end_user_aws_account_id,
    add_enduser.company_name end_user_company_name,
    add_enduser.email_domain end_user_email_domain,
    add_enduser.city end_user_city,
    add_enduser.state_or_region end_user_state,
    add_enduser.country_code end_user_country,
    add_enduser.postal_code end_user_postal_code,
    payer_aws_account_id,
    payer_encrypted_account_id,
    payer_address_id,
    add_payer.company_name payer_company_name,
    add_payer.email_domain payer_email_domain,
    add_payer.city payer_city,
    add_payer.state_or_region payer_state,
    add_payer.country_code payer_country,
    add_payer.postal_code payer_postal_code,
    agreement_id,
    agreement_revision,
    agreement_start_date,
    agreement_end_date,
    agreement_acceptance_date,
    agreement_updated_date,
    case when proposer_account_id = (select seller_account_id from seller_account) then null else acc_proposer.aws_account_id end as reseller_aws_account_id,
    case when proposer_account_id = (select seller_account_id from seller_account) then null else acc_proposer.mailing_company_name end as reseller_company_name,
    usage_period_start_date,
    usage_period_end_date,
    proposer_account_id,
    acc_proposer.aws_account_id as proposer_aws_account_id,
    acceptor_account_id,
    subscriber_aws_account_id,
    subscriber_encrypted_account_id,
    subscriber_address_id,
    add_subscriber.company_name subscriber_company_name,
    add_subscriber.email_domain subscriber_email_domain,
    add_subscriber.city subscriber_city,
    add_subscriber.state_or_region subscriber_state,
    add_subscriber.country_code subscriber_country,
    add_subscriber.postal_code subscriber_postal_code,
    offer_id,
    offer_target,
    offer_name,
    offer_opportunity_name,
    offer_opportunity_description,
    opportunity_id,
    payment_due_date,
    bank_trace_id,
    disbursement_date,
    billing_address_id,
    max(buyer_invoice_id)as buyer_invoice_id,
    max(seller_invoice_id)as seller_invoice_id,
    max(buyer_invoice_date)as buyer_invoice_date,
    max(seller_invoice_date)as seller_invoice_date,
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
    balance_adjustment_this_disbursement_id_or_invoiced,
    seller_rev_credit_this_disbursement_id_or_invoiced,
    aws_ref_fee_credit_this_disbursement_id_or_invoiced,
    (gross_revenue_this_disbursement_id_or_invoiced + gross_refund_this_disbursement_id_or_invoiced + aws_rev_share_this_disbursement_id_or_invoiced + aws_refund_share_this_disbursement_id_or_invoiced + seller_tax_share_this_disbursement_id_or_invoiced + seller_tax_share_refund_this_disbursement_id_or_invoiced
      + cogs_this_disbursement_id_or_invoiced + cogs_refund_this_disbursement_id_or_invoiced + aws_tax_share_listing_fee_this_disbursement_id_or_invoiced + aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced) as seller_net_revenue_this_disbursement_id_or_invoiced,
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
    balance_adjustment_invoiced,
    seller_rev_credit_invoiced,
    aws_ref_fee_credit_invoiced,
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
    balance_adjustment_disbursed,
    seller_rev_credit_disbursed,
    aws_ref_fee_credit_disbursed,
    (gross_revenue_invoiced + gross_revenue_disbursed) as uncollected_gross_revenue,
    -- net revenue = gross revenue - listing fee - tax - cogs
    (gross_revenue_invoiced + gross_refund_invoiced + aws_rev_share_invoiced + aws_refund_share_invoiced + seller_tax_share_invoiced + seller_tax_share_refund_invoiced + cogs_invoiced + cogs_refund_invoiced + aws_tax_share_listing_fee_invoiced + aws_tax_share_refund_listing_fee_invoiced) as seller_net_revenue,
    (gross_revenue_invoiced + gross_refund_invoiced + aws_rev_share_invoiced + aws_refund_share_invoiced + seller_tax_share_invoiced + seller_tax_share_refund_invoiced + cogs_invoiced + cogs_refund_invoiced + aws_tax_share_listing_fee_invoiced + aws_tax_share_refund_listing_fee_invoiced
      + gross_revenue_disbursed + gross_refund_disbursed + aws_rev_share_disbursed + aws_refund_share_disbursed + seller_tax_share_disbursed + seller_tax_share_refund_disbursed + cogs_disbursed + cogs_refund_disbursed + aws_tax_share_listing_fee_disbursed + aws_tax_share_refund_listing_fee_disbursed) as uncollected_seller_net_revenue,
    last_disbursement_date,
    last_disbursement_id,
    last_disburse_bank_trace_id,
    disbursement_date_list,
    disburse_bank_trace_id_list,
    product_code,
    manufacturer_aws_account_id,
    manufacturer_account_id,
    acc_manu.mailing_company_name as manufacturer_company_name,
    cast(null as varchar) as AR_Period,
    case
      when (
        (gross_revenue_invoiced <>0 and gross_revenue_invoiced = -1 * gross_revenue_disbursed)
        or (gross_refund_invoiced <> 0 and gross_refund_invoiced = -1 * gross_refund_disbursed)
        or (balance_adjustment_invoiced <> 0 and balance_adjustment_invoiced = -1 * balance_adjustment_disbursed)
        or (seller_tax_share_refund_invoiced <> 0 and seller_tax_share_refund_invoiced = -1 * seller_tax_share_refund_disbursed)
        or (gross_revenue_invoiced = 0 and gross_refund_invoiced = 0 and balance_adjustment_invoiced = 0 and seller_tax_share_refund_invoiced = 0 and last_disbursement_id is not null)) then 'Yes'
      when gross_revenue_disbursed = 0 and gross_refund_disbursed = 0 and balance_adjustment_disbursed = 0 and seller_tax_share_disbursed = 0 and seller_tax_share_refund_disbursed = 0 then 'No'
      else 'Partial'
    end as Disbursement_Flag
  from line_items_with_window_functions_enrich_offer_product_address as line
  left join accounts_with_history_with_company_name as acc_manu on
    line.manufacturer_account_id = acc_manu.account_id and line.buyer_invoice_date >= acc_manu.valid_from_adjusted and line.buyer_invoice_date <= acc_manu.valid_to
  left join accounts_with_history_with_company_name as acc_proposer on
    line.proposer_account_id = acc_proposer.account_id and line.buyer_invoice_date >= acc_proposer.valid_from and line.buyer_invoice_date < acc_proposer.valid_to
  left join address_with_latest_revision as add_payer on
    line.payer_address_id = add_payer.address_id
  left join address_with_latest_revision as add_subscriber on
    line.subscriber_address_id = add_subscriber.address_id
  left join address_with_latest_revision as add_enduser on
    line.end_user_address_id = add_enduser.address_id
  group by
    line.internal_buyer_line_item_id,
    disbursement_id,
    disbursement_id_or_invoiced,
    product_id,
    legacy_product_id,
    product_title,
    broker_id,
    currency,
    end_user_address_id,
    end_user_account_id,
    end_user_encrypted_account_id,
    end_user_aws_account_id,
    add_enduser.company_name,
    add_enduser.email_domain,
    add_enduser.city,
    add_enduser.state_or_region,
    add_enduser.country_code,
    add_enduser.postal_code,
    payer_aws_account_id,
    payer_encrypted_account_id,
    payer_address_id,
    add_payer.company_name,
    add_payer.email_domain,
    add_payer.city,
    add_payer.state_or_region,
    add_payer.country_code,
    add_payer.postal_code,
    agreement_id,
    agreement_revision,
    case when proposer_account_id = (select seller_account_id from seller_account) then null else acc_proposer.aws_account_id end,
    case when proposer_account_id = (select seller_account_id from seller_account) then null else acc_proposer.mailing_company_name end,
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
    add_subscriber.company_name,
    add_subscriber.email_domain,
    add_subscriber.city,
    add_subscriber.state_or_region,
    add_subscriber.country_code,
    add_subscriber.postal_code,
    offer_id,
    offer_target,
    offer_name,
    offer_opportunity_name,
    offer_opportunity_description,
    opportunity_id,
    payment_due_date,
    bank_trace_id,
    disbursement_date,
    billing_address_id,
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
    balance_adjustment_this_disbursement_id_or_invoiced,
    seller_rev_credit_this_disbursement_id_or_invoiced,
    aws_ref_fee_credit_this_disbursement_id_or_invoiced,
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
    balance_adjustment_invoiced,
    seller_rev_credit_invoiced,
    aws_ref_fee_credit_invoiced,
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
    balance_adjustment_disbursed,
    seller_rev_credit_disbursed,
    aws_ref_fee_credit_disbursed,
    last_disbursement_date,
    last_disbursement_id,
    last_disburse_bank_trace_id,
    disbursement_date_list,
    disburse_bank_trace_id_list,
    product_code,
    manufacturer_aws_account_id,
    manufacturer_account_id,
    acc_manu.mailing_company_name,
    proposer_account_id,
    acc_proposer.aws_account_id
),
invoiced_not_disbursed as(
  select
    --we will filter on rownum =1 in next step,
    -- means internal_buyer_line_item_id, there's only <invoiced> record, no disbursement_id linked
    *,
    max(case when disbursement_id_or_invoiced = '<invoiced>' then 1 else 2 end)
      over (partition by internal_buyer_line_item_id) rownum
  from line_items_with_window_functions_enrich_offer_product_address_name as line_items

),
collections_and_disbursements as (
  select
    ------------------
    -- Invoice Info --
    ------------------
    buyer_invoice_date as Invoice_Date,
    Payment_Due_Date as Payment_Due_Date,
    concat(
      'Net ',
      case
        when abs(date_diff('Day', Payment_due_date, buyer_invoice_date)) > 180 then '180+'
        else cast(abs(date_diff('Day', Payment_due_date, buyer_invoice_date)) as varchar)
        end,
      ' days'
    ) as payment_terms,
    buyer_invoice_id as Invoice_ID,
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when seller_invoice_id = '' then null else seller_invoice_id end,
      'Not applicable') as Listing_Fee_Invoice_ID,

    ---------------------------
    --End user Information --
    ---------------------------
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when End_User_Company_Name = '' then null else End_User_Company_Name end,
      'Not available') as End_User_Company_Name,
    End_User_AWS_Account_ID,
    End_User_Encrypted_Account_ID,
    End_User_Email_Domain,
    End_User_City,
    End_User_State as End_User_State_or_Region,
    End_User_Country,
    End_User_Postal_Code,
    End_User_Address_ID,

    ---------------------------
    --Subscriber Information --
    ---------------------------
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      when Subscriber_Company_Name is null or Subscriber_Company_Name = '' then 'Not provided'
      else Subscriber_Company_Name
    end as Subscriber_Company_Name,
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      else Subscriber_AWS_Account_ID
    end as Subscriber_AWS_Account_ID,
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      else Subscriber_Encrypted_Account_ID
    end as Subscriber_Encrypted_Account_ID,
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      when Subscriber_Email_Domain is null or Subscriber_Email_Domain = '' then 'Not provided'
      else Subscriber_Email_Domain
    end as Subscriber_Email_Domain,
    case
      when Agreement_id is null or Agreement_ID = '' then 'Not available'
      when Subscriber_City is null or Subscriber_City = '' then 'Not provided'
      else Subscriber_City
    end as Subscriber_City,
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      when Subscriber_State is null or Subscriber_State = '' then 'Not provided'
      else Subscriber_State
    end as Subscriber_State_or_Region,
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      when Subscriber_Country is null or Subscriber_Country = '' then 'Not provided'
      else Subscriber_Country
    end as Subscriber_Country,
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      when Subscriber_Postal_Code is null or Subscriber_Postal_Code = '' then 'Not provided'
      else Subscriber_Postal_Code
    end as Subscriber_Postal_Code,
    case
      when Agreement_ID is null or Agreement_ID = '' then 'Not available'
      when Subscriber_Address_ID is null or Subscriber_Address_ID = '' then 'Not provided'
      else Subscriber_Address_ID
    end as Subscriber_Address_ID,

    ----------------------
    -- Procurement Info --
    ----------------------
    -- product title at time of invoice. It is possible that the title changes over time and therefore there may be multiple product titles mapped to a single product id.
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when Product_Title = '' then null else Product_Title end,
      'Not provided') as Product_Title,
    -- offer name at time of invoice. It is possible that the name changes over time therefore there may be multiple offer names mapped to a single offer id.
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      when (Offer_Name is null or Offer_Name = '') and Offer_Target = 'Public' then 'Not applicable'
      else Offer_Name
    end as Offer_Name,
    case
      when Agreement_Id is null or Agreement_ID = ''
      then 'Not available'
      else Offer_ID
    end as Offer_ID,
    -- offer visibility at time of invoice.,
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      else Offer_Target
    end as Offer_Visibility,
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when Agreement_ID = '' then null else Agreement_ID end,
      'Not available') as Agreement_ID,
    Agreement_Start_Date,
    Agreement_Acceptance_Date,
    Agreement_End_Date,

    Usage_Period_Start_Date,
    Usage_Period_End_Date,

    -----------------------
    -- Disbursement Info --
    -----------------------
    case
      when Disbursement_Flag = 'Yes' then 'Disbursed'
      when Disbursement_Flag = 'No' then 'Not Disbursed'
      else 'Other'
    end as Disbursement_Status,
    last_disbursement_date as disbursement_date,
    case
      when Disbursement_Flag = 'Yes' then date_diff('DAY', date_trunc('DAY',payment_due_date), date_trunc('DAY',last_disbursement_date))
      else null
    end as Disbursement_Time,
    case
      when Disbursement_Flag = 'No' then 'Not applicable'
      when bank_trace_id is null or bank_trace_id = '' then 'Not available'
      else bank_trace_id
    end as disburse_bank_trace_id,

    --------------
    -- Revenues --
    --------------
    -- We are rounding the sums using 2 decimal precision
    -- Note that the rounding method might differ between SQL implementations.
    -- The monthly revenue report is using RoundingMode.HALF_UP. This might create tiny discrepancies between this SQL output
    -- and the legacy report
    round(-1 * gross_revenue_this_disbursement_id_or_invoiced,2) as Gross_Revenue,
    round(-1 * gross_refund_this_disbursement_id_or_invoiced,2) as Gross_Refund,
    round(-1 * aws_rev_share_this_disbursement_id_or_invoiced,2) as Listing_Fee,
    round(-1 * aws_refund_share_this_disbursement_id_or_invoiced,2) as Listing_Fee_Refund,
    truncate(
      case
        when gross_revenue_this_disbursement_id_or_invoiced != 0 then abs(aws_rev_share_this_disbursement_id_or_invoiced/gross_revenue_this_disbursement_id_or_invoiced)
        when gross_refund_this_disbursement_id_or_invoiced != 0 then abs(aws_refund_share_this_disbursement_id_or_invoiced/gross_refund_this_disbursement_id_or_invoiced)
        else 0
      end
      ,4) as Listing_Fee_Percentage,
    round(-1 * seller_tax_share_this_disbursement_id_or_invoiced,2) as Seller_Tax_Share,
    round(-1 * seller_tax_share_refund_this_disbursement_id_or_invoiced,2) as Seller_Tax_Share_Refund,
    round(-1 * aws_tax_share_listing_fee_this_disbursement_id_or_invoiced,2) as AWS_Tax_Share_Listing_Fee,
    round(-1 * aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced,2) as AWS_Tax_Share_Refund_Listing_Fee,
    round(-1 * cogs_this_disbursement_id_or_invoiced,2) as Wholesale_cost,
    round(-1 * cogs_refund_this_disbursement_id_or_invoiced,2) as Wholesale_cost_Refund,
    round(-1 * seller_net_revenue_this_disbursement_id_or_invoiced,2) as Seller_Net_Revenue,
    currency as Currency,

    substring(internal_buyer_line_item_id,1,strpos(internal_buyer_line_item_id,'-')-1) as Transaction_Reference_ID,
    broker_id as AWS_seller_of_record,

    -----------------
    -- Resale info --
    -----------------
    case
      when Opportunity_Id is null or Opportunity_Id = '' then
        case
          when Offer_Target = 'Public' then 'Not applicable'
          when (Offer_Target is null or Offer_Target = '') and (Agreement_Id is not null and Agreement_Id != '') then 'Not applicable'
          else null
        end
      else Opportunity_Id
    end as Resale_authorization_ID,
    case
      when Offer_Opportunity_Name is null or Offer_Opportunity_Name = '' then
        case
          when Offer_Target = 'Public' then 'Not applicable'
          when (Offer_Target is null or Offer_Target = '') and (Agreement_Id is not null and Agreement_Id != '') then 'Not applicable'
          else null
        end
      else Offer_Opportunity_Name
    end as Resale_authorization_name,
    case
      when Offer_Opportunity_Description is null or Offer_Opportunity_Description = '' then
        case
          when Offer_Target = 'Public' then 'Not applicable'
          when (Offer_Target is null or Offer_Target = '') and (Agreement_Id is not null and Agreement_Id != '') then 'Not applicable'
          else null
        end
      else Offer_Opportunity_Description
    end as Resale_authorization_description,
    case
      when (Reseller_AWS_Account_ID is not null and Reseller_AWS_Account_ID != '')
        and (Reseller_Company_Name is null or Reseller_Company_Name = '') then 'Not available'
      when (Reseller_AWS_Account_ID is null or Reseller_AWS_Account_ID = '')
        and (opportunity_id is null or opportunity_id = '') then 'Not applicable'
      when (select seller_account_id from seller_account) <> manufacturer_aws_account_id
        and (Reseller_AWS_Account_ID is null or Reseller_AWS_Account_ID = '') then 'Not applicable'
      else Reseller_Company_Name
    end as Reseller_Company_Name,
    case
      when (Reseller_AWS_Account_ID is null or Reseller_AWS_Account_ID = '')
        and (Opportunity_Id is null or Opportunity_Id = '') then 'Not applicable'
      when (select seller_account_id from seller_account) <> manufacturer_aws_account_id
        and (Reseller_AWS_Account_ID is null or Reseller_AWS_Account_ID = '') then 'Not applicable'
      else Reseller_AWS_Account_ID
    end as Reseller_AWS_Account_ID,

    -----------------------
    -- Payer Information --
    -----------------------
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when Payer_Company_Name = '' then null else Payer_Company_Name end,
      'Not available') as Payer_Company_Name,
    Payer_AWS_Account_ID, -- "Customer AWS Account Number" in legacy report
    Payer_Encrypted_Account_ID,
    Payer_Email_Domain,
    Payer_City,
    Payer_State as Payer_State_or_Region,
    Payer_Country,
    Payer_Postal_Code,
    Payer_Address_ID,

    ---------------------
    -- ISV Information --
    ---------------------
    manufacturer_aws_account_id as ISV_Account_ID,
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when Manufacturer_Company_Name = '' then null else Manufacturer_Company_Name end,
      'Not available') as ISV_Company_Name,

    ---------------------
    -- Products info --
    ---------------------
    Legacy_Product_ID,
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when Product_ID = '' then null else Product_ID end,
      'Not provided') as Product_ID,
    -- this is to get the legacy product id https://sim.amazon.com/issues/MP-INSIGHTS-2561
    Product_Code,

    case when Disbursement_Flag = 'Yes' then round(-1 * seller_net_revenue_this_disbursement_id_or_invoiced,2) else 0 end as Disbursed_Net_Revenue,
    case when Disbursement_Flag = 'No' then round(-1 * seller_net_revenue_this_disbursement_id_or_invoiced,2) else 0 end as Undisbursed_Net_Revenue,
    case
      when case when Disbursement_Flag = 'Yes' then date_diff('DAY', date_trunc('DAY',payment_due_date), date_trunc('DAY',last_disbursement_date)) else null end <= 0 then 'Not due'
      when case when Disbursement_Flag = 'Yes' then date_diff('DAY', date_trunc('DAY',payment_due_date), date_trunc('DAY',last_disbursement_date)) else null end <=30 then '1-30 days late'
      when case when Disbursement_Flag = 'Yes' then date_diff('DAY', date_trunc('DAY',payment_due_date), date_trunc('DAY',last_disbursement_date)) else null end <=60 then '31-60 days late'
      when case when Disbursement_Flag = 'Yes' then date_diff('DAY', date_trunc('DAY',payment_due_date), date_trunc('DAY',last_disbursement_date)) else null end <=90 then '61-90 days late'
      when case when Disbursement_Flag = 'Yes' then date_diff('DAY', date_trunc('DAY',payment_due_date), date_trunc('DAY',last_disbursement_date)) else null end <=120 then '91-120 days late'
      when case when Disbursement_Flag = 'Yes' then date_diff('DAY', date_trunc('DAY',payment_due_date), date_trunc('DAY',last_disbursement_date)) else null end >=121 then '121+ days late'
      else null
    end as Disbursement_Period
  from
    line_items_with_window_functions_enrich_offer_product_address_name as line
  where disbursement_id_or_invoiced != '<invoiced>'

  union

  select
    ------------------
    -- Invoice Info --
    ------------------
    buyer_invoice_date as Invoice_Date,
    Payment_Due_Date as Payment_Due_Date,
    concat(
      'Net ',
      case
        when abs(date_diff('Day', Payment_due_date, buyer_invoice_date)) > 180 then '180+'
        else cast(abs(date_diff('Day', Payment_due_date, buyer_invoice_date)) as varchar)
      end,
      ' days'
    ) as payment_terms,
    buyer_invoice_id as Invoice_ID,
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when seller_invoice_id = '' then null else seller_invoice_id end,
      'Not applicable') as Listing_Fee_Invoice_ID,

    ---------------------------
    --End user Information --
    ---------------------------
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when End_User_Company_Name = '' then null else End_User_Company_Name end,
      'Not available') as End_User_Company_Name,
    End_User_AWS_Account_ID,
    End_User_Encrypted_Account_ID,
    End_User_Email_Domain,
    End_User_City,
    End_User_State as End_User_State_or_Region,
    End_User_Country,
    End_User_Postal_Code,
    End_User_Address_ID,

    ---------------------------
    --Subscriber Information --
    ---------------------------
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      when Subscriber_Company_Name is null or Subscriber_Company_Name = '' then 'Not provided'
      else Subscriber_Company_Name
    end as Subscriber_Company_Name,
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      else Subscriber_AWS_Account_ID
    end as Subscriber_AWS_Account_ID,
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      else Subscriber_Encrypted_Account_ID
    end as Subscriber_Encrypted_Account_ID,
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      when Subscriber_Email_Domain is null or Subscriber_Email_Domain = '' then 'Not provided'
      else Subscriber_Email_Domain
    end as Subscriber_Email_Domain,
    case
      when Agreement_id is null or Agreement_ID = '' then 'Not available'
      when Subscriber_City is null or Subscriber_City = '' then 'Not provided'
      else Subscriber_City
    end as Subscriber_City,
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      when Subscriber_State is null or Subscriber_State = '' then 'Not provided'
      else Subscriber_State
    end as Subscriber_State_or_Region,
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      when Subscriber_Country is null or Subscriber_Country = '' then 'Not provided'
      else Subscriber_Country
    end as Subscriber_Country,
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      when Subscriber_Postal_Code is null or Subscriber_Postal_Code = '' then 'Not provided'
      else Subscriber_Postal_Code
    end as Subscriber_Postal_Code,
    case
      when Agreement_ID is null or Agreement_ID = '' then 'Not available'
      when Subscriber_Address_ID is null or Subscriber_Address_ID = '' then 'Not provided'
      else Subscriber_Address_ID
    end as Subscriber_Address_ID,

    ----------------------
    -- Procurement Info --
    ----------------------
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when Product_Title = '' then null else Product_Title end,
      'Not provided') as Product_Title,
    -- offer name at time of invoice. It is possible that the name changes over time therefore there may be multiple offer names mapped to a single offer id.
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      when (Offer_Name is null or Offer_Name = '') and Offer_Target = 'Public' then 'Not applicable'
      else Offer_Name
    end as Offer_Name,
    case
      when Agreement_Id is null or Agreement_ID = ''
      then 'Not available'
      else Offer_ID
    end as Offer_ID,
    -- offer visibility at time of invoice.,
    case
      when Agreement_Id is null or Agreement_ID = '' then 'Not available'
      else Offer_Target
    end as Offer_Visibility,
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when Agreement_ID = '' then null else Agreement_ID end,
      'Not available') as Agreement_ID,
    --case when Agreement_Id is null or Agreement_Id = '' then cast(null as timestamp) else Agreement_Start_Date end as Agreement_Start_Date,
    --case when Agreement_Id is null or Agreement_Id = '' then cast(null as timestamp) else Agreement_End_Date end as Agreement_End_Date,
    --case when Agreement_Id is null or Agreement_Id = '' then cast(null as timestamp) else Agreement_Acceptance_Date end as Agreement_Acceptance_Date,
    Agreement_Start_Date,
    Agreement_Acceptance_Date,
    Agreement_End_Date,

    Usage_Period_Start_Date,
    Usage_Period_End_Date,

    -----------------------
    -- Disbursement Info --
    -----------------------
    case
      when Disbursement_Flag = 'Yes' then 'Disbursed'
      when Disbursement_Flag = 'No' then 'Not Disbursed'
      else 'Other'
    end as Disbursement_Status,
    last_disbursement_date as disbursement_date,
    case
      when Disbursement_Flag = 'Yes' then date_diff('DAY', date_trunc('DAY',payment_due_date), date_trunc('DAY',last_disbursement_date))
      else null
    end as Disbursement_Time,
    case
      when Disbursement_Flag = 'No' then 'Not applicable'
      when bank_trace_id is null or bank_trace_id = '' then 'Not available'
      else bank_trace_id
    end as disburse_bank_trace_id,

    --------------
    -- Revenues --
    --------------
    -- We are rounding the sums using 2 decimal precision
    -- Note that the rounding method might differ between SQL implementations.
    -- The monthly revenue report is using RoundingMode.HALF_UP. This might create tiny discrepancies between this SQL output
    -- and the legacy report
    round(gross_revenue_this_disbursement_id_or_invoiced,2) as Gross_Revenue,
    round(gross_refund_this_disbursement_id_or_invoiced,2) as Gross_Refund,
    round(aws_rev_share_this_disbursement_id_or_invoiced,2) as Listing_Fee,
    round(aws_refund_share_this_disbursement_id_or_invoiced,2) as Listing_Fee_Refund,
    truncate(
      case
        when gross_revenue_this_disbursement_id_or_invoiced != 0 then abs(aws_rev_share_this_disbursement_id_or_invoiced/gross_revenue_this_disbursement_id_or_invoiced)
        when gross_refund_this_disbursement_id_or_invoiced != 0 then abs(aws_refund_share_this_disbursement_id_or_invoiced/gross_refund_this_disbursement_id_or_invoiced)
        else 0
      end
      ,4) as Listing_Fee_Percentage,
    round(seller_tax_share_this_disbursement_id_or_invoiced,2) as Seller_Tax_Share,
    round(seller_tax_share_refund_this_disbursement_id_or_invoiced,2) as Seller_Tax_Share_Refund,
    round(aws_tax_share_listing_fee_this_disbursement_id_or_invoiced,2) as AWS_Tax_Share_Listing_Fee,
    round(aws_tax_share_refund_listing_fee_this_disbursement_id_or_invoiced,2) as AWS_Tax_Share_Refund_Listing_Fee,
    round(cogs_this_disbursement_id_or_invoiced,2) as Wholesale_cost,
    round(cogs_refund_this_disbursement_id_or_invoiced,2) as Wholesale_cost_Refund,
    round(seller_net_revenue_this_disbursement_id_or_invoiced,2) as Seller_Net_Revenue,
    currency as Currency,

    substring(internal_buyer_line_item_id,1,strpos(internal_buyer_line_item_id,'-')-1) as Transaction_Reference_ID,
    broker_id as AWS_seller_of_record,

    -----------------
    -- Resale info --
    -----------------
    case
      when Opportunity_Id is null or Opportunity_Id = '' then
        case
          when Offer_Target = 'Public' then 'Not applicable'
          when (Offer_Target is null or Offer_Target = '') and (Agreement_Id is not null and Agreement_Id != '') then 'Not applicable'
          else null
        end
      else Opportunity_Id
    end as Resale_authorization_ID,
    case
      when Offer_Opportunity_Name is null or Offer_Opportunity_Name = '' then
        case
          when Offer_Target = 'Public' then 'Not applicable'
          when (Offer_Target is null or Offer_Target = '') and (Agreement_Id is not null and Agreement_Id != '') then 'Not applicable'
          else null
        end
      else Offer_Opportunity_Name
    end as Resale_authorization_name,
    case
      when Offer_Opportunity_Description is null or Offer_Opportunity_Description = '' then
        case
          when Offer_Target = 'Public' then 'Not applicable'
          when (Offer_Target is null or Offer_Target = '') and (Agreement_Id is not null and Agreement_Id != '') then 'Not applicable'
          else null
        end
      else Offer_Opportunity_Description
    end as Resale_authorization_description,
    case
      when (Reseller_AWS_Account_ID is not null and Reseller_AWS_Account_ID != '')
        and (Reseller_Company_Name is null or Reseller_Company_Name = '') then 'Not available'
      when (Reseller_AWS_Account_ID is null or Reseller_AWS_Account_ID = '')
        and (opportunity_id is null or opportunity_id = '') then 'Not applicable'
      when (select seller_account_id from seller_account) <> manufacturer_aws_account_id
        and (Reseller_AWS_Account_ID is null or Reseller_AWS_Account_ID = '') then 'Not applicable'
      else Reseller_Company_Name
    end as Reseller_Company_Name,
    case
      when (Reseller_AWS_Account_ID is null or Reseller_AWS_Account_ID = '')
        and (Opportunity_Id is null or Opportunity_Id = '') then 'Not applicable'
      when (select seller_account_id from seller_account) <> manufacturer_aws_account_id
        and (Reseller_AWS_Account_ID is null or Reseller_AWS_Account_ID = '') then 'Not applicable'
      else Reseller_AWS_Account_ID
    end as Reseller_AWS_Account_ID,

    -----------------------
    -- Payer Information --
    -----------------------
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when Payer_Company_Name = '' then null else Payer_Company_Name end,
      'Not available') as Payer_Company_Name,
    Payer_AWS_Account_ID, -- "Customer AWS Account Number" in legacy report
    Payer_Encrypted_Account_ID,
    Payer_Email_Domain,
    Payer_City,
    Payer_State as Payer_State_or_Region,
    Payer_Country,
    Payer_Postal_Code,
    Payer_Address_ID,

    ---------------------
    -- ISV Information --
    ---------------------
    manufacturer_aws_account_id as ISV_Account_ID,
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when Manufacturer_Company_Name = '' then null else Manufacturer_Company_Name end,
      'Not available') as ISV_Company_Name,

    ---------------------
    -- Products info --
    ---------------------
    -- product title at time of invoice. It is possible that the title changes over time and therefore there may be multiple product titles mapped to a single product id.
    Legacy_Product_ID,
    coalesce(
      --empty value in Athena shows as '', change all '' value to null
      case when Product_ID = '' then null else Product_ID end,
      'Not provided') as Product_ID,
    -- this is to get the legacy product id https://sim.amazon.com/issues/MP-INSIGHTS-2561
    Product_Code,

    case when Disbursement_Flag = 'Yes' then round(seller_net_revenue_this_disbursement_id_or_invoiced,2) else 0 end as Disbursed_Net_Revenue,
    case when Disbursement_Flag = 'No' then round(seller_net_revenue_this_disbursement_id_or_invoiced,2) else 0 end as Undisbursed_Net_Revenue,
    case
      when case when Disbursement_Flag = 'Yes' then date_diff('DAY', date_trunc('DAY',payment_due_date), date_trunc('DAY',last_disbursement_date)) else null end <= 0 then 'Not due'
      when case when Disbursement_Flag = 'Yes' then date_diff('DAY', date_trunc('DAY',payment_due_date), date_trunc('DAY',last_disbursement_date)) else null end <=30 then '1-30 days late'
      when case when Disbursement_Flag = 'Yes' then date_diff('DAY', date_trunc('DAY',payment_due_date), date_trunc('DAY',last_disbursement_date)) else null end <=60 then '31-60 days late'
      when case when Disbursement_Flag = 'Yes' then date_diff('DAY', date_trunc('DAY',payment_due_date), date_trunc('DAY',last_disbursement_date)) else null end <=90 then '61-90 days late'
      when case when Disbursement_Flag = 'Yes' then date_diff('DAY', date_trunc('DAY',payment_due_date), date_trunc('DAY',last_disbursement_date)) else null end <=120 then '91-120 days late'
      when case when Disbursement_Flag = 'Yes' then date_diff('DAY', date_trunc('DAY',payment_due_date), date_trunc('DAY',last_disbursement_date)) else null end >=121 then '121+ days late'
      else null
    end as Disbursement_Period
  from
    invoiced_not_disbursed
  where rownum = 1

)

select *
from collections_and_disbursements
where payment_due_date >= date_add('DAY', -90, current_date)
--where payment_due_date between cast('2023-01-01' as timestamp) and cast('2024-12-31' as timestamp)

--where disbursement_date >= date_add('DAY', -90, current_date)
--where disbursement_date between cast('2023-01-01' as timestamp) and cast('2024-12-31' as timestamp)
