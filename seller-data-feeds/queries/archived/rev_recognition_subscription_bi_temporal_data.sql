WITH agreements_with_uni_temporal_data AS
(
  SELECT agreement_id,
         origin_offer_id,
         proposer_account_id,
         acceptor_account_id,
         agreement_revision,
         start_date,
         end_date,
         acceptance_date,
         ROW_NUMBER() OVER(partition by agreement_id order by valid_from desc) as rn
         FROM (
         SELECT agreement_id,
					  delete_date,
                      origin_offer_id,
                      proposer_account_id,
                      acceptor_account_id,
                      agreement_revision,
                      from_iso8601_timestamp(start_date) as start_date,
                      from_iso8601_timestamp(end_date) as end_date,
                      from_iso8601_timestamp(acceptance_date) as acceptance_date,
                      from_iso8601_timestamp(valid_from ) as valid_from,
                      ROW_NUMBER() OVER (PARTITION BY agreement_id, from_iso8601_timestamp(valid_from ) ORDER BY from_iso8601_timestamp(update_date) DESC) AS row_num
				-- TODO change to agreementfeed_v1 when Agreement Feed is GA'ed
               FROM agreementfeed     
          )
  WHERE
  -- a agreement_id can appear multiple times with the same valid_from date but with a different update_date column
  -- we are only interested in the most recent tuple
  row_num = 1
  --  remove the soft-deleted one.
  and (delete_date is null or delete_date = '')
)
, -- an agreement has multiple revisions (each having a valid_from date) but we are only interested in the most recent 
agreements_with_latest_revision as
(
SELECT
         agreement_id,
         origin_offer_id as offer_id,
         proposer_account_id,
         acceptor_account_id,
         agreement_revision,
         start_date,
         end_date,
         acceptance_date
FROM agreements_with_uni_temporal_data
WHERE  rn = 1
  )
, payment_terms_with_uni_temporal_data AS
(
  SELECT agreement_id,
         charge_amount,
         charge_on,
         ROW_NUMBER() OVER(partition by agreement_id,charge_on order by valid_from desc) as rn
         FROM (
       SELECT
       from_iso8601_timestamp(valid_from) as valid_from,
       charge_amount,
	   delete_date,
       from_iso8601_timestamp(charge_on) as charge_on,
       agreement_id,
       row_number() over(partition by agreement_id, charge_on, from_iso8601_timestamp(valid_from) order by from_iso8601_timestamp(update_date) DESC) as row_num
       FROM paymentscheduletermfeed
)
  WHERE
  -- a agreement_id can appear multiple times with the same valid_from date but with a different update_date column
  -- we are only interested in the most recent tuple
  row_num = 1
  --  remove the soft-deleted one.
  and (delete_date is null or delete_date = '')

)
,
-- the same agreement id can have multiple valid from dates, we are only interested in the most recent tuple as we are retreiving the most recent tuple from the agreements
payment_terms_with_latest_revision as
(
SELECT
         agreement_id,
         charge_amount,
         charge_on
FROM payment_terms_with_uni_temporal_data

where rn = 1
)
,
products_with_uni_temporal_data as (
    select
    product_id,
    title,
    manufacturer_account_id,
    valid_from
    from
    (
        select
         product_id,
		 delete_date,
         title,
         manufacturer_account_id,
         from_iso8601_timestamp(valid_from) as valid_from,
         ROW_NUMBER() OVER (PARTITION BY product_id, from_iso8601_timestamp(valid_from) ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
        from
         productfeed_v1

      )
    where
        -- keep latest
        row_num = 1
		--  remove the soft-deleted one.
		and (delete_date is null or delete_date = '')

),
-- Here, we build the validity time range (adding valid_to on top of valid_from) of each product revision.
-- We will use it to get the product title at invoice time.
-- NB: If you'd rather get "current" product title, un-comment "products_with_latest_revision"
products_with_history as (
    select
        product_id,
        title,
        manufacturer_account_id,
        valid_from,
        coalesce(
            lead(valid_from) over (partition by product_id order by valid_from asc),
            timestamp '2999-01-01 00:00:00'
        ) as valid_to
    from products_with_uni_temporal_data
),
-- provided for reference only if you are interested into get "current" product title
--  (ie. not used afterwards)
products_with_latest_revision as (
    select
    product_id,
    title,
    manufacturer_account_id
    from
    (
        select
            product_id,
            title,			
            manufacturer_account_id,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY valid_from desc) as row_num_latest_revision
        from
            products_with_uni_temporal_data
    )
    where
        row_num_latest_revision = 1
)
,
-- An offer_id has several valid_from dates (each representing an offer revision)
--   but because of bi-temporality, an offer_id + valid_from tuple can appear multiple times with a different update_date.
-- We are only interested in the most recent tuple (ie, uni-temporal model)
offers_with_uni_temporal_data as (
    select
    offer_id,
    offer_revision,
    name,
    opportunity_id,
    opportunity_name,
  	opportunity_description,
    valid_from
    from
    (
        select
            offer_id,
			delete_date,
            offer_revision,
            name,
            opportunity_id,
            opportunity_name,
  	        opportunity_description,
            from_iso8601_timestamp(valid_from) as valid_from,
            ROW_NUMBER() OVER (PARTITION BY offer_id, from_iso8601_timestamp(valid_from) ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
        from offerfeed_v1
    )
    where
        -- keep latest ...
        row_num = 1
		--  remove the soft-deleted one.
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
        coalesce(lead(valid_from) over (partition by offer_id order by valid_from asc), timestamp '2999-01-01 00:00:00') as valid_to
    from offers_with_uni_temporal_data
),
-- provided for reference only if you are interested into get "current" offer name
-- (ie. not used afterwards)
offers_with_latest_revision as (
    select
    offer_id,
    offer_revision,
    name,
    opportunity_id,
    opportunity_name,
  	opportunity_description
    from
    (
        select
            offer_id,
            offer_revision,
            name,
            opportunity_id,
            opportunity_name,
  	        opportunity_description,
            valid_from,
            ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY valid_from desc) as row_num_latest_revision
        from
         offers_with_uni_temporal_data
    )
    where
        row_num_latest_revision = 1
)
,

offer_targets_with_uni_temporal_data as (
    select
    offer_id,
    offer_revision,
    target_type,
    value
    from
    (
        select
            offer_id,
			delete_date,
            offer_revision,
            target_type,
            value,
            ROW_NUMBER() OVER (PARTITION BY offer_target_id, from_iso8601_timestamp(valid_from) ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
        from offertargetfeed_v1
    )
    where
        -- keep latest ...
        row_num = 1
		--  remove the soft-deleted one.
		and (delete_date is null or delete_date = '')
		
),
 offers_with_history_with_target_type as (
    select
        ofr.offer_id,
        ofr.offer_revision,
		-- Though it is possible to combine several types of targeting in a single offer, in this report, we are only choosing ONE target type, based on the below preferences        
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
        ofr.opportunity_id,
        min(ofr.name) as name,
        ofr.valid_from,
        ofr.valid_to
    from
        offers_with_history ofr
        -- left joining because public offers don't have targets
        left join offer_targets_with_uni_temporal_data off_tgt on ofr.offer_id=off_tgt.offer_id and ofr.offer_revision=off_tgt.offer_revision
    group by
        ofr.offer_id,
        ofr.offer_revision,
        ofr.opportunity_id,
        -- redundant with offer_revision, as each revision has a dedicated valid_from/valid_to (but cleaner in the group by)
        ofr.valid_from,
        ofr.valid_to
),
-- provided for reference only if you are interested into get "current" offer targets
--  (ie. not used afterwards)
offers_with_latest_revision_with_target_type as (
    select
        ofr.offer_id,
        ofr.offer_revision,
        -- even though today it is  possible to combine several types of targeting in a single offer we are only picking target type for an offer
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
        ofr.opportunity_id
    from
        offers_with_latest_revision ofr
        -- left joining because public offers don't have targets
        left join offer_targets_with_uni_temporal_data off_tgt on ofr.offer_id=off_tgt.offer_id and ofr.offer_revision=off_tgt.offer_revision
    group by
        ofr.offer_id,
        ofr.opportunity_id,
        ofr.offer_revision
)
-- An offer revision has several valid_from dates (each representing an offer revision)
-- We are only interested in the most recent tuple (ie, uni-temporal model)
, offer_products_with_uni_temporal_data as (
    select
    offer_id,
    offer_revision,
    product_id
    from
    (
        select
            offer_id,
			delete_date,
            offer_revision,
            product_id,
            ROW_NUMBER() OVER (PARTITION BY offer_id, offer_revision ORDER BY from_iso8601_timestamp(update_date) desc) as row_num
        from offerproductfeed_v1
    )
    where
        -- keep latest ...
        row_num = 1
		--  remove the soft-deleted one.
		and (delete_date is null or delete_date = '')
),
 offers_products_history as (
    select
        ofr.offer_id,
        ofr.offer_revision,
        product_id,
        ofr.valid_from,
        ofr.valid_to
    from
        offers_with_history ofr
        left join offer_products_with_uni_temporal_data off_pro on ofr.offer_id=off_pro.offer_id and ofr.offer_revision=off_pro.offer_revision
    group by
        ofr.offer_id,
        ofr.offer_revision,
        product_id,
        -- redundant with offer_revision, as each revision has a dedicated valid_from/valid_to (but cleaner in the group by)
        ofr.valid_from,
        ofr.valid_to
),
-- provided for reference only if you are interested into get "current" offer products
--  (ie. not used afterwards)
offers_products_with_latest_revision as (
    select
        ofr.offer_id,
        ofr.offer_revision,
        product_id
    from
        offers_with_latest_revision ofr
        -- left joining because public offers don't have targets
        left join offer_products_with_uni_temporal_data off_pro on ofr.offer_id=off_pro.offer_id and ofr.offer_revision=off_pro.offer_revision
    group by
        ofr.offer_id,
        ofr.offer_revision,
        product_id
),
accounts_with_uni_temporal_data as (
    select
    account_id,
    aws_account_id,
    encrypted_account_id,
    mailing_address_id,
    tax_address_id,
    valid_from
    from
    (
        select
            account_id,
			delete_date,
            aws_account_id,
            encrypted_account_id,
            mailing_address_id,
            tax_address_id,
            from_iso8601_timestamp(valid_from) as valid_from,
            ROW_NUMBER() OVER (PARTITION BY account_id, from_iso8601_timestamp(valid_from) ORDER BY from_iso8601_timestamp(update_date)desc) as row_num
        from accountfeed_v1
    )
    where
        -- keep latest ...
        row_num = 1
		--  remove the soft-deleted one.
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
        valid_from,
        coalesce(
            lead(valid_from) over (partition by account_id order by valid_from asc),
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
		--  remove the soft-deleted one.
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
            ROW_NUMBER() OVER (PARTITION BY address_id ORDER BY valid_from desc) as row_num_latest_revision
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
        pii.company_name,
        acc.valid_from,
        acc.valid_to
    from accounts_with_history acc
    -- left join because mailing_address_id can be null but when exists
    left join pii_with_latest_revision pii on acc.mailing_address_id=pii.address_id
)
,
agreements_at_subscription_intermediate as (
select
agg.offer_id,
agg.agreement_id,
agg.agreement_revision,
case when pro.manufacturer_account_id <> agg.proposer_account_id then agg.proposer_account_id else null end as reseller_account_id,
agg.acceptor_account_id,
agg.start_date as agreement_start_date,
agg.end_date as agreement_end_date,
agg.acceptance_date as agreement_acceptance_date,
ot.offer_target,
ot.name as  offer_name,
ot.opportunity_id as opportunity_id,
op.product_id,
pro.title as product_title,
case when pt.agreement_Id is not null and  row_number() over(partition by agg.agreement_id order by charge_on) = 1 then  sum(charge_amount) over(partition by agg.agreement_id ) end  as total_contract_value,
case when pt.agreement_Id is not null then dense_rank() over(partition by agg.agreement_id order by charge_on) end as dn,
charge_on as scheduled_invoice_date,
charge_amount as installment_amount
from agreements_with_latest_revision agg
-- retrieving only the agreements where the payment terms exist
join payment_terms_with_latest_revision pt on agg.agreement_id = pt.agreement_Id
-- to get the latest offer target and offer name instead of at the agreement time use offers_with_latest_revision_with_target_type 
left join offers_with_history_with_target_type ot on agg.offer_id = ot.offer_id and agg.acceptance_date  >= ot.valid_from and agg.acceptance_date < ot.valid_to
-- to get the latest product id associated to an offer instead of at the agreement time  use offers_products_with_latest_revision
join offers_products_history op on agg.offer_id = op.offer_id and agg.acceptance_date  >=  op.valid_from and agg.acceptance_date < op.valid_to
-- to get the latest product title instead of at the agreement time  use products_with_latest_revision 
join products_with_history pro on op.product_id = pro.product_id and agg.acceptance_date  >=  pro.valid_From and agg.acceptance_date < pro.valid_to
)
,
--add subscriber addressID, subscriber address preference order: tax address >  mailing address rf:https://issues.amazon.com/issues/MP-SELLER-REPORTS-13
agreements_at_subscription_time as (
select
offer_id
,offer_name
,opportunity_id
,offer_target
,agreement_id
,agreement_revision
,agreement_start_date
,agreement_end_date
,agreement_acceptance_date
,res_acc.aws_account_id as reseller_account_id
,res_acc.company_name as reseller_name
,buyer_acc.aws_account_id as subscriber_account_id
,buyer_acc.encrypted_account_id as subscriber_encrypted_account_id
, coalesce (
  		     --empty value in Athena shows as '', change all '' value to null in order to follow the preference order logic above
  		     case when buyer_acc.tax_address_id ='' then null else buyer_acc.tax_address_id end,
  		     case when buyer_acc.mailing_address_id = '' then null else buyer_acc.mailing_address_id end) as subscriber_address_id
,product_id
,product_title
,max(dn) over(partition by agreement_id) as number_of_installments
,total_contract_value
,scheduled_invoice_date
,installment_amount
from agreements_at_subscription_intermediate agg
join accounts_with_history_with_company_name buyer_acc on agg.acceptor_account_id = buyer_acc.account_id and agg.agreement_acceptance_date  >=  buyer_acc.valid_From and agg.agreement_acceptance_date < buyer_acc.valid_to
-- left join as reseller account id can be null
left join accounts_with_history_with_company_name res_acc on agg.reseller_account_id = res_acc.account_id and agg.agreement_acceptance_date  >=  res_acc.valid_From and agg.agreement_acceptance_date < res_acc.valid_to
)
select 
-----------------------
-- Procurement Info ---
-----------------------
offer_id as "Offer ID"
-- offer name at time of subscription. It is possible that the name changes over time therefore there may be multiple offer names mapped to a single offer id.
,offer_name as "Offer Name"
,offer.opportunity_name as "Opportunity Name"
,offer.opportunity_description as "Opportunity Description"
-- offer target at time of subscription, forced to "private" for channel partners offers (because legal prevents us from exposing the actual targeting of CPPOs to ISVs, but so far, all those offers are private)
, case when reseller_account_id is not null then 'Private' else offer_target end as "Offer Target"
-- all agreement related data are surfaced as they were at time of subscription.
,agreement_id as "Agreement ID"
,agreement_revision as "Agreement Revision"
,agreement_start_date as "Agreement Start Date"
,agreement_end_date as "Agreement End Date"
,agreement_acceptance_date as "Agreement Acceptance Date"
,reseller_account_id as "Reseller Account ID"
-- reseller company name at time of subscription
,reseller_name as "Reseller Company Name"
-------------------
-- Customer Info --
-------------------
,subscriber_account_id as "Subscriber Account ID"
,subscriber_encrypted_account_id as "Subscriber Encrypted Account ID"
-- subscriber company name at time of subscription
,pii.company_name as "Subscriber Company Name"
,pii.email_domain as "Subscriber Email Domain"
,pii.country as "Subscriber Country"
,pii.state_or_region as "Subscriber State"
,pii.city as "Subscriber City"
,pii.postal_code as "Subscriber Postal Code"
------------------
-- Product Info --
------------------
,product_id as "Product ID"
-- product title at time of invoice. It is possible that the title changes over time and therefore there may be multiple product titles mapped to a single product id.
,product_title as "Product Title"
--------------------------
-- Scheduled Payments ----
--------------------------
,number_of_installments as "Number of Installments"
,total_contract_value as "Total Contract Value"
,scheduled_invoice_date as "Scheduled Invoice Date"
,installment_amount as "Installment Amount"
from agreements_at_subscription_time sub
join pii_with_latest_revision pii on sub.subscriber_address_id = pii.address_id
join (select distinct opportunity_id,opportunity_name,opportunity_description from offers_with_history)offer on sub.opportunity_id = offer.opportunity_id
-- Filter results to within a 90 trailing period
where agreement_acceptance_date > date_add('DAY', -90, current_date)
