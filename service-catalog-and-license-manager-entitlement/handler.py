import os
import json
import logging
import boto3
from botocore.exceptions import ClientError
import time

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger()


portfolio_share = []
portfolio_id = os.environ.get('PORTFOLIO_ID')



"""
Purpose: By creating new product type "MARKETPLACE" 
cloudwatch event rule trigger lambda which validate portfolio share status,
and add product to portfolio.
:event: cloudwatch event rule PrivateMarketplaceProductRule
:context: context

"""



def handler(event, context):
    logger.info('## Triggered by copy to SC product event: ')
    validate_portfolio_share(portfolio_id)
    associate_product(event)
    share_grants_to_linked_accounts(event)

        
def validate_portfolio_share(portfolio_id):
    """
    Purpose: check portfolio share status for organization and / or organization unit.
    Input: portfolio_id created in the marketplace stack 
    """

    try: 
        client = boto3.client('servicecatalog')
        for item in ['ORGANIZATION', 'ORGANIZATIONAL_UNIT']:

            response = client.describe_portfolio_shares(
                    PortfolioId=portfolio_id,
                    Type=item)
            if len(response['PortfolioShareDetails']) == 0:
                pass
            elif len(response['PortfolioShareDetails']) != 0 and response['PortfolioShareDetails'][0]['PrincipalId'] not in portfolio_share:
                portfolio_share.append(response['PortfolioShareDetails'][0]['PrincipalId'])            
                return portfolio_share 
        if len(portfolio_share) == 0:
            logger.warning('Marketplace Porfolio: %s must be shared across Organization!', portfolio_id)
        else:
            logger.info('%s is shared with %s', portfolio_id, portfolio_share)
    except ClientError as e:
        if e.response['Error']['Code'] == 'AccessDeniedException':
            logging.warning('Lambda does not have DescribePortfolioShares permission for client operation on %s', portfolio_id)
        else:
            raise e 
            
def associate_product(event):

    """
    Purpose: extract product product_id then associate with Marketplace portfolio.
    """

    client = boto3.client('servicecatalog')
    message = event['detail']['responseElements']
    product_id = message['productViewDetail']['productViewSummary']['productId']
    try:
        response = client.associate_product_with_portfolio(
                        ProductId=product_id,
                        PortfolioId=portfolio_id)
        logging.info('%s is associated with Portfolio %s', product_id, portfolio_id)
        return response
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logging.error(e)
        else:
            raise e


def share_grants_to_linked_accounts(event):
    """
    Purpose: Share grants to the linked accounts. 
    """
    
    message = event['detail']['responseElements']
    
    product_name = message['productViewDetail']['productViewSummary']['name']
    
    logging.info('Find license for product name %s', product_name)
    
    mp_licenses = boto3.client('license-manager').list_received_licenses()
    mp_license = next(filter(lambda obj: obj.get('ProductName') == product_name, mp_licenses['Licenses']), None)
    
    logging.info('License found: %s', mp_license)


    org = boto3.client('organizations')

    paginator = org.get_paginator('list_accounts')
    account_iterator = paginator.paginate()
    current_account_id = boto3.client("sts").get_caller_identity()["Account"]
    
    for accounts in account_iterator:        
        for account in accounts['Accounts']:
            print(account) # print the account
            
            if account['Id'] == current_account_id:
                continue
            
            try:
                
                create_grant_response = boto3.client('license-manager', region_name='us-east-1').create_grant(
                            ClientToken= f"token_{round(time.time() * 1000)}",
                            GrantName= f"{product_name}-{account['Id']}",
                            LicenseArn=mp_license['LicenseArn'],
                            Principals=[
                                f"arn:aws:iam::{account['Id']}:root",
                            ],
                            HomeRegion='us-east-1',
                            AllowedOperations= ["CheckoutLicense", "CheckInLicense",
                                 "ExtendConsumptionLicense", "ListPurchasedLicenses"]
                        )
                logging.info('Grant Created: %s', create_grant_response)

                response_create_version = boto3.client('license-manager', region_name='us-east-1').create_grant_version(
                    ClientToken=f"token_{round(time.time() * 1000)}",
                    GrantArn=create_grant_response['GrantArn'],
                    Status='ACTIVE'
                )
                
                logging.info('Grant Version Created: %s', response_create_version)
                

                        
            except ClientError as e:
                logging.error(e)
                

    
