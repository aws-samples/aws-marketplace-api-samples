### List AmiProduct entities on Catalog API
This sample retrieves all AmiProduct entities owned by a vendor account using ListEntities operation in AWS Marketplace Catalog API. To run the sample, set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, and `AWS_REGION` parameters in terminal session.

```
# Run AWS CLI command
aws marketplace-catalog list-entities \
  --catalog "AWSMarketplace" \
  --entity-type "AmiProduct"
```

**Response Structure**
```
{
    "EntitySummaryList": [
        {
            "Name": "Sample entity name",
            "EntityType": "AmiProduct",
            "EntityId": "00000000-0000-0000-0000-000000000000",
            "EntityArn": "arn:aws:aws-marketplace:us-east-1:123456789012:AWSMarketplace/AmiProduct/00000000-0000-0000-0000-000000000000",
            "LastModifiedDate": "...",
            "Visibility": "Public"
        }
    ],
    "NextToken": "..."
}
```