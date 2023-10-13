### Describe an AmiProduct on Catalog API
This sample retrieves details of an an AmiProduct using DescribeEntity operation in AWS Marketplace Catalog API. To run the sample, set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, and `AWS_REGION` parameters in terminal session.

```commandline
# Run AWS CLI command
aws marketplace-catalog describe-entity \
  --catalog "AWSMarketplace" \
  --entity-id "00000000-0000-0000-0000-000000000000"
```

**Response Structure**
```
{
    "EntityType": "AmiProduct@1.0",
    "EntityIdentifier": "00000000-0000-0000-0000-000000000000",
    "EntityArn": "arn:aws:aws-marketplace:us-east-1:1234567890012:AWSMarketplace/AmiProduct/00000000-0000-0000-0000-000000000000",
    "LastModifiedDate": "...",
    "Details": "<ENTITY_SPECIFIC_DETAILS_AS_A_STRING>",
    "DetailsDocument": <ENTITY_SPECIFIC_DETAILS_AS_JSON>
}
```
`DetailsDocument` attribute in DescribeEntity response contains the entity type specific JSON object.

**AmiProduct Details**
```commandline
# Run AWS CLI command
aws marketplace-catalog describe-entity \
  --catalog "AWSMarketplace" \
  --entity-id "00000000-0000-0000-0000-000000000000" \
  --query DetailsDocument
```

```json
{
   "Versions": [
      {
         "Id": "10000000-0000-0000-0000-000000000000",
         "ReleaseNotes": "Sample release notes",
         "UpgradeInstructions": "Sample upgrade instructions",
         "VersionTitle": "Sample version title",
         "CreationDate": "...",
         "Sources": [
            {
               "Id": "20000000-0000-0000-0000-000000000000",
               "Type": "AmazonMachineImage",
               "Image": "ami-12345678",
               "Architecture": "x86_64",
               "VirtualizationType": "hvm",
               "OperatingSystem": {
                  "Name": "AMAZONLINUX",
                  "Version": "1.0",
                  "Username": "ec2-user",
                  "ScanningPort": 22
               },
               "Compatibility": {
                  "AvailableInstanceTypes": [
                     "m4.xlarge",
                     "c5.large"
                  ],
                  "RestrictedInstanceTypes": [
                     "t2.micro"
                  ]
               }
            }
         ],
         "DeliveryOptions": [
            {
               "Id": "30000000-0000-0000-0000-000000000000",
               "Type": "AmazonMachineImage",
               "SourceId": "20000000-0000-0000-0000-000000000000",
               "ShortDescription": "Sample description",
               "Instructions": {
                  "Usage": "Sample usage instructions"
               },
               "Recommendations": {
                  "SecurityGroups": [
                     {
                        "Protocol": "tcp",
                        "FromPort": 443,
                        "ToPort": 443,
                        "CidrIps": ["0.0.0.0./0"]
                     }
                  ],
                  "InstanceType": "m4.xlarge"
               },
               "Visibility": "Public",
               "Title": "(X86_64) Amazon Machine Image"
            }
         ]
      }
   ],
   "Description": {
      "Highlights": [
         "Sample highlight"
      ],
      "ProductCode": "0000001111",
      "SearchKeywords": [
         "Sample keyword"
      ],
      "ProductTitle": "Sample product title",
      "ShortDescription": "Brief description",
      "LongDescription": "Detailed description",
      "Manufacturer": "Sample vendor name",
      "Visibility": "Public",
      "AssociatedProducts": [],
      "Sku": "SKU-1234",
      "Categories": [
         "Sample category"
      ]
   },
   "PromotionalResources": {
      "LogoUrl": "https://sample.amazonaws.com/logo.png",
      "Videos": [
         {
            "Type": "Link",
            "Title": "Sample video",
            "Url": "https://sample.amazonaws.com/play.html"
         }
      ],
      "AdditionalResources": [
         {
            "Type": "Link",
            "Text": "Sample text",
            "Url": "https://sample.amazonaws.com/learn.html"
         }
      ]
   },
   "SupportInformation": {
      "Description": "Sample information",
      "Resources": []
   },
   "RegionAvailability": {
      "Regions": [
         "us-east-1",
         "eu-west-1"
      ],
      "FutureRegionSupport": "All"
   }
}
```