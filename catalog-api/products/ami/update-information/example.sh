#!/bin/bash

# Set details JSON
# Properties that don't require an update can be omitted
DETAILS_JSON_AS_STRING=$(echo '
{
  "ProductTitle": "Sample product",
  "ShortDescription": "Brief description",
  "LongDescription": "Detailed description",
  "Sku": "SKU-123",
  "LogoUrl": "https://s3.amazonaws.com/logos/sample.png",
  "VideoUrls": [
    "https://sample.amazonaws.com/awsmp-video-1",
    "https://sample.amazonaws.com/awsmp-video-2"
  ],
  "Highlights": [
    "Sample highlight"
  ],
  "AdditionalResources": [
    {
      "Text": [
        "Sample resource",
        "https://sample.amazonaws.com"
      ]
    }
  ],
  "SupportDescription": "Product support information",
  "Categories": [
    "Operating Systems"
  ],
  "SearchKeywords": [
    "Sample keyword"
  ]
}' | jq "tostring")

# Run AWS CLI command
# Entity identifier is same as the productId
aws marketplace-catalog start-change-set \
  --catalog "AWSMarketplace" \
  --change-set '[
      {
        "ChangeType": "UpdateInformation",
        "Entity": {
          "Identifier": "10000000-0000-0000-0000-000000000000",
          "Type": "AmiProduct@1.0"
        },
        "Details": '"${DETAILS_JSON_AS_STRING}"'
      }
    ]';