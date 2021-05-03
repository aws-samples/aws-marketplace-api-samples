#!/bin/bash

# Set details JSON
DETAILS_JSON_AS_STRING=$(echo '
{
  "Version": {
    "ReleaseNotes": "Updated release notes"
  },
  "DeliveryOptions": [
    {
      "Id": "00000000-0000-0000-0000-000000000000",
      "Details": {
        "AmiDeliveryOptionDetails": {
          "UsageInstructions": "Updated usage instructions",
          "RecommendedInstanceType": "m4.2xlarge",
          "AccessEndpointUrl": {
            "Port": 8080,
            "Protocol": "https",
            "RelativePath": "index.html"
          },
          "SecurityGroups": [
            {
              "IpProtocol": "tcp",
              "FromPort": 443,
              "ToPort": 443,
              "IpRanges": [
                "0.0.0.0/0"
              ]
            }
          ]
        }
      }
    }
  ]
}' | jq "tostring")

# Run AWS CLI command
# Entity identifier is same as the productId
aws marketplace-catalog start-change-set \
  --catalog "AWSMarketplace" \
  --change-set '[
      {
        "ChangeType": "UpdateDeliveryOptions",
        "Entity": {
          "Identifier": "10000000-0000-0000-0000-000000000000",
          "Type": "AmiProduct@1.0"
        },
        "Details": '"${DETAILS_JSON_AS_STRING}"'
      }
    ]';