#!/bin/bash

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
        "DetailsDocument": {
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
        }
      }
    ]';