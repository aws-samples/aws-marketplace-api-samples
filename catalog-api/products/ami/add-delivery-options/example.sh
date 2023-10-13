#!/bin/bash

# Run AWS CLI command
# Entity identifier is same as the productId
aws marketplace-catalog start-change-set \
  --catalog "AWSMarketplace" \
  --change-set '[
      {
        "ChangeType": "AddDeliveryOptions",
        "Entity": {
          "Identifier": "00000000-0000-0000-0000-000000000000",
          "Type": "AmiProduct@1.0"
        },
        "DetailsDocument": {
          "Version": {
            "VersionTitle": "Sample title",
            "ReleaseNotes": "Sample release notes"
          },
          "DeliveryOptions": [
            {
              "Details": {
                "AmiDeliveryOptionDetails": {
                  "AmiSource": {
                    "AmiId": "ami-12345678",
                    "AccessRoleArn": "arn:aws:iam::123456789012:role/sampleRole",
                    "UserName": "Sample username",
                    "OperatingSystemName": "Sample OS",
                    "OperatingSystemVersion": "Sample OS Version"
                  },
                  "UsageInstructions": "Sample usage instructions",
                  "RecommendedInstanceType": "m4.xlarge",
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