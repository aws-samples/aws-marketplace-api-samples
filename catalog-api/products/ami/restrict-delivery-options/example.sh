#!/bin/bash

# Set the title of version to restrict
VERSION_TITLE_TO_RESTRICT="new-version"

# Extract DeliveryOptionId for the version to restrict
DELIVERY_OPT_ID=$(aws marketplace-catalog describe-entity \
  --catalog AWSMarketplace \
  --entity-id 10000000-0000-0000-0000-000000000000 \
  --query 'DetailsDocument' |
  jq '.Versions[] | select(.VersionTitle == "'"${VERSION_TITLE_TO_RESTRICT}"'") | .DeliveryOptions[].Id')

if [ -z "$DELIVERY_OPT_ID" ]; then
  echo "Version with title '${VERSION_TITLE_TO_RESTRICT}' not found"
  exit 1
fi

# Set details JSON
DETAILS_JSON=$(echo '
{
  "DeliveryOptionIds": [
    '${DELIVERY_OPT_ID}'
  ]
}')

# Run AWS CLI command
# Entity identifier is same as the productId
aws marketplace-catalog start-change-set \
  --catalog "AWSMarketplace" \
  --change-set '[
      {
        "ChangeType": "RestrictDeliveryOptions",
        "Entity": {
          "Identifier": "10000000-0000-0000-0000-000000000000",
          "Type": "AmiProduct@1.0"
        },
        "DetailsDocument": '${DETAILS_JSON}'
      }
    ]'