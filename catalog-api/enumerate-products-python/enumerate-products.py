#!/usr/bin/env python

import boto3

client = boto3.client('marketplace-catalog', region_name='us-east-1')

catalog_name = 'AWSMarketplace'
entity_types = ['DataProduct']

for entity_type in entity_types:
  print("Enumerating " + entity_type)
  try:
    res = client.list_entities(
      Catalog=catalog_name, 
      EntityType=entity_type
    )

    for entity_summary in res['EntitySummaryList']:
      print(' ' + entity_summary['Name'])

  except (client.exceptions.ValidationException, client.exceptions.AccessDeniedException):
    print(" skipping, unauthorized")
    pass