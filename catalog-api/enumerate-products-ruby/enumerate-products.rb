# frozen_string_literal: true

require 'aws-sdk-marketplacecatalog'

Aws.config.update(
  region: ENV['AWS_REGION'] || 'us-east-1',
  credentials: Aws::Credentials.new(
    ENV['AWS_ACCESS_KEY_ID'],
    ENV['AWS_SECRET_ACCESS_KEY'],
    ENV['AWS_SESSION_TOKEN']
  )
)

catalog_name = 'AWSMarketplace'

catalog = Aws::MarketplaceCatalog::Client.new

product_types = ['DataProduct']

product_types.each do |product_type|
  begin
    puts "Enumerating your #{product_type}s ..."

    entities = catalog.list_entities(
      catalog: catalog_name,
      entity_type: product_type,
    ).entity_summary_list

    entities.each do |entity|
      puts " #{entity.entity_id}: #{entity.name}"

      # entity details
      # described_entity = catalog.describe_entity(catalog: catalog_name, entity_id: entity.entity_id)
      # described_entity_details = JSON.parse(described_entity.details)
    end
  rescue Aws::MarketplaceCatalog::Errors::ServiceError => e
    puts " skipping, #{e.message}"
  end
end

puts 'Done.'
