# AWS Marketplace AmiProduct Samples
API samples listed on this page make use of [jq](https://stedolan.github.io/jq/manual/) filters.

#### Entity Management APIs
* [ListEntities](list-entities): Enumerate AMI products on Marketplace that you own
* [DescribeEntity](describe-entity): Retrieve details of an AmiProduct entity

#### Change Management APIs
**StartChangeSet:** Start Catalog API change set using one or more of the following change types
* [add-delivery-options](add-delivery-options/README.md): Add a new version
* [update-delivery-options](update-delivery-options/README.md): Update details of an existing version 
* [restrict-delivery-options](restrict-delivery-options/README.md): Restrict one or more versions
* [update-information](update-information/README.md): Update product information

## API References

* [AWS Marketplace AMI Products](https://docs.aws.amazon.com/marketplace-catalog/latest/api-reference/ami-products.html)
* [AWS Marketplace Catalog API](https://docs.aws.amazon.com/marketplace-catalog/latest/api-reference/welcome.html)
