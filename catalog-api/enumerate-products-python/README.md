# Catalog API: Enumerate Products

This sample uses the [AWS Marketplace Catalog API](https://docs.aws.amazon.com/marketplace-catalog/latest/api-reference/welcome.html) to enumerate your products in the AWS Marketplace catalog.

To run the sample, set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` and `AWS_REGION`.

### Install Dependencies

Install with `pipenv`.

```bash
catalog-api/enumerate-products-python> python3 -m pipenv install

Locking [dev-packages] dependencies...
Locking [packages] dependencies...
Building requirements...
Resolving dependencies...
✔ Success! 
To activate this project's virtualenv, run pipenv shell.
Alternatively, run a command inside the virtualenv with pipenv run.
```

### Run Sample

```bash
export export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

catalog-api/enumerate-products-python> python3 -m pipenv run ./enumerate-products.py 
```
