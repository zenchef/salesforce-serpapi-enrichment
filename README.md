# SalesforceAccountFetcher

This project contains `SalesforceAccountFetcher`, a small helper that fetches Account records from Salesforce (using `simple_salesforce`) and can return them as a Python list-of-dicts or as a pandas `DataFrame`.

## Setup

1. Create a virtualenv and install requirements:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Set your Salesforce credentials as environment variables (or pass them to the constructor):

```bash
export SF_USERNAME=you@example.com
export SF_PASSWORD=yourpassword
export SF_SECURITY_TOKEN=yoursecuritytoken
# set SF_DOMAIN or DOMAIN to 'test' for sandbox orgs
export SF_DOMAIN=login
export DOMAIN=login
```

## Usage

```python
from main import SalesforceAccountFetcher

fetcher = SalesforceAccountFetcher()
# get list of dicts
records = fetcher.get_all_accounts()

# get a pandas DataFrame
df = fetcher.get_all_accounts_df(normalize=False)
print(df.head())

# flatten nested JSON fields (if any)
df_flat = fetcher.get_all_accounts_df(normalize=True)
print(df_flat.head())
```

### Run with an exact fields file

If you have a plain file containing the exact field API names (one per line), set the `ACCOUNTS_FIELDS_FILE` env var to point to it and the script will use that list directly (avoids CSV-based parsing and potential invalid-field filtering):

```bash
export ACCOUNTS_FIELDS_FILE=/path/to/accounts_fields.txt
python main.py
```

The `accounts_fields.txt` file should look like:

```
Id
Name
AccountNumber
OwnerId
BilllingStreet__c
... (one API name per line)
```

### OAuth / consumer key connection

If you prefer to connect using a consumer key / consumer secret (OAuth-style), set the env vars `CONSUMER_KEY` and `CONSUMER_SECRET` and optionally `SF_DOMAIN` (or pass them to the constructor):

```python
from main import SalesforceAccountFetcher

fetcher = SalesforceAccountFetcher(
	consumer_key=os.getenv("CONSUMER_KEY"),
	consumer_secret=os.getenv("CONSUMER_SECRET"),
	# prefer DOMAIN env var, fall back to SF_DOMAIN, default to login
	domain=os.getenv("DOMAIN", os.getenv("SF_DOMAIN", "login")),
)

df = fetcher.get_all_accounts_df()
print(df.head())
```

## Notes
- The code expects `accounts_columns.csv` in the project root to infer fields when you don't pass `fields` explicitly.
- This script does not perform remote calls during install; you must have valid Salesforce credentials to fetch records.
