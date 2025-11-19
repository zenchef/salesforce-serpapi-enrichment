# Salesforce SerpAPI Enrichment

Enrich Salesforce Account records with Google Maps data (ratings, reviews, place IDs, price levels) using SerpAPI.

## Features

- **Parallel Processing**: Uses ThreadPoolExecutor for efficient batch enrichment
- **Defensive Parsing**: Handles varying SerpAPI response formats gracefully
- **Restaurant Validation**: Optional filtering to ensure results are restaurants
- **Retry Logic**: Built-in exponential backoff for transient API errors
- **Flexible Queries**: Uses Website, Name, or existing Google Place IDs from Account records

## Installation

```bash
pip install google-search-results pandas
```

## Usage

```python
import pandas as pd
from enrich_accounts_serpapi import enrich_with_serpapi

# Load your Salesforce Account data
df = pd.read_csv("accounts.csv")  # Must have 'Id' column

# Enrich with Google Maps data
enriched_df = enrich_with_serpapi(
    df,
    api_key="your_serpapi_key",
    workers=5,
    validate_restaurant=True,  # Skip non-restaurant results
    save_csv="enriched_accounts.csv"
)
```

## Parameters

- `df`: DataFrame with at least an `Id` column (plus `Website` or `Name` recommended)
- `api_key`: Your SerpAPI key (or set `SERPAPI_API_KEY` environment variable)
- `workers`: Number of parallel threads (default: 5)
- `pause`: Seconds between requests per worker (default: 0.1)
- `validate_restaurant`: Skip non-restaurant results (default: False)
- `save_csv`: Optional path to save enriched CSV

## Output Fields

The function adds these columns to your DataFrame:

- `Google_Place_ID__c`: Google Place ID
- `Google_Data_ID__c`: Google Data ID
- `Google_Rating__c`: Average rating
- `Google_Review_Count__c`: Number of reviews
- `Google_Price__c`: Price level ($, $$, $$$, $$$$)
- `Restaurant_Type__c`: Business categories
- `Has_Google_Accept_Bookings_Extension__c`: Booking availability
- `Prospection_Status__c`: Business status (e.g., "Permanently Closed")
- `Google_Updated_Date__c`: Data fetch timestamp

## License

MIT
