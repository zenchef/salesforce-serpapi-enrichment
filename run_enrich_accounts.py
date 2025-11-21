"""
Example script to enrich Salesforce Account records with Google data via SerpAPI.

This script:
1. Connects to Salesforce
2. Queries Account records (you can customize the SOQL query)
3. Calls SerpAPI to enrich with Google Maps data
4. Saves results to CSV
5. Optionally updates Salesforce with enriched data
"""
import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce
import pandas as pd
from enrich_accounts_serpapi import enrich_with_serpapi

# Load environment variables
load_dotenv()

# Connect to Salesforce
print("=" * 80)
print("SALESFORCE ACCOUNT ENRICHMENT WITH SERPAPI")
print("=" * 80)

print("\n1. Connecting to Salesforce...")
sf = Salesforce(
    username=os.getenv("SF_USERNAME"),
    password=os.getenv("SF_PASSWORD"),
    security_token=os.getenv("SF_TOKEN")
)
print("   ✅ Connected")

# Query accounts - customize this SOQL as needed
print("\n2. Querying Accounts...")
soql = """
    SELECT Id, Name, Website, BillingStreet, BillingCity, 
           BillingPostalCode, BillingCountry, Phone
    FROM Account
    WHERE RecordType.DeveloperName = 'Customer'
    AND BillingCountry IN ('France', 'Belgium', 'Switzerland')
    AND Google_Place_ID__c = NULL
    AND Id = '001AZ000002wu8wYAA'
    LIMIT 1
"""

result = sf.query_all(soql)
records = result['records']
print(f"   Found {len(records)} accounts")

if not records:
    print("\n   ⚠️  No records found. Adjust your SOQL query.")
    exit(0)

# Convert to DataFrame
print("\n3. Converting to DataFrame...")
# Remove 'attributes' metadata from Salesforce
for r in records:
    if 'attributes' in r:
        del r['attributes']
df = pd.DataFrame(records)
print(f"   DataFrame shape: {df.shape}")
print(f"   Columns: {', '.join(df.columns)}")

# Enrich with SerpAPI
print("\n4. Enriching with SerpAPI (this may take a few minutes)...")
api_key = os.getenv("SERPAPI_API_KEY")
if not api_key:
    print("   ❌ SERPAPI_API_KEY not found in .env file")
    exit(1)

from datetime import datetime

try:
    # Generate timestamped filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"accounts_enriched_{timestamp}.csv"
    
    enriched_df = enrich_with_serpapi(
        df,
        api_key=api_key,
        workers=3,  # Adjust based on your SerpAPI plan limits
        pause=0.5,  # Pause between requests to avoid rate limits
        engine="google_maps",
        save_csv=csv_filename,
        max_retries=3,
        hl="fr",  # Language
        gl="fr",  # Geographic location
        validate_restaurant=True  # ✅ Only accept restaurants
    )
except Exception as e:
    print(f"   ❌ SerpAPI enrichment failed: {e}")
    print(f"   Check your SERPAPI_API_KEY and account limits")
    exit(1)

print(f"\n5. Enrichment complete!")
print(f"   Enriched {len(enriched_df)} accounts")
print(f"   Saved to: {csv_filename}")

# Show sample results
print("\n6. Sample results:")
display_cols = ["Id", "Name", "Google_Rating__c", "Google_Review_Count__c", 
                "Google_Place_ID__c", "Restaurant_Type__c"]
available_cols = [c for c in display_cols if c in enriched_df.columns]
print(enriched_df[available_cols].head(10).to_string(index=False))

# Optional: Update Salesforce with enriched data
update_sf = input("\n\nUpdate Salesforce with enriched data? (y/N): ").strip().lower()
if update_sf == 'y':
    print("\n7. Updating Salesforce...")
    
    # Fields to update (only Google fields we enriched)
    update_fields = [
        "Google_Place_ID__c",
        "Google_Data_ID__c",
        "Google_Rating__c",
        "Google_Review_Count__c",
        "Google_Price__c",
        "Google_Updated_Date__c",
        "Restaurant_Type__c",
        "Has_Google_Accept_Bookings_Extension__c",
        "Prospection_Status__c"
    ]
    
    updated_count = 0
    failed_count = 0
    
    for idx, row in enriched_df.iterrows():
        account_id = row['Id']
        
        # Build update dict with only non-null enriched values
        update_data = {}
        for field in update_fields:
            if field in row and pd.notna(row[field]) and row[field] != '':
                update_data[field] = row[field]
        
        if not update_data:
            continue
        
        try:
            sf.Account.update(account_id, update_data)
            updated_count += 1
            print(f"   ✅ Updated {account_id} ({row.get('Name', 'N/A')})")
        except Exception as e:
            failed_count += 1
            print(f"   ❌ Failed to update {account_id}: {e}")
    
    print(f"\n   Summary: {updated_count} updated, {failed_count} failed")
else:
    print("\n   Skipping Salesforce update")

print("\n" + "=" * 80)
print("ENRICHMENT COMPLETE")
print("=" * 80)
