from typing import List, Optional
import os
import re
import logging
import pandas as pd
from simple_salesforce import Salesforce, SalesforceMalformedRequest, SalesforceAuthenticationFailed

class SalesforceAccountFetcher:
    """
    Fetch all Account records from Salesforce using simple_salesforce.
    Uses the verified list of fields from the Salesforce 'Account' object.
    """

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        security_token: Optional[str] = None,
        domain: Optional[str] = None,  # use "test" for sandbox
        consumer_key: Optional[str] = None,
        consumer_secret: Optional[str] = None,
    ):
        # prefer explicit args, then env vars
        self.username = username or os.getenv("SF_USERNAME")
        self.password = password or os.getenv("SF_PASSWORD")
        self.security_token = security_token or os.getenv("SF_SECURITY_TOKEN")
        self.domain = domain or os.getenv("DOMAIN") or "login"
        self.consumer_key = consumer_key or os.getenv("CONSUMER_KEY")
        self.consumer_secret = consumer_secret or os.getenv("CONSUMER_SECRET")

        try:
            if self.consumer_key and self.consumer_secret:
                self.sf = Salesforce(
                    consumer_key=self.consumer_key,
                    consumer_secret=self.consumer_secret,
                    domain=self.domain,
                )
            else:
                if not (self.username and self.password):
                    raise ValueError("Salesforce username and password must be provided.")
                self.sf = Salesforce(
                    username=self.username,
                    password=self.password,
                    security_token=self.security_token,
                    domain=self.domain,
                )
        except SalesforceAuthenticationFailed as e:
            raise RuntimeError(f"Salesforce auth failed: {e}")
        except Exception as e:
            raise RuntimeError(f"Salesforce connection failed: {e}")

    # Exact verified Salesforce Account fields (262 total)
    DEFAULT_ACCOUNT_FIELDS = [
        "Acccount_Owner_Booking_Link__c",
        "CurrencyIsoCode",
        "Account_ICP_Score__c",
        "Account_Id_18__c",
        "Name",
        "AccountNumber",
        "OwnerId",
        "RecordTypeId",
        "Site",
        "AccountSource",
        "Account_Type__c",
        "chargebeeapps__Active_Cb_Id__c",
        "AM_Email_Address__c",
        "AM_Name__c",
        "AnnualRevenue",
        "chargebeeapps__AutoCloseInvoices__c",
        "chargebeeapps__Auto_Collection__c",
        "Average_Bill_Amount__c",
        "BillingAddress",
        "Booking_Methods__c",
        "Concurrent__c",
        "Bookings_Last_30_Days__c",
        "chargebeeapps__Business_Entity__c",
        "Business_Issue__c",
        "PersonActionCadenceId",
        "PersonActionCadenceAssigneeId",
        "temp_Campaign_Tags__c",
        "ChannelProgramLevelName",
        "ChannelProgramName",
        "chargebeeapps__Billing_Contact__c",
        "chargebeeapps__Email__c",
        "chargebeeapps__First_Name__c",
        "chargebeeapps__CB_Id__c",
        "chargebeeapps__Last_Name__c",
        "chargebeeapps__CB_Site__c",
        "Client_Context__c",
        "Client_Needs__c",
        "Collection_Plans_Total_Dues__c",
        "COM__c",
        "COM_Notes__c",
        "IsCompatibleZenchefPay__c",
        "Contract_Start_Date__c",
        "Converted_from_Lead_Id__c",
        "Count_Active_Price_Increase__c",
        "Count_Active_Price_Increase_text__c",
        "Count_Active_Subscriptions__c",
        "Count_A_no_Opportunities__c",
        "Count_Child_Accounts__c",
        "Count_Churns__c",
        "Count_Collection_Plans__c",
        "Count_Migration_Opportunity__c",
        "Count_Open_Opportunities__c",
        "Count_Prospection_Opportunities__c",
        "Count_Sales_Opportunities__c",
        "Country__c",
        "CountSelfOB__c",
        "CreatedById",
        "Credit_Card_Deposit_Last_Month__c",
        "CSM__c",
        "Customer_Data__c",
        "IsCustomerPortal",
        "Customer_Segment__c",
        "Customer_Segment_Override__c",
        "Customer_Segment_Override_Reason__c",
        "Customer_Tiering__c",
        "Jigsaw",
        "Last_Active_Churn_Date__c",
        "IsDesirable__c",
        "Distinction_CADHI__c",
        "Distinction_Collectionneurs__c",
        "Distinction_College_Culinaire__c",
        "Distinction_Ecotable__c",
        "Distinction_Figaroscope__c",
        "Distinction_Fooding__c",
        "Distinction_Fork_Insider__c",
        "Distinction_Gault_Millau__c",
        "Distinction_Gault_Millau_Score__c",
        "Distinction_Gault_Millau_URL__c",
        "Distinction_Hotel__c",
        "Distinction_Jeunes_Restaurateurs__c",
        "Distinction_Lebey__c",
        "Distinction_Michelin__c",
        "Distinction_Michelin_URL__c",
        "Distinction_MOF__c",
        "Distinction_Owner__c",
        "Distinction_Relais_Chateaux__c",
        "Distinction_White_Guide__c",
        "Distinction_World_Best__c",
        "Duplicate_Account__c",
        "Tier",
        "Email__c",
        "NumberOfEmployees",
        "End_Of_Season_Date__c",
        "IsExcludedFromRealign",
        "IsVATNumberExempted__c",
        "Fax",
        "Feature_Disinterests__c",
        "Feature_Interests__c",
        "Features__c",
        "Future_Opening__c",
        "Generated_Password__c",
        "HasGoogleAcceptBookingsExtension__c",
        "Google_Data_ID__c",
        "HasGoogleError__c",
        "HasGoogleTerraceExtension__c",
        "Google_Open_State__c",
        "Google_Place_ID__c",
        "Google_Price__c",
        "Google_Rating__c",
        "Google_Raw_Booking_System__c",
        "Google_Reviews__c",
        "Google_Thumbnail__c",
        "Google_Thumbnail_URL__c",
        "Google_Type__c",
        "Google_Updated_Date__c",
        "Google_URL__c",
        "Pr_sence_sur_Guide__c",
        "HasParentAccount__c",
        "HasPinnedTask__c",
        "HasVouchers__c",
        "Health_Score__c",
        "Health_Score_Image__c",
        "Hotel_Restaurant__c",
        "Industry",
        "Instagram_URL__c",
        "Instagram_Followers__c",
        "IsCustomer__c",
        "chargebeeapps__Is_Deprecated__c",
        "IsMigrated__c",
        "chargebeeapps__Is_Synced__c",
        "IsBlockingRestaurant__c",
        "IsDectivatingOnWeekday__c",
        "IsKeyAccount__c",
        "IsMyAccount__c",
        "IsOwnerActive__c",
        "IsSeasonal__c",
        "IsSyncAccountZendesk__c",
        "IsTest__c",
        "IsTestingPendo__c",
        "IsZenchefIDOrUID__c",
        "Grand_Compte__c",
        "Last_Active_CB_Subscription__c",
        "Last_Close_Date_Month__c",
        "Last_Created_Churn_Date__c",
        "Last_Lost_Opportunity_Close_Date__c",
        "Customer_Onboarding__c",
        "LastModifiedById",
        "Last_Onboarding_Requested_Start_Date__c",
        "Last_Onboarding_Typeform_Completed__c",
        "Last_Onboarding_Typeform_Completed_Date__c",
        "Last_Opportunity_Close_Date__c",
        "Last_Opportunity_Created_Date__c",
        "Last_Payment_Method_Updated_At__c",
        "Last_Pulse_Modified_Date__c",
        "Last_Won_Deal_Date__c",
        "Last_Won_Deal_DateTime__c",
        "Last_Won_Deal_Morning_Date__c",
        "Last_Won_Opportunity_Owner_Name__c",
        "Life_Time__c",
        "Lifecycle_Stage__c",
        "chargebeeapps__Locale__c",
        "Manual_Bookings_Last_30_Days__c",
        "IsMichelinStar__c",
        "temp_Mission_1_Owner__c",
        "temp_Mission_2_Owner__c",
        "temp_Mission_3_Owner__c",
        "MRR__c",
        "chargebeeapps__Net_Term_Days__c",
        "Next_Billing_Date__c",
        "NPS_Rating__c",
        "NPS_Category__c",
        "NPS_Rating_Date__c",
        "NPS_Score__c",
        "Onboarder__c",
        "Onboarding_Stage__c",
        "OpeningDate__c",
        "OperatingHoursId",
        "Owner_Email__c",
        "Ownership",
        "Pain_Points__c",
        "ParentId",
        "Sum_of_Parent_MRR__c",
        "IsPartner",
        "chargebeeapps__Payment_Method_Status__c",
        "chargebeeapps__Payment_Method_Type__c",
        "Payment_Velocity__c",
        "Phone",
        "POS__c",
        "Acc_Power_of_One__c",
        "IsPremium__c",
        "Previous_CSM__c",
        "Principal_Reason_of_Last_Churn_actif__c",
        "Product_Origin__c",
        "UID__c",
        "Prospect_Scoring__c",
        "Prospection_Behavorial_Score__c",
        "Prospection_Demographic_Score__c",
        "Prospection_Score__c",
        "Prospection_Status__c",
        "Prospection_Status_Complement__c",
        "Prospection_Status_Detail__c",
        "Pulse__c",
        "Pulse_Comment__c",
        "Rating",
        "RecordType_ID__c",
        "RecordType_Picklist__c",
        "RecordType_Text__c",
        "chargebeeapps__Resource_Version__c",
        "Restaurant_Capacity__c",
        "Restaurant_Context__c",
        "Nom_du_restaurant__c",
        "Sale__c",
        "Description",
        "Sales_Talking_Point__c",
        "Scoring_Label__c",
        "Scoring_Ciblage__c",
        "Season_Start_Date__c",
        "Seasonal__c",
        "ShippingAddress",
        "Sic",
        "SicDesc",
        "SIREN__c",
        "SIRET__c",
        "Source_Complement__c",
        "Source_Detail__c",
        "SourceSystemIdentifier",
        "Subscription_Due_Since__c",
        "Subscription_Total_Dues__c",
        "chargebeeapps__Sync_with_Chargebee__c",
        "Target_MRR__c",
        "chargebeeapps__Taxability__c",
        "Territory_Country__c",
        "Territory_Geographic__c",
        "Territory_Owner_Id__c",
        "Territory_Region__c",
        "Territory_Zone__c",
        "TickerSymbol",
        "Total_Dues__c",
        "Tripadvisor_Ranking__c",
        "Tripadvisor_Rating__c",
        "Tripadvisor_URL__c",
        "Type",
        "Typeform_Completed__c",
        "Typeform_Completed_Date__c",
        "typeform__Typeform_Form_Mapping__c",
        "Typeform_Onboarding_Link__c",
        "Upsell_Potential_Details__c",
        "HasUpsellPotentialIdentified__c",
        "Upsert_ID__c",
        "chargebeeapps__VAT_Number__c",
        "VAT_Number__c",
        "FrenchVAT__c",
        "Vouchers_Date__c",
        "WasSoldZenchefPay__c",
        "Website",
        "Zenchef_ID__c",
        "Zenchef_Link__c",
        "Zenchef_Pay_First_Payment_Date__c",
        "Zenchef_Pay_Payments__c",
        "Zenchef_Payments_Amount_Last_30_Days__c",
        "Zenchef_Payments_Last_30_Days__c",
        "Zendesk_ID__c",
    ]

    def _filter_valid_fields(self, fields: List[str]) -> List[str]:
        try:
            desc = self.sf.Account.describe()
            api_field_names = {f.get("name") for f in desc.get("fields", [])}
        except Exception as e:
            logging.warning("Could not describe Account object: %s", e)
            return fields

        valid = [f for f in fields if f in api_field_names]
        invalid = [f for f in fields if f not in api_field_names]
        if invalid:
            logging.warning("Invalid fields skipped: %s", invalid)
        return valid

    def get_all_accounts(self, fields: Optional[List[str]] = None) -> List[dict]:
        if fields is None:
            fields = list(self.DEFAULT_ACCOUNT_FIELDS)

        fields = self._filter_valid_fields(fields)
        if not fields:
            raise ValueError("No valid Account fields remain.")

        soql = f"SELECT {', '.join(fields)} FROM Account limit 5000"
        try:
            result = self.sf.query_all(soql)
        except SalesforceMalformedRequest as e:
            raise RuntimeError(f"Malformed query: {e}")
        records = result.get("records", [])
        for r in records:
            r.pop("attributes", None)
        return records

    def get_all_accounts_df(self, fields: Optional[List[str]] = None, normalize: bool = False) -> pd.DataFrame:
        records = self.get_all_accounts(fields=fields)
        if not records:
            return pd.DataFrame()
        if normalize:
            try:
                return pd.json_normalize(records)
            except Exception:
                return pd.DataFrame(records)
        return pd.DataFrame(records)


if __name__ == "__main__":
    fetcher = SalesforceAccountFetcher()
    accounts = fetcher.get_all_accounts()
    print(f"Fetched {len(accounts)} accounts")
    if accounts:
        print(list(accounts[0].keys()))
