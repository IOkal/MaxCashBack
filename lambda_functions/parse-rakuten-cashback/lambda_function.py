import boto3
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb')

# Table references
retailers_table = dynamodb.Table('Retailers')
cashback_table = dynamodb.Table('CashbackRates')

def lambda_handler(event, context):
    # Fetch Rakuten.ca store page
    url = "https://www.rakuten.ca/stores"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Parse each store block
    stores = soup.find_all('div', class_='promo-store-block')
    
    for store in stores:
        store_name = store.find('a', class_='store-name').text.strip()
        store_url = store.find('a', class_='store-name')['href'].strip()
        cashback_text = store.find('span', class_='now_rebate').text.strip()

        # Extract cashback percentage and check if it's an "up to" value
        cashback_rate, is_up_to = parse_cashback(cashback_text)
        
        # Create a unique ID for the retailer (based on the store name)
        retailer_id = hashlib.md5(store_name.encode()).hexdigest()
        
        # Add retailer to the Retailers table if not exists
        add_retailer_if_not_exists(retailer_id, store_name, store_url)
        
        # Insert the cashback rate into the CashbackRates table
        insert_cashback_rate(retailer_id, 'rakuten.ca', cashback_rate, is_up_to)

def parse_cashback(cashback_text):
    cashback_text = cashback_text.lower().replace("cash back", "").strip()
    
    is_up_to = "up to" in cashback_text
    if is_up_to:
        cashback_text = cashback_text.replace("up to", "").strip()
    
    cashback_rate = cashback_text.replace("%", "").strip()
    return cashback_rate, is_up_to

def add_retailer_if_not_exists(retailer_id, retailer_name, store_url):
    try:
        # Check if the retailer already exists
        response = retailers_table.get_item(Key={'RetailerID': retailer_id})
        if 'Item' not in response:
            # Insert the new retailer
            retailers_table.put_item(
                Item={
                    'RetailerID': retailer_id,
                    'RetailerName': retailer_name,
                    'WebsiteAliases': {'rakuten.ca': store_url},
                    'CreatedAt': datetime.utcnow().isoformat(),
                    'UpdatedAt': datetime.utcnow().isoformat()
                }
            )
    except Exception as e:
        print(f"Error adding retailer {retailer_name}: {e}")

def insert_cashback_rate(retailer_id, website_name, cashback_rate, is_up_to):
    try:
        # Insert cashback rate with the "up to" flag and timestamp
        cashback_table.put_item(
            Item={
                'RetailerID': retailer_id,
                'WebsiteName': website_name,
                'CashbackRate': cashback_rate,
                'IsUpTo': is_up_to,
                'Timestamp': datetime.utcnow().isoformat(),
                'CreatedAt': datetime.utcnow().isoformat(),
                'UpdatedAt': datetime.utcnow().isoformat()
            }
        )
    except Exception as e:
        print(f"Error inserting cashback rate for {retailer_id}: {e}")

