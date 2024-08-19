import json
import boto3
import requests
from bs4 import BeautifulSoup
import datetime

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')

# Function to update Retailers table with new website aliases
def update_retailer_website_aliases(retailer_id, new_alias, retailer_name):
    table = dynamodb.Table('Retailers')
    
    try:
        response = table.update_item(
            Key={
                'RetailerID': retailer_id
            },
            UpdateExpression="SET WebsiteAliases = :wa, UpdatedAt = :ua",
            ExpressionAttributeValues={
                ':wa': new_alias,
                ':ua': datetime.datetime.now().isoformat()
            },
            ReturnValues="UPDATED_NEW"
        )
        print(f"Updated RetailerID {retailer_id} with new alias {new_alias} for {retailer_name}")
    except Exception as e:
        print(f"Error updating RetailerID {retailer_id} for {retailer_name}: {str(e)}")

# Function to insert cashback rate data into CashbackRates-GCR table
def insert_cashback_rate_gcr(data):
    table = dynamodb.Table('CashbackRates-GCR')
    try:
        table.put_item(Item=data)
        print(f"Inserted cashback rate for {data['RetailerIDWebsiteName']}")
    except Exception as e:
        print(f"Error inserting cashback rate for {data['RetailerIDWebsiteName']}: {str(e)}")

# Function to scrape cashback data from a given section URL
def scrape_section(section_url):
    response = requests.get(section_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    for item in soup.find_all('fieldset', class_='smallbox nolegend'):
        try:
            retailer_name = item.find('a', class_='listshopname').get_text(strip=True)
            cashback = item.find_next('span', class_='listrebate').get_text(strip=True)
            is_up_to = 'Up to' in item.find_next('span', class_='listrebate').parent.get_text(strip=True)

            retailer_id = "generate_or_lookup_id_based_on_logic"  # Replace with actual logic to determine RetailerID
            retailer_id_website_name = f"{retailer_id}_{item.find('a', class_='listshopname')['href']}"

            cashback_data = {
                'RetailerIDWebsiteName': retailer_id_website_name,
                'Timestamp': datetime.datetime.now().isoformat(),
                'CashbackRate': cashback,
                'CreatedAt': datetime.datetime.now().isoformat(),
                'IsUpTo': is_up_to,
                'RetailerID': retailer_id,
                'UpdatedAt': datetime.datetime.now().isoformat(),
                'WebsiteName': 'greatcanadianrebates.ca'
            }

            insert_cashback_rate_gcr(cashback_data)

            # Update Retailers table with the new alias if needed
            new_alias = json.dumps({ "greatcanadianrebates.ca": { "S": item.find('a', class_='listshopname')['href'] } })
            update_retailer_website_aliases(retailer_id, new_alias, retailer_name)
        
        except AttributeError as e:
            print(f"Error processing an item: {str(e)}")

# Main Lambda handler function
def lambda_handler(event, context):
    sections = [
        "https://www.greatcanadianrebates.ca/display/Apparel/",
        "https://www.greatcanadianrebates.ca/display/Flowers/",
        "https://www.greatcanadianrebates.ca/display/Automotive/",
        "https://www.greatcanadianrebates.ca/display/Groceries/",
        "https://www.greatcanadianrebates.ca/display/Baby/",
        "https://www.greatcanadianrebates.ca/display/Health/",
        "https://www.greatcanadianrebates.ca/display/BMM/",
        "https://www.greatcanadianrebates.ca/display/Home/",
        "https://www.greatcanadianrebates.ca/display/Business/",
        "https://www.greatcanadianrebates.ca/display/Jewelry/",
        "https://www.greatcanadianrebates.ca/display/Hobbies/",
        "https://www.greatcanadianrebates.ca/display/Pets/",
        "https://www.greatcanadianrebates.ca/display/Computers/",
        "https://www.greatcanadianrebates.ca/display/Services/",
        "https://www.greatcanadianrebates.ca/display/Electronics/",
        "https://www.greatcanadianrebates.ca/display/Sporting/",
        "https://www.greatcanadianrebates.ca/display/Finance/",
        "https://www.greatcanadianrebates.ca/display/Toys/",
        "https://www.greatcanadianrebates.ca/display/Gift-Cards/",
        "https://www.greatcanadianrebates.ca/display/Travel/"
    ]

    for section_url in sections:
        scrape_section(section_url)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Scraping and data insertion completed successfully.')
    }
