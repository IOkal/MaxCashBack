import json
import requests
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('YourDynamoDBTableName')  # Replace with your DynamoDB table name

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

def scrape_section(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    cashback_data = []
    
    # Extract relevant data for each retailer on the page
    for item in soup.find_all('td', class_='listlogocell'):
        retailer_name = item.find_next('a', class_='listshopname').get_text(strip=True)
        cashback = item.find_next('span', class_='listrebate').get_text(strip=True)
        
        # Check if the cashback contains "Up to"
        if "Up to" in cashback:
            cashback = "UP TO " + cashback.replace("Up to", "").strip()
        else:
            cashback = cashback.strip()
        
        cashback_data.append({
            'RetailerName': retailer_name,
            'Cashback': cashback,
            'URL': url
        })
    
    return cashback_data

def store_data(data):
    for entry in data:
        try:
            table.put_item(Item=entry)
        except ClientError as e:
            print(f"Error inserting cashback rate for {entry['RetailerName']}: {e.response['Error']['Message']}")

def lambda_handler(event, context):
    all_cashback_data = []
    
    for section_url in sections:
        cashback_data = scrape_section(section_url)
        store_data(cashback_data)
        all_cashback_data.extend(cashback_data)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data scraped and stored successfully!')
    }
