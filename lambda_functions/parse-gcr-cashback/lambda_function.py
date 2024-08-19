import requests
from bs4 import BeautifulSoup
import boto3
import hashlib
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('YourDynamoDBTableName')

def scrape_section(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError if the HTTP request returned an unsuccessful status code
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for URL {url}: {str(e)}")
        return []  # Return an empty list if the request fails

    soup = BeautifulSoup(response.content, 'html.parser')
    cashback_items = []
    
    for item in soup.find_all('tr'):
        try:
            retailer_name_element = item.find('a', class_='listshopname')
            cashback_span = item.find_next('span', class_='listrebate')
            
            if retailer_name_element and cashback_span:
                retailer_name = retailer_name_element.get_text(strip=True)
                cashback = cashback_span.get_text(strip=True)
                
                # Handle 'Up to' case
                if 'Up to' in cashback:
                    cashback = cashback.replace('Up to', '').strip()
                    cashback = f"UP TO {cashback}"
                
                retailer_id = hashlib.md5((retailer_name + url).encode('utf-8')).hexdigest()
                
                cashback_items.append({
                    'RetailerIDWebsiteName': retailer_id,
                    'RetailerName': retailer_name,
                    'Cashback': cashback,
                    'WebsiteName': url
                })
            else:
                logger.warning(f"Missing retailer name or cashback information for item in URL {url}")
        
        except Exception as e:
            logger.error(f"Error processing item in URL {url}: {str(e)}")
            continue  # Skip to the next item if an error occurs

    return cashback_items

def lambda_handler(event, context):
    section_urls = [
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
    
    for section_url in section_urls:
        logger.info(f"Scraping section URL: {section_url}")
        cashback_data = scrape_section(section_url)
        
        for item in cashback_data:
            try:
                table.put_item(Item=item)
                logger.info(f"Successfully inserted cashback rate for {item['RetailerName']}")
            except Exception as e:
                logger.error(f"Error inserting cashback rate for {item['RetailerName']}: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': 'Scraping and insertion complete'
    }

