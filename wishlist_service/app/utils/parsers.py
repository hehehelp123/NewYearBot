import json
from bs4 import BeautifulSoup
import datetime
import logging 
import re
from pathlib import Path


logger = logging.getLogger(__name__)

def convert_russian_short_date(short_date):
    """
    Converts a Russian date string like "25 окт" to "25 октября" (genetive case).
    """
    month_map = {
        "янв": "января",
        "фев": "февраля",
        "мар": "марта",
        "апр": "апреля",
        "май": "мая",
        "июн": "июня",
        "июл": "июля",
        "авг": "августа",
        "сен": "сентября",
        "окт": "октября",
        "ноя": "ноября",
        "дек": "декабря"
    }
    
    parts = short_date.strip().split()
    if len(parts) != 2:
        return short_date
    
    day, month_abbr = parts[0], parts[1].lower()
    full_month = month_map.get(month_abbr, month_abbr)
    
    return f"{day} {full_month}"

def map_date_difference(input_date_str):
    # Dictionary to map Russian month names to numbers
    month_map = {
        'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
        'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
    }
    
    day, month_str = input_date_str.split()
    day = int(day)
    month = month_map[month_str.lower()]
    
    current_year = datetime.datetime.now().year
    input_date = datetime.datetime(current_year, month, day)
    
    current_date = datetime.datetime.now()
    
    delta = (input_date - current_date ).days
    if delta < 5:
        return "<5 дней"
    elif delta < 14:
        return "<14 дней"
    else:
        return "Китайские заначки (Больше 14 дней)"

def parse_yamarket_data(html_content) -> dict:
    product_info = {
        'name': 'Not Found',
        'cost': 'Not Found',
        'delivery_date': 'Not Found',
        'image_url': 'Not Found'
    }

    soup = BeautifulSoup(html_content, 'html.parser')

    title_tag = soup.find('title')
    if title_tag:
        full_title = title_tag.text.strip()
        match = re.search(r'(.+?)\s—\sкупить', full_title)
        product_info['name'] = match.group(1).strip() if match else full_title

    image_meta = soup.find('meta', property='og:image')
    if image_meta:
        product_info['image_url'] = image_meta.get('content')

    cost_span = soup.find('span', {'data-auto': 'snippet-price-current'})
    if cost_span:
        raw_cost = cost_span.get_text(strip=True)
        clean_cost = re.sub(r'[\s\u2009\u00A0]', '', raw_cost)
        product_info['cost'] = clean_cost

    date_pattern = re.compile(r'"deliveryDate"\s*:\s*"([^"]+)"')
    
    match = date_pattern.search(html_content)
    
    if match:
        short_date = match.group(1).strip()
        # Convert and store the date
        product_info['delivery_date'] = map_date_difference(convert_russian_short_date(short_date))
    
    return product_info

def parse_ozon_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    product_data = {
        "name": None,
        "cost": None,
        "currency": None,
        "delivery_date": None,
        "image_url": None
    }

    script_tag = soup.find('script', {'type': 'application/ld+json'})
    if script_tag:
        try:
            json_data = json.loads(script_tag.string)
            product_data['name'] = json_data.get('name')
            product_data['image_url'] = json_data.get('image')
            offers = json_data.get('offers', {})
            product_data['cost'] = offers.get('price')
            product_data['currency'] = offers.get('priceCurrency')
        except (json.JSONDecodeError, AttributeError) as e:
            print(f"Could not parse JSON-LD script tag: {e}")

    delivery_span = soup.find('span', class_='q6b3_0_2-a1')
    if delivery_span:
        product_data['delivery_date'] = delivery_span.get_text(strip=True)
        logging.fatal(product_data['delivery_date'])
        if product_data['delivery_date'] == "завтра" or product_data['delivery_date'] == "послезавтра":
            return product_data
        try:
            product_data['delivery_date'] = map_date_difference(product_data['delivery_date'])
        except Exception as e:
            logging.fatal("Ошибка при парсинге озона: ", e)
            product_data['delivery_date'] = "неизвестно"
    return product_data


def parse_wildberries_data(html_string: str):
    soup = BeautifulSoup(html_string, 'html.parser')

    product_data = {
        'name': 'Not Found',
        'price': 'Not Found',
        'delivery_date': 'Not Found',
        'image_url': 'Not Found',
    }

    try:
        name_element = soup.select_one('h3[class*="productTitle--J2W7I"]')
        if name_element:
            product_data['name'] = name_element.text.strip()
    except Exception as e:
        print(f"Error extracting name: {e}")

    try:
        price_element = soup.select_one('span[class*="priceBlockWalletPrice"]')
        if price_element:
            # Clean up the price string (remove currency symbol, spaces, etc.)
            raw_price : str = price_element.text.strip()
            # In this case, we'll keep the raw string as it contains currency (₽)
            product_data['price'] = raw_price.replace("\xa0", ' ')
    except Exception as e:
        print(f"Error extracting price: {e}")

    try:
        delivery_element = soup.select_one('div[class*="deliveryTitle--zdyCe"]')
        if delivery_element:
            product_data['delivery_date'] = map_date_difference(delivery_element.text.strip())
    except Exception as e:
        print(f"Error extracting delivery date: {e}")

    try:
        image_element = soup.select_one('div[class*="imgContainer--BxsUt"] img')
        
        if not image_element:
            image_element = soup.find('img')
            
        if image_element:
            image_url = image_element.get('data-src-pb') or image_element.get('src')
            if image_url:
                product_data['image_url'] = image_url
            
    except Exception as e:
        print(f"Error extracting image URL: {e}")
        
    return product_data

def parse_aliexpress_data(html_string):
    soup = BeautifulSoup(html_string, 'html.parser')
    product_data = {
        "product_name": None,
        "price": None,
        "image_url": None
    }

    title_tag = soup.find('title')
    if title_tag and title_tag.string:
        name_parts = title_tag.string.split('|')
        product_data["product_name"] = name_parts[0].strip()

    price_tag = soup.find('p', class_=lambda c: c and 'HazeStickyOfferPrice__price' in c)
    if price_tag:
        raw_price = price_tag.text.strip()
        cleaned_price = raw_price.replace('\xa0', ' ')
        product_data["price"] = cleaned_price

    og_image_tag = soup.find('meta', property='og:image')
    if og_image_tag and og_image_tag.get('content'):
        product_data["image_url"] = og_image_tag['content']

    return product_data