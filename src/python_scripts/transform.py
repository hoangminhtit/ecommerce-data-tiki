import re
import pandas as pd

def clean_brand_name(brand_name: str) -> str:
    sub_str = r'(thương hiệu:)|(tác giả:)'
    brand_name = brand_name.lower()
    brand_name = re.sub(sub_str, '', brand_name)
    brand_name = brand_name.strip() 
    return brand_name.title() 

def clean_price_review(price):
    res = re.sub(r'[^\d]', '', str(price))
    return int(res) if res else 0

def clean_rating_average(num):
    num = str(num).strip()
    match = re.search(r'^\d+(\.\d+)?', num)
    return float(match.group()) if match else 0

def clean_quantity_sold(number):
    # number = str(n)
    number = re.sub('k', '000', str(number))
    cleaned_number = re.sub(r'[^\d]', '', number)
    return cleaned_number

if __name__=='__main__':
    df = pd.read_csv('/var/tmp/data/raw_data_tiki.csv')
    
    df['in_stock'] = df['in_stock'].astype('bool')
    df['brand_name'] = df['brand_name'].fillna('Undefined').apply(clean_brand_name)
    df['price_origin'] = df['price_origin'].fillna(0).apply(clean_price_review).astype('int')
    df['price_discount'] = df['price_discount'].fillna(0).apply(clean_price_review).astype('int')
    df['rating_average'] = df['rating_average'].fillna(0).apply(clean_rating_average).astype('float')
    df['review_count'] = df['review_count'].fillna(0).apply(clean_price_review).astype('int')
    df['quantity_sold'] = df['quantity_sold'].fillna(0).apply(clean_quantity_sold).astype('int')
    
    df.to_csv('/var/tmp/data/transform_product_data.csv', index=False)
    

