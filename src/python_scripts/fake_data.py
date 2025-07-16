import random
import logging
from datetime import timedelta, datetime
from faker import Faker
import pandas as pd
import numpy as np

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

faker_data = Faker()

def generate_phone_number():
    return f"{faker_data.random_int(100, 999)}-{faker_data.random_int(100, 999)}-{faker_data.random_int(100, 999)}"
    
def generate_customer_data():
    customers = []
    for _ in range(50):
        customer = {
            'customer_id': faker_data.uuid4(),
            'customer_last_name': faker_data.last_name(),
            'customer_first_name': faker_data.first_name(),
            'customer_city': faker_data.city(),
            'customer_phone': generate_phone_number(),
            'customer_gender': faker_data.random_element(elements=['Male', 'Female']),
            'customer_address': faker_data.address().replace('\n', ','),
            'customer_birth_day': faker_data.date_of_birth(minimum_age=12, maximum_age=65),
            'customer_country': faker_data.country()
        }
        customers.append(customer)
    return customers

def random_past_date(days_back=30):
    """Trả về một datetime ngẫu nhiên trong `days_back` ngày gần đây."""
    past_days = random.randint(0, days_back)
    random_seconds = random.randint(0, 86400)  # số giây trong 1 ngày
    return datetime.now() - timedelta(days=past_days, seconds=random_seconds)

def generate_order_data(df_product, df_customer):
    orders = []
    for _ in range(100):  # 100 đơn hàng
        product = df_product.sample(1).iloc[0]
        customer = df_customer.sample(1).iloc[0]
        quantity = random.randint(1, 3)
        order_date = random_past_date(30)
        
        orders.append({
            "order_id": faker_data.unique.random_int(min=100, max=999),
            "customer_id": customer['customer_id'],
            "product_id": product['product_id'],
            "product_name": product['product_name'],
            "price": product['price_discount'],
            "quantity": quantity,
            "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "total_amount": quantity * product['price_discount'],
        })
    return orders

if __name__=='__main__':
    # Get DataFrame customer
    customers = generate_customer_data()
    df_customer = pd.DataFrame(customers)
    df_customer.to_csv('/var/tmp/data/dim_customers.csv', index=False)
    logger.info('Create dim_customers')
    
    #Get DataFrame product
    df_product = pd.read_csv('/var/tmp/data/transform_product_data.csv')
    df_product['customer_id'] = np.random.choice(df_customer['customer_id'], size=len(df_product), replace=True)
    df_product.to_csv('/var/tmp/data/dim_products.csv', index=False)
    logger.info('Create dim_products')

    #Category
    data_category = df_product[['category_id', 'category_name']]
    data_category.drop_duplicates(inplace=True)
    data_category.to_csv('/var/tmp/data/dim_categories.csv', index=False)
    df_product = df_product.drop(columns=['category_name'], axis=1)
    logger.info('Create dim_categories')

    #Get DataFrame orders
    orders = generate_order_data(df_product, df_customer)
    df_orders = pd.DataFrame(orders)
    df_orders.to_csv('/var/tmp/data/dim_orders.csv', index=False)
    logger.info('Create dim_orders')

        

            
        