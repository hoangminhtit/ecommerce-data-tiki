import logging
import psycopg2 as ps
import pandas as pd

def create_tables(cur):
    scripts = """
CREATE SCHEMA IF NOT EXISTS db;

CREATE TABLE IF NOT EXISTS Customers (
  customerID integer PRIMARY KEY NOT NULL,
  lastName varchar(255),
  firstName varchar(255),
  city varchar(255),              
  phone varchar(15),                  
  gender varchar(10),                 
  address varchar(255),
  dayOfBirth timestamp,                
  country varchar(255)
);
    
CREATE TABLE IF NOT EXISTS Categories (
  categoryID varchar(50) PRIMARY KEY NOT NULL,
  categoryName varchar(255)
);
    
CREATE TABLE IF NOT EXISTS Products (
  productID integer PRIMARY KEY NOT NULL,
  productName varchar(255),
  brandName varchar(255),
  categoryID varchar(255),
  priceOrigin decimal(10,2),
  priceDiscount decimal(10,2),
  quantitySold integer,
  ratingAverage float,
  reviewCount integer,
  crawl_date timestamp,
  in_stock boolean,
  FOREIGN KEY (categoryID) REFERENCES Categories(categoryID) ON UPDATE CASCADE ON DELETE CASCADE
);
    
CREATE TABLE IF NOT EXISTS OrderDetails (
  orderID integer PRIMARY KEY NOT NULL,          
  customerID integer,
  productID integer,
  price decimal(10,2),
  quantity integer,
  orderDate timestamp,
  totalAmount decimal(10,2),
  FOREIGN KEY (customerID) REFERENCES Customers(customerID) ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY (productID) REFERENCES Products(productID) ON UPDATE CASCADE ON DELETE CASCADE
);
    """
    cur.execute(scripts)
    logging.info("Created tables")

def write_data_to_database(cur, conn):
    # ================== INSERT INTO Customers ==================
    df_customer = pd.read_csv('/var/tmp/data/dim_customers.csv')
    insert_query_cust = """
    INSERT INTO Customers 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (customerID) DO NOTHING;
    """
    for _, row in df_customer.iterrows():
        cur.execute(insert_query_cust, (
            row['customer_id'], row['customer_last_name'], row['customer_first_name'],
            row['customer_city'], row['customer_phone'], row['customer_gender'],
            row['customer_address'], row['customer_birth_day'], row['customer_country']
        ))
    logging.info("Inserted customer data")

    # ================== INSERT INTO Categories ==================
    df_category = pd.read_csv('/var/tmp/data/dim_categories.csv')
    insert_query_cat = """
    INSERT INTO Categories
    VALUES (%s, %s)
    ON CONFLICT (categoryID) DO NOTHING;
    """
    for _, row in df_category.iterrows():
        cur.execute(insert_query_cat, (
            row['category_id'], row['category_name']
        ))
    logging.info("Inserted category data")

    # ================== INSERT INTO Products ==================
    df_product = pd.read_csv('/var/tmp/data/dim_products.csv')
    insert_query_prod = """
    INSERT INTO Products
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (productID) DO UPDATE SET
        priceOrigin = EXCLUDED .priceOrigin,
        priceDiscount = EXCLUDED .priceDiscount,
        reviewCount = EXCLUDED .reviewCount,
        ratingAverage = EXCLUDED .ratingAverage;
    """
    for _, row in df_product.iterrows():
        cur.execute(insert_query_prod, (
            row['product_id'], row['product_name'], row['brand_name'], row['category_id'],
            row['price_origin'], row['price_discount'], row['quantity_sold'],
            row['rating_average'], row['review_count'],
            row['crawl_date'], row['in_stock']
        ))
    logging.info("Inserted product data")

    # ================== INSERT INTO OrderDetails ==================
    df_orders = pd.read_csv('/var/tmp/data/dim_order_details.csv')
    insert_query_order = """
    INSERT INTO OrderDetails
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (orderID) DO NOTHING;
    """
    for _, row in df_orders.iterrows():
        cur.execute(insert_query_order, (
            row['order_id'], row['customer_id'], row['product_id'],
            row['price'], row['quantity'], row['order_date'], row['total_amount']
        ))
    logging.info("Inserted order details data")

    # ================== Commit once ==================
    conn.commit()
    logging.info("All data inserted successfully.")

    
if __name__=='__main__':
    conn = ps.connect(
        host = 'host.docker.internal',
        port = 5432,
        user = 'postgres',
        password = 'postgres',
        dbname = 'postgres'
    )
    cur = conn.cursor()
    
    create_tables(cur)
    write_data_to_database(cur, conn)
    cur.close()
    conn.close()
    