import logging
import psycopg2 as ps
import pandas as pd

def create_tables(cur):
    scripts = """
CREATE SCHEMA IF NOT EXISTS db;

CREATE TABLE IF NOT EXISTS Customers (
  customerID integer PRIMARY KEY NOT NULL,
  lastName nvarchar(255),
  firstName nvarchar(255),
  city nvarchar(255),              
  phone varchar(15),                  
  gender varchar(10),                 
  address nvarchar(255),
  dayOfBirth timestamp,                
  country nvarchar(50)
);
    
CREATE TABLE IF NOT EXISTS Categories (
  categoryID varchar(50) PRIMARY KEY NOT NULL,
  categoryName nvarchar(255)
);
    
CREATE TABLE IF NOT EXISTS Products (
  productID integer PRIMARY KEY NOT NULL,
  productName nvarchar(255),
  brandName nvarchar(50),
  categoryID varchar(50),
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
            row['customerID'], row['lastName'], row['firstName'],
            row['city'], row['phone'], row['gender'],
            row['address'], row['dayOfBirth'], row['country']
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
            row['categoryID'], row['categoryName']
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
            row['productID'], row['productName'], row['brandName'], row['categoryID'],
            row['priceOrigin'], row['priceDiscount'], row['quantitySold'],
            row['ratingAverage'], row['reviewCount'],
            row['crawl_date'], row['in_stock']
        ))
    logging.info("Inserted product data")

    # ================== INSERT INTO OrderDetails ==================
    df_orders = pd.read_csv('/var/tmp/data/dim_orders.csv')
    insert_query_order = """
    INSERT INTO OrderDetails
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (orderID) DO NOTHING;
    """
    for _, row in df_orders.iterrows():
        cur.execute(insert_query_order, (
            row['orderID'], row['customerID'], row['productID'],
            row['price'], row['quantity'], row['orderDate'], row['totalAmount']
        ))
    logging.info("Inserted order details data")

    # ================== Commit once ==================
    conn.commit()
    logging.info("All data inserted successfully.")

    
if __name__=='__main__':
    conn = ps.connect(
        host = 'docker.host.interval',
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
    