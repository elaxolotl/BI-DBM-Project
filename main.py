import pandas as pd
import mysql.connector
import os
import sqlite3

#extraction
data_path = "data/dataset-glovo.xlsx"
df_orders = pd.read_excel(data_path, sheet_name=0, header=1)
df_feedback = pd.read_excel(data_path, sheet_name=1, header=1)

#cleaning
df_orders.columns = [col.lower().replace(' ', '_') for col in df_orders.columns]
df_feedback.columns = [col.lower().replace(' ', '_') for col in df_feedback.columns]

def clean_dataset(df):
    df = df.drop_duplicates()
    df.fillna(0, inplace=True)
    return df

df_orders = clean_dataset(df_orders)
df_orders = df_orders.drop(columns="commission")
df_orders['city'] = df_orders['city'].astype('category')
df_orders['city'] = df_orders['city'].cat.add_categories([0])
df_orders['store_code'] = df_orders['store_code'].astype(str)
df_orders['date'] = pd.to_datetime(df_orders['date'])
df_orders['week'] = df_orders['date'].dt.isocalendar().year.astype(str) + '-' + \
                    df_orders['date'].dt.isocalendar().week.astype(str).str.zfill(2)

aggregated_orders = df_orders.groupby(['week', 'store_code']).agg(
    total_basket_size=('basket_size', 'sum'),
    avg_delivery_fee=('delivery_fee', 'mean'),
    avg_cpo=('cost_per_order_(cpo)', 'mean'),
    total_distance=('distance_in_km_(pick_up_to_delivery)', 'sum'),
    avg_waiting_time=('courier_waiting_time_(mins)', 'mean'),
).reset_index()

df_feedback = clean_dataset(df_feedback)
df_feedback['store_code'] = df_feedback['store_code'].astype(str)
df_feedback['week'] = pd.to_datetime(df_feedback['week'])
df_feedback['week'] = df_feedback['week'].dt.isocalendar().year.astype(str) + '-' + \
                      df_feedback['week'].dt.isocalendar().week.astype(str).str.zfill(2)

df_merged = pd.merge(aggregated_orders, df_feedback, on=['week', 'store_code'], how='inner')
df_merged= clean_dataset(df_merged)
print(df_merged)

#loading
conn = mysql.connector.connect(user='root', password=os.getenv('MYSQL_PASSWORD'), host='localhost', database='glovo')
cursor = conn.cursor()

#dim store
cursor.execute("""
CREATE TABLE dim_store (
    store_code VARCHAR(50) PRIMARY KEY,
    city VARCHAR(50)
);
""")
dim_store = df_orders[['store_code', 'city']].drop_duplicates()
for _, row in dim_store.iterrows():
    cursor.execute("""
        INSERT INTO dim_store (store_code, city)
        VALUES (%s, %s)
    """, (row['store_code'], row['city']))

#dim time
cursor.execute("""
CREATE TABLE dim_time (
    week VARCHAR(50) PRIMARY KEY,
    month INT,
    year INT,
    INDEX (week)
);
""")
dim_time = aggregated_orders[['week']].drop_duplicates()
dim_time['year'] = dim_time['week'].str.split('-').str[0].astype(int)
dim_time['month'] = pd.to_datetime(
    dim_time['year'].astype(str) + '-W' + 
    dim_time['week'].str.split('-').str[1] + '-1', 
    format='%Y-W%W-%w'
).dt.month
for _, row in dim_time.iterrows():
    cursor.execute("""
        INSERT INTO dim_time (week, month, year)
        VALUES (%s, %s, %s)
    """, (row['week'], row['month'], row['year']))
    
#dim date
cursor.execute("""
CREATE TABLE dim_date (
    date DATE PRIMARY KEY,
    week VARCHAR(50),
    FOREIGN KEY (week) REFERENCES dim_time(week)
    );
""")
dim_date = df_orders[['date']].drop_duplicates()
dim_date['week'] = dim_date['date'].dt.isocalendar().year.astype(str) + '-' + \
                   dim_date['date'].dt.isocalendar().week.astype(str).str.zfill(2)

for _, row in dim_date.iterrows():
    cursor.execute("""
        INSERT INTO dim_date (date, week)
        VALUES (%s, %s)
    """, (row['date'], row['week']))

#dim order
cursor.execute("""
CREATE TABLE dim_order (
    order_id INT PRIMARY KEY,
    store_code VARCHAR(50),
    FOREIGN KEY (store_code) REFERENCES dim_store(store_code)
)
""")
dim_order = df_orders[['order_id', 'store_code']]
for _, row in dim_order.iterrows():
    cursor.execute("""
                   INSERT INTO dim_order (order_id, store_code)
                   VALUES (%s, %s)
                   """, (row['order_id'], row['store_code']))

#fact orders
cursor.execute("""
CREATE TABLE fact_orders (
    order_id INT PRIMARY KEY,
    date DATE,
    basket_size FLOAT,
    delivery_fee FLOAT,
    cost_per_order FLOAT,
    distance_in_km FLOAT,
    courier_waiting_time FLOAT,
    FOREIGN KEY (order_id) REFERENCES dim_order(order_id),
    FOREIGN KEY (date) REFERENCES dim_date(date)
);
""")
fact_orders = df_orders[[
    'order_id', 'date', 'basket_size', 'delivery_fee',
    'cost_per_order_(cpo)', 'distance_in_km_(pick_up_to_delivery)', 'courier_waiting_time_(mins)'
]]
for _, row in fact_orders.iterrows():
    cursor.execute("""
        INSERT INTO fact_orders (order_id, date, basket_size, delivery_fee, 
                                 cost_per_order, distance_in_km, courier_waiting_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (row['order_id'], row['date'], row['basket_size'], 
          row['delivery_fee'], row['cost_per_order_(cpo)'], 
          row['distance_in_km_(pick_up_to_delivery)'], row['courier_waiting_time_(mins)']))

#fact cancellations
cursor.execute("""
CREATE TABLE fact_cancellations (
    store_code VARCHAR(50),
    week VARCHAR(50),
    customer_absent INT,
    partner_printer_issue INT,
    partner_product_unavailable INT,
    store_closed INT,
    store_cant_deliver INT,
    refunds FLOAT,
    FOREIGN KEY (store_code) REFERENCES dim_store(store_code),
    FOREIGN KEY (week) REFERENCES dim_time(week)
);
""")
fact_cancellations = df_merged[[
    'store_code', 'week', 
    'cancelled_orders_due_to_customer_absent', 
    'cancelled_orders_due_to_partner_printer/internet_issue', 
    'cancelled_orders_due_to_partner_products_not_available', 
    'cancelled_orders_due_to_partner_store_closed', 
    'cancelled_orders_due_to_store_cant_deliver',
    'refunds_to_customer'
]]
for _, row in fact_cancellations.iterrows():
    cursor.execute("""
        INSERT INTO fact_cancellations (store_code, week, customer_absent, 
                                        partner_printer_issue, partner_product_unavailable, 
                                        store_closed, store_cant_deliver, refunds)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (row['store_code'], row['week'], row['cancelled_orders_due_to_customer_absent'], 
          row['cancelled_orders_due_to_partner_printer/internet_issue'], 
          row['cancelled_orders_due_to_partner_products_not_available'], 
          row['cancelled_orders_due_to_partner_store_closed'], 
          row['cancelled_orders_due_to_store_cant_deliver'],
          row['refunds_to_customer']
          ))
    
#fact ratings
cursor.execute("""
CREATE TABLE fact_ratings (
    store_code VARCHAR(50),
    week VARCHAR(50),
    wrong_or_missing_products INT,
    packaging_issues INT,
    allergy_not_considered INT,
    poor_quality INT,
    FOREIGN KEY (store_code) REFERENCES dim_store(store_code),
    FOREIGN KEY (week) REFERENCES dim_time(week)
);
""")
fact_ratings = df_merged[[
    'store_code', 'week', 
    'bad_rated_orders_due_to_wrong_or_missing_products', 
    'bad_rated_orders_due_to_store_packaging_issues', 
    'bad_rated_orders_due_to_store_allergy_not_considered', 
    'bad_rated_orders_due_to_to__poor_quality'
]]
for _, row in fact_ratings.iterrows():
    cursor.execute("""
        INSERT INTO fact_ratings (store_code, week, 
                                  wrong_or_missing_products,
                                  packaging_issues,
                                  allergy_not_considered,
                                  poor_quality)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (row['store_code'], row['week'], 
          row['bad_rated_orders_due_to_wrong_or_missing_products'], 
          row['bad_rated_orders_due_to_store_packaging_issues'], 
          row['bad_rated_orders_due_to_store_allergy_not_considered'], 
          row['bad_rated_orders_due_to_to__poor_quality']))

#exporting
with pd.ExcelWriter('fact_tables.xlsx', engine='xlsxwriter') as writer:
    fact_cancellations.to_excel(writer, sheet_name='fact_cancellations', index=False)
    fact_orders.to_excel(writer, sheet_name='fact_orders', index=False)
    fact_ratings.to_excel(writer, sheet_name='fact_ratings', index=False)
with pd.ExcelWriter('dim_tables.xlsx', engine='xlsxwriter') as writer:
    dim_store.to_excel(writer, sheet_name='dim_stores', index=False)
    dim_time.to_excel(writer, sheet_name='dim_time', index=False)
    dim_date.to_excel(writer, sheet_name='dim_date', index=False)
    dim_order.to_excel(writer, sheet_name='dim_order', index=False)

conn.commit()

