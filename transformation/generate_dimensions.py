import pandas as pd



def orders_dimension():
    orders_dim = df[['ORDERNUMBER', 'QUANTITYORDERED', 'PRICEEACH', 'ORDERLINENUMBER', 'STATUS']]
    orders_dim = orders_dim.drop_duplicates().reset_index(drop=True)
    orders_dim.index = orders_dim.index + 1
    orders_dim['ORDERID'] = orders_dim.index
    orders_dim = orders_dim[['ORDERID','ORDERNUMBER', 'QUANTITYORDERED', 'PRICEEACH', 'ORDERLINENUMBER', 'STATUS' ]]
    orders_dim.to_csv('orders.csv', index=False)
    return orders_dim
    
def products_dimension():
    products_dim = df[['PRODUCTCODE', 'PRODUCTLINE', 'MSRP']]
    products_dim = products_dim.drop_duplicates().reset_index(drop=True)
    products_dim.index = products_dim.index + 1
    products_dim['PRODUCTID'] = products_dim.index
    products_dim = products_dim[['PRODUCTID','PRODUCTCODE', 'PRODUCTLINE', 'MSRP']]
    products_dim.to_csv('products.csv', index=False)
    return products_dim

def customers_dimension():
    customers_dim = df[['CUSTOMERID', 'CUSTOMERNAME', 'CONTACTLASTNAME','CONTACTFIRSTNAME', 'PHONE']]
    customers_dim = customers_dim.drop_duplicates().reset_index(drop=True)
    customers_dim
    customers_dim.to_csv('customers.csv', index=False)
    return customers_dim
    
def date_dimension():
    date_dim = df[['ORDERDATE', 'MONTH_ID', 'YEAR_ID']]
    date_dim = date_dim.drop_duplicates().reset_index(drop=True)
    date_dim.index = date_dim.index + 1
    date_dim['DATEID'] = date_dim.index
    date_dim = date_dim[['DATEID', 'ORDERDATE', 'MONTH_ID', 'YEAR_ID' ]]
    date_dim.to_csv('dates.csv', index=False)
    date_dim
    
def location_dimension():
    location_dim = df[['ADDRESSLINE1', 'CITY', 'STATE', 'POSTALCODE', 'COUNTRY']]
    location_dim = location_dim.drop_duplicates().reset_index(drop=True)
    location_dim.index = location_dim.index + 1
    location_dim['LOCATIONID'] = location_dim.index
    location_dim = location_dim[['LOCATIONID', 'ADDRESSLINE1', 'CITY', 'STATE', 'POSTALCODE', 'COUNTRY']]
    location_dim.to_csv('location.csv', index=False)
    return location_dim

def snowflake_location():
    #Insert LOCATIONID as a foreign key in CUSTOMERS dimension
    locations = df[['ADDRESSLINE1', 'CITY', 'STATE', 'POSTALCODE', 'COUNTRY']]
    customer_dim = pd.read_csv('customers.csv', encoding="cp1252")
    location_dim = pd.read_csv('location.csv', encoding="cp1252")
    location_id_fk = {'LOCATIONID': []}
    for index, row in locations.iterrows():
        for index2, rowl in location_dim.iterrows():
            if(rowl['CITY'] == row['CITY'] and rowl['COUNTRY'] == row['COUNTRY']):
                location_id_fk['LOCATIONID'].append(rowl['LOCATIONID'])
    location_id_col = pd.DataFrame(location_id_fk)
    customer_dim.insert(loc=5, column="LOCATIONID", value=location_id_col.astype(int))
    customer_dim.to_csv('customers.csv', encoding="cp1252")

global df
df = pd.read_csv(r".\sales_dataset_r.csv", encoding="cp1252")
df['ORDERDATE'] = pd.to_datetime(df['ORDERDATE'])
df.to_csv('sales_dataset_w.csv', index=False)
orders_dimension()
products_dimension()
customers_dimension()
date_dimension()
location_dimension()
snowflake_location()   