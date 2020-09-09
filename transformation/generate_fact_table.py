import pandas as pd


print("Generating sales fact table.....")
df = pd.read_csv(r".\sales_dataset_w.csv", encoding="cp1252")

#Retain CustomerID
fact_table = df[['CUSTOMERID', 'SALES', 'DEALSIZE']]

#Insert ORDERID as the foreign key
orders_dim = pd.read_csv(r".\orders.csv", encoding="cp1252")
order_id_col = orders_dim['ORDERID']
order_id_col
fact_table.insert(loc=0, column='ORDERID', value=order_id_col)

#Insert PRODUCTID as a foreign key
products = df[['PRODUCTCODE', 'PRODUCTLINE', 'MSRP']] 
products_dim = pd.read_csv(r".\products.csv", encoding="cp1252") 
product_id_fk = {'PRODUCTID': []}
for index, row in products.iterrows():
    for index2, rowp in products_dim.iterrows():
        if(rowp['PRODUCTCODE'] == row['PRODUCTCODE'] and rowp['PRODUCTLINE'] == row['PRODUCTLINE']):
            product_id_fk['PRODUCTID'].append(rowp['PRODUCTID'])
product_id_col = pd.DataFrame(product_id_fk)
product_id_col
fact_table.insert(loc=1, column='PRODUCTID', value=product_id_col)
fact_table.to_csv("fact_table.csv", index=False)

#Insert DATEID as a foreign key
dates = df[['ORDERDATE', 'MONTH_ID', 'YEAR_ID']] 
dates_dim = pd.read_csv(r".\dates.csv", encoding="cp1252") 
dates_dim
date_id_fk = {'DATEID': []}
for index, row in dates.iterrows():
      for index2, rowd in dates_dim.iterrows():
          if(rowd['ORDERDATE'] == row['ORDERDATE'] and rowd['MONTH_ID'] == row['MONTH_ID'] and rowd['YEAR_ID'] == row['YEAR_ID']):
            date_id_fk['DATEID'].append(rowd['DATEID'])               
date_id_col = pd.DataFrame(date_id_fk)
date_id_col
fact_table.insert(loc=2, column='DATEID', value=date_id_col.astype(int))
fact_table.to_csv('fact_table_w.csv', index=False)
if(not fact_table.empty):
    print("--Fact table successfully created--")
fact_table