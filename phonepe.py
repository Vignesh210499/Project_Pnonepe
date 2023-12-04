import os
import json
import pandas as pd
import psycopg2

# aggregation transaction

path1 = "C:/Users/vigne/New folder/PHONE PAY/pulse/data/aggregated/transaction/country/india/state/"
agg_tran_list = os.listdir(path1)

columns1 = {"States":[],"Years":[],"Quarter":[],"Transaction_type":[],"Transaction_count":[],"Transaction_amount":[]}

for state in agg_tran_list:
    cur_states = path1+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)
        
        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")

            A = json.load(data)
            
            for i in A["data"]["transactionData"]:
                name = i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                columns1["Transaction_type"].append(name)
                columns1["Transaction_count"].append(count)
                columns1["Transaction_amount"].append(amount)
                columns1["States"].append(state)
                columns1["Years"].append(year)
                columns1["Quarter"].append(int(file.strip(".json")))

aggre_transaction = pd.DataFrame(columns1)

aggre_transaction["States"] = aggre_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggre_transaction["States"] = aggre_transaction["States"].str.replace("-"," ")
aggre_transaction["States"] = aggre_transaction["States"].str.title()
aggre_transaction["States"] = aggre_transaction["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar and daman and diu")

#aggregated user

path2 = "C:/Users/vigne/New folder/PHONE PAY/pulse/data/aggregated/user/country/india/state/" 
agg_user_list = os.listdir(path2)

columns2 = {"States":[],"Years":[],"Quarter":[],"Brand":[],"Transaction_count":[],"Percentage":[]}

for state in agg_user_list:
    cur_states = path2+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)
        
        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            B = json.load(data)
            
            try:

                for i in B["data"]["usersByDevice"]:
                    brand = i["brand"]
                    count = i["count"]
                    percentage = i["percentage"]
                    columns2["Brand"].append(brand)
                    columns2["Transaction_count"].append(count)
                    columns2["Percentage"].append(percentage)
                    columns2["States"].append(state)
                    columns2["Years"].append(year)
                    columns2["Quarter"].append(int(file.strip(".json")))
            
            except:
                pass

aggre_user = pd.DataFrame(columns2)

aggre_user["States"] = aggre_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggre_user["States"] = aggre_user["States"].str.replace("-"," ")
aggre_user["States"] = aggre_user["States"].str.title()
aggre_user["States"] = aggre_user["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar and daman and diu")


# map transaction

path3 = "C:/Users/vigne/New folder/PHONE PAY/pulse/data/map/transaction/hover/country/india/state/"
map_tran_list = os.listdir(path3)

columns3 = {"States":[],"Years":[],"Quarter":[],"Transaction_name":[],"Transaction_count":[],"Transaction_amount":[]}

for state in map_tran_list:
    cur_states = path3+state+"/"
    map_year_list = os.listdir(cur_states)
    
    for year in map_year_list:
        cur_years = cur_states+year+"/"
        map_file_list = os.listdir(cur_years)
        
        for file in map_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            C = json.load(data)
            
            for i in C["data"]["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns3["Transaction_name"].append(name)
                columns3["Transaction_count"].append(count)
                columns3["Transaction_amount"].append(amount)
                columns3["States"].append(state)
                columns3["Years"].append(year)
                columns3["Quarter"].append(int(file.strip(".json")))
                
map_transaction = pd.DataFrame(columns3)

map_transaction["States"] = map_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_transaction["States"] = map_transaction["States"].str.replace("-"," ")
map_transaction["States"] = map_transaction["States"].str.title()
map_transaction["States"] = map_transaction["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar and daman and diu")

           
# map_user

path4 = "C:/Users/vigne/New folder/PHONE PAY/pulse/data/map/user/hover/country/india/state/"
map_user_list = os.listdir(path4)

columns4 = {"States":[],"Years":[],"Quarter":[],"Districts":[],"Registeredusers":[],"Appopens":[]}

for state in map_user_list:
    cur_states = path4+state+"/"
    map_year_list = os.listdir(cur_states)
    
    for year in map_year_list:
        cur_years = cur_states+year+"/"
        map_file_list = os.listdir(cur_years)
        
        for file in map_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            D = json.load(data)

            for i in D["data"]["hoverData"].items():
                district = i[0]
                registeredusers = i[1]["registeredUsers"]
                appopens = i[1]["appOpens"]
                columns4["Districts"].append(district)
                columns4["Registeredusers"].append(registeredusers)
                columns4["Appopens"].append(appopens)
                columns4["States"].append(state)
                columns4["Years"].append(year)
                columns4["Quarter"].append(int(file.strip(".json")))
              
map_user = pd.DataFrame(columns4)

map_user["States"] = map_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_user["States"] = map_user["States"].str.replace("-"," ")
map_user["States"] = map_user["States"].str.title()
map_user["States"] = map_user["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar and daman and diu")


# top_transaction

path5 = "C:/Users/vigne/New folder/PHONE PAY/pulse/data/top/transaction/country/india/state/"
top_tran_list = os.listdir(path5)

columns5 = {"States":[],"Years":[],"Quarter":[],"Pincodes":[],"Transaction_count":[],"Transaction_amount":[]}

for state in top_tran_list:
    cur_states = path5+state+"/"
    top_year_list = os.listdir(cur_states)
    
    for year in top_year_list:
        cur_years = cur_states+year+"/"
        top_file_list = os.listdir(cur_years)
        
        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            E = json.load(data)

            for i in E["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns5["Pincodes"].append(entityName)
                columns5["Transaction_count"].append(count)
                columns5["Transaction_amount"].append(amount)
                columns5["States"].append(state)
                columns5["Years"].append(year)
                columns5["Quarter"].append(int(file.strip(".json")))

                

top_transaction = pd.DataFrame(columns5)

top_transaction["States"] = top_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_transaction["States"] = top_transaction["States"].str.replace("-"," ")
top_transaction["States"] = top_transaction["States"].str.title()
top_transaction["States"] = top_transaction["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar and daman and diu")


# top_user

path6 ="C:/Users/vigne/New folder/PHONE PAY/pulse/data/top/user/country/india/state/"
top_user_list = os.listdir(path6)

columns6 = {"States":[],"Years":[],"Quarter":[],"Pincodes":[],"Registeredusers":[]}

for state in top_user_list:
    cur_states = path6+state+"/"
    top_year_list = os.listdir(cur_states)
    
    for year in top_year_list:
        cur_years = cur_states+year+"/"
        top_file_list = os.listdir(cur_years)
        
        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            F = json.load(data)

            for i in F["data"]["pincodes"]:
                name = i["name"]
                registeredusers = i["registeredUsers"]
                columns6["Pincodes"].append(name)
                columns6["Registeredusers"].append(registeredusers)
                columns6["States"].append(state)
                columns6["Years"].append(year)
                columns6["Quarter"].append(int(file.strip(".json")))

top_user = pd.DataFrame(columns6)

top_user["States"] = top_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_user["States"] = top_user["States"].str.replace("-"," ")
top_user["States"] = top_user["States"].str.title()
top_user["States"] = top_user["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar and daman and diu")

# Table creation

mydb = psycopg2.connect(host = "localhost",
                        user = "postgres",
                        password = "Vignesh213@",
                        database = "phonepay_data",
                        port = "5432")
cursor = mydb.cursor()

#create query aggre_transaction

create_query1 = '''CREATE TABLE if not exists aggregated_transaction (States varchar(50),
                                                                        Years int,
                                                                        Quarter int,
                                                                        Transaction_type varchar(50),
                                                                        Transaction_count bigint,
                                                                        Transaction_amount bigint)'''

cursor.execute(create_query1)
mydb.commit()

for index,row in aggre_transaction.iterrows():
    insert_query1 = '''INSERT INTO aggregated_transaction(States,
                                                            Years,
                                                            Quarter,
                                                            Transaction_type,
                                                            TRansaction_count,
                                                            Transaction_amount)
                                                            
                                                            values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Transaction_type"],
              row["Transaction_count"],
              row["Transaction_amount"])
    
    cursor.execute(insert_query1,values)
    mydb.commit()

# create table for aggregate user

create_query2 = '''CREATE TABLE if not exists aggregated_user(States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                Brand varchar(50),
                                                                Transaction_count bigint,
                                                                Percentage float)'''

cursor.execute(create_query2)
mydb.commit()

for index,row in aggre_user.iterrows():
    insert_query2 = '''INSERT INTO aggregated_user(States,
                                                    Years,
                                                    Quarter,
                                                    Brand,
                                                    Transaction_count,
                                                    Percentage)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)'''
    
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Brand"],
              row["Transaction_count"],
              row["Percentage"])
    
    cursor.execute(insert_query2,values)
    mydb.commit()

# create table for map transaction

create_query3 = '''CREATE TABLE if not exists map_transaction(States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                Transaction_name varchar(50),
                                                                Transaction_count bigint,
                                                                Transaction_amount bigint)'''
cursor.execute(create_query3)
mydb.commit()

for index,row in map_transaction.iterrows():
    insert_query3 = '''INSERT INTO map_transaction(States,
                                                    Years,
                                                    Quarter,
                                                    Transaction_name,
                                                    Transaction_count,
                                                    Transaction_amount)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)'''
    
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Transaction_name"],
              row["Transaction_count"],
              row["Transaction_amount"])
    cursor.execute(insert_query3,values)
    mydb.commit

#create table for map user

create_query4 = '''CREATE TABLE if not exists map_user(States varchar(50),
                                                        Years int,
                                                        Quarter int,
                                                        Districts varchar(50),
                                                        Registeredusers bigint,
                                                        Appopens bigint)'''

cursor.execute(create_query4)
mydb.commit()

for index,row  in map_user.iterrows():
    insert_query4 = '''INSERT INTO map_user(States,
                                            Years,
                                            Quarter,
                                            Districts,
                                            Registeredusers,
                                            Appopens)
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
    
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Districts"],
              row["Registeredusers"],
              row["Appopens"])
    
    cursor.execute(insert_query4,values)
    mydb.commit()

# Create table for top transaction

create_query5 = '''CREATE TABLE if not exists top_transaction(States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                Pincodes int,
                                                                Transaction_count bigint,
                                                                Transaction_amount bigint)'''

cursor.execute(create_query5)
mydb.commit()

for index,row in top_transaction.iterrows():
    insert_query5 = '''INSERT INTO top_transaction(States,
                                                    Years,
                                                    Quarter,
                                                    Pincodes,
                                                    Transaction_count,
                                                    Transaction_amount)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)'''
    
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Pincodes"],
              row["Transaction_count"],
              row["Transaction_amount"])

    cursor.execute(insert_query5,values)
    mydb.commit()

# Create table for top user

create_query6 = '''CREATE TABLE if not exists top_user(States varchar(50),
                                                        Years int,
                                                        Quarter int,
                                                        Pincodes int,
                                                        Registeredusers bigint)'''

cursor.execute(create_query6)
mydb.commit()

for index,row in top_user.iterrows():
    insert_query6 = '''INSERT INTO top_user(States,
                                            Years,
                                            Quarter,
                                            Pincodes,
                                            Registeredusers)
                                            
                                            values(%s,%s,%s,%s,%s)'''
    
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Pincodes"],
              row["Registeredusers"])
    
    cursor.execute(insert_query6,values)
    mydb.commit()
