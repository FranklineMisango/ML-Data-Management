# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *
import json
%run /Shared/MetaDatarepliaction_Backend_Code/Post_Replication_Test

# COMMAND ----------

dbutils.widgets.text('user_input', "[{'source_type':'csv','table_name':'Orders'}]")
input=dbutils.widgets.get("user_input")
#dbutils.widgets.removeAll()

# COMMAND ----------

delta_files_list_dict={}
for i in dbutils.fs.ls('dbfs:/databricks-datasets/tpch/delta-001/'):
    if i.size ==0:
        delta_files_list_dict[i.name[:-1].capitalize()]=i.path

# COMMAND ----------

csv_tables_list_dict={}
for i in dbutils.fs.ls('dbfs:/mnt/input/Input_CSV/'):
    if i.size !=0:
        csv_tables_list_dict[i.name[:-4].capitalize()]=i.path

# COMMAND ----------

def delta_file_replication(tab_name,delta_files_list_dict):
    delta_tables_list={}
    try:
        (dbutils.fs.ls('/mnt/replication/replication_folder_delta_tables/'+tab_name))
        full_load=False
    except:
        full_load=True
    if tab_name in delta_files_list_dict and not full_load:
            deltaTable = DeltaTable.forPath(spark, '/mnt/replication/replication_folder_delta_tables/'+tab_name)
            df1=spark.read.load(delta_files_list_dict[tab_name])
            with open('/Workspace/Repos/rohith@azuredezyre.onmicrosoft.com/Metadatamanagement_Projectpro/Metadatamanagement/SourceDefinitionFiles/Delta_Lake/'+tab_name+'.json', 'r') as f:
                data = json.load(f)
            cond='target.'+data['Primary_key']+ '='+ 'updates.'+data['Primary_key']
            deltaTable.alias('target').merge(df1.alias('updates'),cond).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
           
    elif tab_name in delta_files_list_dict and  full_load:
        #print("Full load completed")
        df1=spark.read.load(delta_files_list_dict[tab_name])
        df1.write.save("/mnt/replication/replication_folder_delta_tables/"+tab_name)
        

            

    else:
        print("No table ",tab_name," found with the path mentioned, Please recheck the list mentioned")








# COMMAND ----------

def sqlserver_replication(tab_name):
    server_name = "jdbc:sqlserver://metadatamanagementreplication.database.windows.net"
    database_name = "metadatamanagementreplication"
    replication_folder=""
    url = 'jdbc:sqlserver://metadatamanagementreplication.database.windows.net:1433;database=metadatamanagementreplication;user=admin_sqlserver@metadatamanagementreplication;password=Welcome@123;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;'
    username="admin_sqlserver"
    password="Welcome@123"
    connectionProperties={"user":username,"password":password,"driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"}
    #query="SELECT table_name FROM INFORMATION_SCHEMA. TABLES WHERE table_type = 'BASE TABLE' and table_name ="+tab_name+  ";"
    
    try:
        df1=spark.read.jdbc(url=url,table=tab_name,properties=connectionProperties)
        try:
            table_path=dbutils.fs.ls("/mnt/replication/sql_server/"+tab_name)
            replication_folder=True
        except:
            replication_folder=False
    except:
        print("Table not present in DB")
    if replication_folder:
        deltaTable = DeltaTable.forPath(spark, "/mnt/replication/sql_server/"+tab_name)
        with open('/Workspace/Repos/rohith@azuredezyre.onmicrosoft.com/Metadatamanagement_Projectpro/Metadatamanagement/SourceDefinitionFiles/SQL_server/'+tab_name[4:]+'.json', 'r') as f:
                data = json.load(f)
        
        cond='target.'+data['Primary_key']+ '='+ 'updates.'+data['Primary_key']
        
        deltaTable.alias('target').merge(df1.alias('updates'),cond).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
    else:
        df1.write.save("/mnt/replication/sql_server/"+tab_name)







    
        
    

# COMMAND ----------

def  csv_replication(file_name,csv_tables_list_dict):
    try:
        (dbutils.fs.ls('/mnt/replication/replication_folder_csv_tables/'+file_name))
        full_load=False
    except:
        full_load=True
    if file_name in csv_tables_list_dict and not full_load:
            deltaTable = DeltaTable.forPath(spark, '/mnt/replication/replication_folder_csv_tables/'+file_name)
            df1=spark.read.option("header",True).option("inferschema",True).load(csv_tables_list_dict[file_name])
            with open('/Workspace/Repos/rohith@azuredezyre.onmicrosoft.com/Metadatamanagement_Projectpro/Metadatamanagement/SourceDefinitionFiles/CSV/'+file_name+'.json', 'r') as f:
                data = json.load(f)
            cond='target.'+data['Primary_key']+ '='+ 'updates.'+data['Primary_key']
            deltaTable.alias('target').merge(df1.alias('updates'),cond).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
           
    elif file_name in csv_tables_list_dict and  full_load:
        #print("Full load completed")
        df1=spark.read.option("inferschema",True).option("header",True).csv(csv_tables_list_dict[file_name])
        df1.write.save("/mnt/replication/replication_folder_csv_tables/"+file_name)
        

            

    else:
        print("No table ",file_name," found with the path mentioned, Please recheck the list mentioned")


# COMMAND ----------

for i in eval(input):
    print(i)
    if i['source_type']=="dbfs_delta_Table":
        tab_name=i["table_name"]
        delta_file_replication(tab_name,delta_files_list_dict)
    elif i['source_type']=="sql_server":
        sqlserver_replication(i["table_name"])
    elif i['source_type']=="csv":
        csv_replication(i["table_name"],csv_tables_list_dict)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/input/Input_CSV/
