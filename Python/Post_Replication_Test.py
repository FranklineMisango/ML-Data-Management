# Databricks notebook source
import json
import requests

# COMMAND ----------

class deltatable_test:
    def __init__(self,delta_table_name):
        self.delta_table_name=delta_table_name
        delta_files_list_dict={}
        for i in dbutils.fs.ls('dbfs:/databricks-datasets/tpch/delta-001/'):
            if i.size ==0:
                delta_files_list_dict[i.name[:-1].capitalize()]=i.path
        self.delta_files_list_dict=delta_files_list_dict
    


    def count_test(self):
        df_source=spark.read.load( self.delta_files_list_dict[self.delta_table_name])
        df_target=spark.read.load("/mnt/replication/replication_folder_delta_tables/"+self.delta_table_name)
        if df_source.count()==df_target.count():
            return True
        else:
            print("There is difference in counts i.e Source Count =" ,df_source.count(),"Target Count is ",df_target.count())
            return False
    def pk_join(self):

        with open('/Workspace/Repos/rohith@azuredezyre.onmicrosoft.com/Metadatamanagement_Projectpro/Metadatamanagement/SourceDefinitionFiles/Delta_Lake/'+self.delta_table_name+'.json', 'r') as f:
                data = json.load(f)
       
        df_source=spark.read.load( self.delta_files_list_dict[self.delta_table_name])
        df_target=spark.read.load("/mnt/replication/replication_folder_delta_tables/"+self.delta_table_name)
        if df_source.join(df_target,data['Primary_key']).count()==df_target.count():
            return True
        else:
            print("There is difference in counts i.e Source Count =" ,df_source.count(),"Target Count is ",df_target.count())
            return False
    


    

        
    

# COMMAND ----------

class sql_server:
    def __init__(self,table_name):
        self.table_name=table_name
        server_name = "jdbc:sqlserver://metadatamanagementreplication.database.windows.net"
        database_name = "metadatamanagementreplication"
        username="admin_sqlserver"
        password="Welcome@123"
        url = 'jdbc:sqlserver://metadatamanagementreplication.database.windows.net:1433;database=metadatamanagementreplication;user=admin_sqlserver@metadatamanagementreplication;password=Welcome@123;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;'
        connectionProperties={"user":username,"password":password,"driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"}
        
    #query="SELECT table_name FROM INFORMATION_SCHEMA. TABLES WHERE table_type = 'BASE TABLE' and table_name ="+tab_name+  ";"
        self.url=url
        self.database_name=database_name
        self.server_name=server_name
        self.connectionProperties=connectionProperties
        self.username=username
        self.password=password
        
    


    def count_test(self):
        df_source=spark.read.jdbc(url=self.url,table=self.table_name,properties=self.connectionProperties)
        df_target=spark.read.load("/mnt/replication/sql_server/"+self.table_name)
        if df_source.count()==df_target.count():
            return True
        else:
            print("There is difference in counts i.e Source Count =" ,df_source.count(),"Target Count is ",df_target.count())
            return False
    def pk_join(self):

        with open('/Workspace/Repos/rohith@azuredezyre.onmicrosoft.com/Metadatamanagement_Projectpro/Metadatamanagement/SourceDefinitionFiles/SQL_server/'+self.table_name[4:].capitalize()+'.json', 'r') as f:
                data = json.load(f)
        
        df_source=spark.read.jdbc(url=self.url,table=self.table_name,properties=self.connectionProperties)
        df_target=spark.read.load("/mnt/replication/sql_server/"+self.table_name)
        if df_target.count()==df_source.join(df_target,data['Primary_key']).count():
            return True
        else:
            print("There is difference in counts i.e Source Count =" ,df_source.count(),"Target Count is ",df_target.count())
            return False
    


    

        
    

# COMMAND ----------

# url = 'https://metadatamanagementemail.azurewebsites.net:443/api/sample_email/triggers/When_a_HTTP_request_is_received/invoke?api-version=2022-05-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=Lz1K7ZusrhyeY1lxp9zV12sPUJUEHxlHVH1HTwL0MMA'
# myobj = {
#     "emailaddress":"<>",
#     "body":"triggered from python Databricks",
#     "subject":"sample"
# }

# x = requests.post(url, json = myobj)

# print(x.text)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/input
