#import project_art
import numpy as np
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, array_contains, sum, count, asc
import matplotlib.pyplot as plt



#print(project_art.logo1_1)
spark = SparkSession.builder.master("local[*]").appName("Capstone Project app").getOrCreate()

data_files_folder = "C:/Users/Learner_9ZH3Z128/Documents/PerScholas_Work/Perscholas_Project_Files/CapstoneProject/data_files/"
data_file_branch = "cdw_sapp_branch.json"
data_file_credit = "cdw_sapp_credit.json"
data_file_customer = "cdw_sapp_customer.json"
data_file_loan_app = "cdw_sapp_loan_application.json"

df_branch = spark.read.json(data_files_folder + data_file_branch)
# df_branch.printSchema()
# df_branch.filter(col("BRANCH_STATE") == "MI").show(truncate=False)
# print(df_branch.filter(col("BRANCH_STATE") == "MI").count())
# df_branch.filter(col("BRANCH_STATE") == "MI").filter(col(""))

df_credit = spark.read.json(data_files_folder + data_file_credit)
# df_credit.printSchema()
# df_credit.filter(col("MONTH")=="7").select(col("TRANSACTION_VALUE")).show(truncate=False)
#df_credit.select(df_credit.BRANCH_CODE, df_credit.TRANSACTION_TYPE, df_credit.TRANSACTION_VALUE).filter(col("TRANSACTION_TYPE")=="Healthcare").show(truncate=False)
print(df_credit.select(df_credit.BRANCH_CODE, df_credit.TRANSACTION_VALUE)\
    .filter(col("TRANSACTION_TYPE")=="Healthcare").count())#.show(truncate=False)
#df_credit.select(df_credit.BRANCH_CODE, df_credit.TRANSACTION_VALUE).filter(col("TRANSACTION_TYPE")=="Gas").show(truncate=False)
#df_credit.groupBy(df_credit.BRANCH_CODE).agg({"TRANSACTION_VALUE":"sum"}).sort(asc("BRANCH_CODE")).show()

#print("$$$$$$$$$$$$$$$$$$$$$$$$$")


df_credit.filter(col("TRANSACTION_TYPE")=="Healthcare")\
    .groupBy(df_credit.BRANCH_CODE)\
    .agg({"TRANSACTION_VALUE":"sum"}).sort(df_credit.BRANCH_CODE.desc()).show()

'''
df_credit.filter(col("TRANSACTION_TYPE")=="Healthcare")\
    .groupBy(df_credit.BRANCH_CODE)\
    .agg({"TRANSACTION_VALUE":"sum"}).sort(sum({"TRANSACTION_VALUE":"sum"}).desc()).show()
'''
#d1 = df_credit.groupBy(df_credit.BRANCH_CODE).agg({"TRANSACTION_VALUE":"sum"}).sort(asc("BRANCH_CODE"))

#plt.show()

df_customer = spark.read.json(data_files_folder + data_file_customer)
#df_customer.printSchema()
#df_customer.fil, ter(col("CUST_STATE")=="NJ").show(truncate=False)
#df_customer.filter(col("CUST_ZIP")=="48124").show(truncate=False)


df_loan_app = spark.read.json(data_files_folder + data_file_loan_app)
#df_loan_app.printSchema()
#
#df_loan_app.filter(col("Income")=="high").filter(col("Dependents")=="3+").show(truncate=False)
#print(df_loan_app.filter(col("Property_Area")=="Urban").count())
#df_loan_app.filter(col("Property_Area")=="Rural").show(truncate=False)
#


branch_data_schema = StructType([
    StructField("BRANCH_CODE", IntegerType(), True),
    StructField("BRANCH_NAME", StringType(), True),
    StructField("BRANCH_STREET", StringType(), True),
    StructField("BRANCH_CITY", StringType(), True),
    StructField("BRANCH_STATE", StringType(), True),
    StructField("BRANCH_ZIP", StringType(), True),
    StructField("BRANCH_PHONE", StringType(), True),
    StructField("LAST_UPDATED", StringType(), True),
])




#df_branch = spark.createDataFrame(data, schema = branch_data_schema)
#spark.sql("CREATE DATABASE capstone")
#spark.write.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
#                                    user="root",\
#                                    password="root_password",\
#                                    url="jdbc:mysql://localhost:3306/capstone",\
#

# 
#                                     dbtable=query).load()

spark.stop()