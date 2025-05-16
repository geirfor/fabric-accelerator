# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "75b4a237-662c-4e25-9346-cbb8fb56876d",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "17891602-5bda-4086-af4a-ca26bd8b62c7",
# META       "known_lakehouses": [
# META         {
# META           "id": "75b4a237-662c-4e25-9346-cbb8fb56876d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run /DeltaLakeFunctions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Iterate through all tables in lakehouse and run OPTIMIZE and VACCUM commands

# CELL ********************

df = spark.sql("show tables")
tableList = df.select("tableName").rdd.flatMap(lambda x:x).collect()
# print (tables)
for table in tableList:
    print ("optimizing",table)
    optimizeDelta(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
