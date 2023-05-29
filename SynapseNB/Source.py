import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants
from pyspark.sql.functions import col


# In[ ]:

df = (spark.read
                     .option(Constants.DATABASE, "ccSQLpool01")
                     .option(Constants.SERVER, "ccuse2synapse01p.sql.azuresynapse.net")
                     .option(Constants.TEMP_FOLDER, "abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/source")
                     .option(Constants.QUERY, "SELECT * FROM src_sap_r3.Bseg where GJAHR =2022 ")
                     .synapsesql()
)

df.coalesce(1).write.format('parquet').mode("overwrite").save("abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/src_sap_r3/Bseg")

