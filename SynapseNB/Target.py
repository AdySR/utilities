import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants
from pyspark.sql.functions import col


# In[ ]:

from datetime import datetime

DATABASE = 'ccSQLpool01'
SERVER = 'ccuscsynapse01t.sql.azuresynapse.net'
TEMP_FOLDER = 'abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/source'

df = spark.read.format("parquet").load("abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/src_sap_r3/bseg")



(df.write.option(Constants.SERVER, SERVER).option(Constants.TEMP_FOLDER,TEMP_FOLDER).mode("overwrite").synapsesql("ccSQLpool01.src_sap_r3.Bseg"))