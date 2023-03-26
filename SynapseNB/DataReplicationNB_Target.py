#!/usr/bin/env python
# coding: utf-8

# ## DataReplicationNB_Target
# 
# 
# 

# In[10]:


import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants
from pyspark.sql.functions import col


# In[9]:


Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod_ADLS_10.schema


# In[12]:


from datetime import datetime

DATABASE = 'ccSQLpool01'
SERVER = 'ccusCsynapse01D.sql.azuresynapse.net'
TEMP_FOLDER = 'abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/source'
QUERY = 'SELECT top 10 * FROM edw_SCM.OMP_FLUORO_Forecasts_history_R3 '
QUERY_LIST ={}
daydelta =1000000

Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod = (spark.read.option(Constants.DATABASE,DATABASE).option(Constants.SERVER, SERVER).option(Constants.TEMP_FOLDER,TEMP_FOLDER).option(Constants.QUERY,QUERY).synapsesql())
Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod.schema



# In[20]:


from datetime import datetime

readlist=(20221201000000,20230101000000, 20230201000000, 20230301000000)
DATABASE = 'ccSQLpool01'
SERVER = 'ccusCsynapse01D.sql.azuresynapse.net'
TEMP_FOLDER = 'abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/source'
QUERY_LIST ={}


for r in readlist:
    # print("abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/edw_SCM/",str(r),"OMP_FLUORO_Forecasts_history_R3")
    Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod_ADLS = spark.read.format("parquet").load("abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/edw_SCM/"+str(r)+"OMP_FLUORO_Forecasts_history_R3")

    (Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod_ADLS.write.option(Constants.SERVER, SERVER).option(Constants.TEMP_FOLDER,TEMP_FOLDER).mode("append").synapsesql("ccSQLpool01.edw_SCM.OMP_FLUORO_Forecasts_history_R3"))




# In[ ]:


display(Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod_ADLS)

