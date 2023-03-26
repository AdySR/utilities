#!/usr/bin/env python
# coding: utf-8

# ## DataReplicationNB_Source_v2
# 
# 
# 

# In[1]:


import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants
from pyspark.sql.functions import col


# In[ ]:


from datetime import datetime

OMP_FLUORO_Forecasts_history_R3_list = (20221201000000, 20230101000000, 20230201000000,20230301000000,20230401000000)
DATABASE = 'ccSQLpool01'
SERVER = 'ccuse2synapse01p.sql.azuresynapse.net'
TEMP_FOLDER = 'abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/source'
QUERY = 'SELECT * FROM edw_SCM.OMP_FLUORO_Forecasts_history_R3 where timestamp between '
QUERY_LIST ={}
daydelta =1000000

length_List = len(OMP_FLUORO_Forecasts_history_R3_list)
# my_date_days = my_date + datetime.timedelta(days = 18)

for index, obj in enumerate(OMP_FLUORO_Forecasts_history_R3_list):
    if index == 0:
        QUERY_LIST[obj] =(QUERY +str(obj) + ' AND '+str(OMP_FLUORO_Forecasts_history_R3_list[index+1]))


    elif (index > 0) & (index < length_List-1):
        QUERY_LIST[obj] =(QUERY +str(obj+daydelta) + ' AND '+str(OMP_FLUORO_Forecasts_history_R3_list[index+1]))


for timestamp in QUERY_LIST:
    Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod = (spark.read.option(Constants.DATABASE,DATABASE).option(Constants.SERVER, SERVER).option(Constants.TEMP_FOLDER,TEMP_FOLDER).option(Constants.QUERY,QUERY_LIST[timestamp]).synapsesql())
    Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod.coalesce(1).write.format('parquet').mode("overwrite").save("abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/edw_SCM/"+str(timestamp)+"OMP_FLUORO_Forecasts_history_R3")


# In[ ]:


Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod.coalesce(1).write.format('parquet').mode("overwrite").save("abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/edw_SCM/OMP_FLUORO_Forecasts_history_R3")


# In[ ]:


display(Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod)


# In[ ]:


Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod_ADLS = spark.read.format("parquet").load("abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/edw_SCM/OMP_FLUORO_Forecasts_history_R3")

