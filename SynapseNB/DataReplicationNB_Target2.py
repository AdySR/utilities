#!/usr/bin/env python
# coding: utf-8

# ## DataReplicationNB_Target2
# 
# 
# 

# In[1]:


import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants
from pyspark.sql.functions import col


# In[ ]:


Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod = (spark.read
                     .option(Constants.DATABASE, "ccSQLpool01")
                     .option(Constants.SERVER, "ccuse2synapse01p.sql.azuresynapse.net")
                     .option(Constants.TEMP_FOLDER, "abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/source")
                     .option(Constants.QUERY, "SELECT top 10 * FROM edw_SCM.OMP_FLUORO_Forecasts_history_R3 ")
                     .synapsesql()
)


# In[ ]:


Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod.coalesce(1).write.format('parquet').mode("overwrite").save("abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/edw_SCM/OMP_FLUORO_Forecasts_history_R3")


# In[ ]:


display(Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod)


# In[ ]:


Df_Raw_edw_SCM_OMP_FLUORO_Forecasts_history_R3_prod_ADLS = spark.read.format("parquet").load("abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/edw_SCM/OMP_FLUORO_Forecasts_history_R3")

