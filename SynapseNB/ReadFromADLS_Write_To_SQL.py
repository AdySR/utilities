#!/usr/bin/env python
# coding: utf-8

# ## ReadFromADLS_Write_To_SQL
# 
# 
# 

# In[6]:


import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants
from pyspark.sql.functions import col


# In[7]:


Df_F_COPA_ACTUALS = spark.read.format("parquet").load("abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/edw_FIN/Df_F_COPA_ACTUALS")


# In[8]:


p_DATABASE = 'ccSQLpool01'
p_SERVER = 'ccusCsynapse01D.sql.azuresynapse.net'
p_TEMP_FOLDER = 'abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/source'


# In[9]:


(Df_F_COPA_ACTUALS.write.option(Constants.SERVER, p_SERVER).option(Constants.TEMP_FOLDER,p_TEMP_FOLDER).mode("overwrite").synapsesql("ccSQLpool01.edw_FIN.F_COPA_ACTUALS"))


# In[ ]:


edw_FIN.F_COPA_ACTUALS

