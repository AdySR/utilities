#!/usr/bin/env python
# coding: utf-8

# ## basics_learning
# 
# 
# 

# In[8]:


print("hey")


# In[9]:


import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants
from pyspark.sql.functions import col


# In[10]:


# Read from existing internal table
dfToReadFromTable = (spark.read
                     .option(Constants.SERVER, "ccusCsynapse01D.sql.azuresynapse.net")
                     .option(Constants.TEMP_FOLDER, "abfss://lsa@ccuscadlsanalytics01.dfs.core.windows.net/source")
                     .synapsesql("ccSQLpool01.edw_ctrl.CDW_JOB_RUN_STATUS")
                     .select("Status", "ObjectID")
                     .limit(10))

# Show contents of the dataframe
# dfToReadFromTable.show()


# In[ ]:


dfToReadFromTable.show()


# In[1]:


# Add required imports
import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants
from pyspark.sql.functions import col


# Name of the SQL Dedicated Pool or database where to run the query
# Database can be specified as a Spark Config or as a Constant - Constants.DATABASE
spark.conf.set("spark.sqlanalyticsconnector.dw.database", "ccSQLpool01")


# In[2]:


dfToReadFromQueryAsOption = (spark.read
                     .option(Constants.DATABASE, "ccSQLpool01")
                     .option(Constants.SERVER, "ccusCsynapse01D.sql.azuresynapse.net")
                     .option(Constants.TEMP_FOLDER, "abfss://lsa@ccuscadlsanalytics01.dfs.core.windows.net/source")
                     .option(Constants.QUERY, "Select * from edw_ctrl.cdw_objects")
                     .synapsesql()
)

dfToReadFromQueryAsArgument = (spark.read.option(Constants.DATABASE, "ccSQLpool01")\
                        .option(Constants.SERVER, "ccusCsynapse01D.sql.azuresynapse.net")\
                        .option(Constants.TEMP_FOLDER, "abfss://lsa@ccuscadlsanalytics01.dfs.core.windows.net/source")\
                        .synapsesql("Select * from edw_ctrl.cdw_objects")
)


# In[13]:


# Show contents of the dataframe
dfToReadFromQueryAsOption.show()


# In[14]:


dfToReadFromQueryAsArgument.show()


# In[16]:


import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants

(dfToReadFromQueryAsArgument.write
 .option(Constants.SERVER, "ccusCsynapse01D.sql.azuresynapse.net")\
 .option(Constants.TEMP_FOLDER, "abfss://lsa@ccuscadlsanalytics01.dfs.core.windows.net/source")\
 .mode("overwrite")
 .synapsesql("ccSQLpool01.edw_ctrl.cdw_objects_ady"))

