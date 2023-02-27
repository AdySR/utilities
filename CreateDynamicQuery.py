Erdat_list = (20220101,20220201,20220301)
DATABASE = 'ccSQLpool01'
SERVER = 'ccuse2synapse01p.sql.azuresynapse.net'
TEMP_FOLDER = 'abfss://cdw@ccuscadlsanalytics01.dfs.core.windows.net/source'
QUERY = 'SELECT * FROM src_sap_r3.vbrp WHERE ERDAT >'

length_List = len(Erdat_list)

for index, obj in enumerate(Erdat_list):
    if index == 0:
        print(QUERY ,obj)
    
    if index < length_List-1:
        print(QUERY ,obj , ' AND Erdat <=',Erdat_list[index+1])
