import pyodbc
import pandas as pd
import sys

funcpath = r'C:\Users\somesh\Desktop\logs\Collection\functions'
sys.path.append(funcpath)
from nnmdata import *

nnmXmlfile =r'\\10.66.100.14\Applications\EMS\Status\info.xml'




#Get SM events table
print('fetchting sm table')
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=EMSSQLSTD;DATABASE=HPSM;UID=sa;PWD=EmsDB@dmIN9')
cursor = conn.cursor()
query = "select NUMBER,OPEN_TIME,RESOLVED_TIME,OPENED_BY,SEVERITY,PRIORITY_CODE,ASSIGNMENT,STATUS,PROBLEM_STATUS,SM_DEVICE_DISPLAY_NAME,IPADDRESS,CI_CLASSIFICATION,BRIEF_DESCRIPTION,RESOLUTION,ROOT_CAUSE_CODE,CATEGORY,SUBCATEGORY from PROBSUMMARYM1 where PROBLEM_STATUS <> 'Closed'"
smOpenIncidents = pd.read_sql(query, conn)
cursor.close()
conn.close()

#Get OMi events table
print('fetchting omi table')
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=EMSSQLSTD;DATABASE=event;UID=sa;PWD=EmsDB@dmIN9')
cursor = conn.cursor()
query = "select CONTROL_EXTERNAL_ID,APPLICATION,TIME_STATE_CHANGED,TIME_RECEIVED,NODEHINTS_HINT,STATE,TITLE from all_events where STATE <> 'CLOSED' and STATE <> 'RESOLVED' and CONTROL_EXTERNAL_ID IS NOT NULL;"
omiEvents = pd.read_sql(query, conn)
cursor.close()
conn.close()

#omi sm open events
print('consolidate omi events')
omism = omiEvents[omiEvents['CONTROL_EXTERNAL_ID'].notnull()]
omism = omism[omism['STATE'] !='RESOLVED']
omism = omism[['CONTROL_EXTERNAL_ID','APPLICATION','TIME_RECEIVED','TIME_STATE_CHANGED','STATE','TITLE']]
omism = omism.rename(columns = {'CONTROL_EXTERNAL_ID':'NUMBER'})

#sm omi open events
print('consolidate sm events')
smomi = smOpenIncidents[smOpenIncidents['OPENED_BY']=='SMOMi']
smomi = smomi[smomi['PROBLEM_STATUS']!='Resolved']
smomi = smomi[['NUMBER','OPENED_BY','OPEN_TIME','ASSIGNMENT','PROBLEM_STATUS','SM_DEVICE_DISPLAY_NAME','IPADDRESS','CI_CLASSIFICATION','BRIEF_DESCRIPTION']]

smomiMerge = smomi.merge(omism, how = 'left', on='NUMBER')
smVarData = smomiMerge[smomiMerge["APPLICATION"].isnull()]

#nnm dev stat
print('fetching nnmXml data')
data = readNnmXmlFile(nnmXmlfile)
xmlcolumns = ['shortname','status','nodelaststatuschange','NPS Annotation']
XML = nnmXmlList2df(data,xmlcolumns)
XML['SM_DEVICE_DISPLAY_NAME'] = XML.shortname.apply(lambda c: c.lower())

smomiNNMMerge = smVarData.merge(XML, how = 'left', on='SM_DEVICE_DISPLAY_NAME')

print('writing to excel')
smomiNNMMerge.to_excel(r'C:\Users\somesh\Desktop\logs\smomiNNMMerge.xlsx')
