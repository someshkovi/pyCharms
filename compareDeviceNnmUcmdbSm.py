import pandas as pd
import pyodbc
import sys

funcpath = r'C:\Users\somesh\Desktop\logs\Collection\functions'
sys.path.append(funcpath)

from nnmdata import *

xmlfile = r'\\10.66.100.14\Applications\EMS\Status\info.xml'

#get xml file
data = readNnmXmlFile(xmlfile)
CA = ['NPS Annotation','Category','Sub-Category','Commisionerate','Zone','PoliceStation','Location','Location_Type']
#required node standard fields
ND = ['shortname','status','nodelaststatuschange']

xmlcolumns = ND+CA
XML = nnmXmlList2df(data,xmlcolumns)
XML = XML.rename(columns={"NPS Annotation":"assetcode"})

#fetching sm device data
print('accessing sm device data')
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=EMSSQLSTD;DATABASE=HPSM;UID=sa;PWD=EmsDB@dmIN9')
cursor = conn.cursor()
query = "select LOGICAL_NAME,ASSET_TAG,ASSIGNMENT,IP_ADDRESS,DISPLAY_NAME,NETWORK,CI_CLASSIFICATION from DEVICE2M1"
smdev = pd.read_sql(query, conn)
cursor.close()
conn.close()

#fetching ucmdb device data
print('accessing ucmdb device data')
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=EMSSQLSTD;DATABASE=UCMDB;UID=sa;PWD=EmsDB@dmIN9')
cursor = conn.cursor()
query = "select A_Asset_tag,A_DISPLAY_LABEL from NODE_1"
ucmdbdev = pd.read_sql(query, conn)
cursor.close()
conn.close()

print('fetching selective xml columns')
devices = XML[['shortname','assetcode','status','Category','Sub-Category','Location']]
print('sname in devices')
devices.loc[:,'sname'] = devices.shortname.apply(lambda c: c.lower())
print('sname in smdev')
smdev.loc[:,'sname'] = smdev.DISPLAY_NAME.apply(lambda c: c.lower())
print('sname in ucmdbdev')
ucmdbdev.loc[:,'sname'] = ucmdbdev.A_DISPLAY_LABEL.apply(lambda c: c.lower())

print('drop_duplicates')
for f in [devices,smdev,ucmdbdev]:
    f.drop_duplicates(subset ="sname", keep = False, inplace = True)
print('merge')
devices = devices.merge(ucmdbdev, how = 'left', on='sname')
devices = devices.merge(smdev, how = 'left', on='sname')

devices.to_excel(r'C:\Users\somesh\Desktop\logs\devices.xlsx')
