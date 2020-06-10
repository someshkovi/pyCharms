import pandas as pd
import time
import sys

funcpath = r'C:\Users\somesh\Desktop\logs\Collection\functions'
sys.path.append(funcpath)

from nnmdata import *

MSN = r'C:\Users\somesh\Desktop\logs\once\MS.xlsx'
coded = r'C:\Users\somesh\Desktop\DevP\nnm_class\asset_class.csv'
xmlfile = r'\\10.66.100.14\Applications\EMS\Status\info.xml'

start_time = time.time()

dev = importJunctionDataFromMaster(MSN)
codedata = junctionDevClassification(coded)

devdata = dev.merge(codedata, how = 'left', on='Code')

data = readNnmXmlFile(xmlfile)


CA = ['NPS Annotation','Category','Sub-Category','Commisionerate','Zone','PoliceStation','Location','Location_Type']
#required node standard fields
ND = ['shortname','status','nodelaststatuschange']

xmlcolumns = ND+CA


XML = nnmXmlList2df(data,xmlcolumns)
XML = XML.rename(columns={"NPS Annotation":"assetcode"})

devdata = devdata.merge(XML, how = 'left', on='assetcode')

devdata.to_excel(r"C:\Users\somesh\Desktop\logs\once\devdata250520-2.xlsx")
print("--- %s XML seconds ---" % (time.time() - start_time))
