import pandas as pd
import xlsxwriter
import sys
import glob
import pyodbc
from datetime import datetime, date
import time
import logging

funcpath = r'C:\Users\somesh\Desktop\logs\Collection\functions'
sys.path.append(funcpath)

from nnmdata import *

def file2dt(f):
    d = re.split(r'[-_.]', f)
    s = [int(i) for i in d[:4]]
    dt = datetime(year=s[0], month=s[1], day=s[2], hour = s[3])
    return(dt)

path = r'E:\nnm\Status'
os.chdir(path)

list_of_files = [f for f in glob.glob("*.xml")]


xmlcolumns = ['shortname','status','nodelaststatuschange','NPS Annotation','File']


#connect to database
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=EMSENTSQL;DATABASE=STESTDB;UID=sa;PWD=EmsDB@dmIN8')
cursor = conn.cursor()

for f in list_of_files:
    devlist = readNnmXmlFile(f)
    XML = nnmXmlList2df(devlist,xmlcolumns)
    XML.loc[:,'File'] = XML.File.apply(lambda c: file2dt(c))
    asset  = XML[XML['NPS Annotation'].notnull()]
    noasset = XML[XML['NPS Annotation'].isnull()]
    try:
        for index, row in asset.iterrows():
            cursor.execute("Insert into nnmDeviceStatus([shortname],[status],[nodelaststatuschange],[NPS Annotation],[File]) values(?,?,?,?,?)",row['shortname'],row['status'],row['nodelaststatuschange'],row['NPS Annotation'],row['File'])
        conn.commit()
        for index, row in noasset.iterrows():
            cursor.execute("Insert into nnmDeviceStatus([shortname],[status],[nodelaststatuschange],[File]) values(?,?,?,?)",row['shortname'],row['status'],row['nodelaststatuschange'],row['File'])
        conn.commit()
        print(f)
    except:
        print(f,' is corrupted')
cursor.close()
conn.close()


