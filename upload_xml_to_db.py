import xml.etree.ElementTree as ET
import pandas as pd
import os
import numpy as np
import xlsxwriter
import pyodbc
import pandas as pd
from datetime import datetime, date
import time
import logging

def lastchangesplit(l_ch):
    dch = l_ch.split('T')[0]
    tch = l_ch.split('T')[1].split('.')[0]
    dchs = dch.split('-')
    tchs = tch.split(':')
    laser = datetime(int(dchs[0]),int(dchs[1]),int(dchs[2]),int(tchs[0]),0,0)
    dt = datetime.now()
    dt = dt.replace(second=0, microsecond=0)
    evntage = dt -laser
    return(evntage)

def currenttime():
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    return(current_time)

#get xml file
try:
    f = r'\\10.66.43.11\nnm\info.xml'
    tree = ET.parse(f)
    root = tree.getroot()
    print(root.tag)
except:
    print('xml read failed')

#required fields
devName = []
devStatus = []
devNotes = []
deventage = []
dcurdate = []
dcurtime = []

#fetching details

for device in root.findall('node'):
    shortname = device.find('shortname').text
    status = device.find('status').text
    notes = device.find('notes').text
    lastchange = device.find('nodelaststatuschange').text
    if status =='CRITICAL' or status =='UNKNOWN':
        eventage = str(lastchangesplit(lastchange))
    else:
        eventage = '0'
    curdate = str(date.today())
    curtime = str(currenttime())
    devName.append(shortname)
    devStatus.append(status)
    devNotes.append(notes)
    deventage.append(eventage)
    dcurdate.append(curdate)
    dcurtime.append(curtime)

#convert data to dataframe
data = {'shortname':devName, 'status':devStatus, 'notes':devNotes, 'event_age':deventage, 'date':dcurdate,'time':dcurtime}
df = pd.DataFrame(data)

#connect to database
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=EMSENTSQL;DATABASE=STESTDB;UID=sa;PWD=EmsDB@dmIN8')

cursor = conn.cursor()

for index, row in df.iterrows():
    cursor.execute("Insert into s_test([shortname],[status],[notes],[event_age],[date],[time]) values(?,?,?,?,?,?)",
row['shortname'],row['status'],row['notes'],row['event_age'],row['date'],row['time'])
conn.commit()
cursor.close()
conn.close()
