import xml.etree.ElementTree as ET
import pandas as pd
import os
import numpy as np
import xlsxwriter
import re
from datetime import datetime, date

def lastchange(l):
    d = re.split(r'[-T:.Z]', l)
    s = [int(i) for i in d[:4]]
    dt = datetime(year=s[0], month=s[1], day=s[2], hour = s[3])
    return(dt)

def importJunctionDataFromMaster(ms):
    MS = pd.read_excel(ms, 'LnT phase-1,2,3',index_col=None, header = 4)
    devdata = MS[['New Unique codes','Commisionerate','Zone','Police Station',
                 'Location','RJIO/ACT \nConnectivity','Reachable \nto DC','Camera Names','IP address']]
    devdata = devdata[pd.notnull(devdata['IP address'])]
    devdata = devdata[pd.notnull(devdata['RJIO/ACT \nConnectivity'])]
    devdata = devdata.rename(columns={"New Unique codes":"assetcode"})
    devdata = devdata.rename(columns={"Police Station":"PoliceStation"})
    devdata['assetcode'] = devdata.assetcode.apply(lambda c: c.replace(' ',''))
    devdata['Code'] = devdata.assetcode.apply(lambda c: c.split('-')[3])
    devdata['Location_Type'] = 'Junction'
    return(devdata)

def junctionDevClassification(file):
    codedata = pd.read_csv(file)
    return(codedata)

def readNnmXmlFile(xmlfile):
    #print(convert date to readable format)
    tree = ET.parse(xmlfile)
    root = tree.getroot()
    print(root.tag)

    #required node standard fields
    ND = ['id','shortname','status','nodelaststatuschange']

    data = []
    for device in root.findall('node'):
        dev= dict()
        #fetching node standard fields
        for i in range(len(ND)):
            dev[ND[i]] = device.find(ND[i]).text
            #fetching custom attributes
        for child in device.findall('extendedAttributes'):
            for x in range(len(child.findall('attribute'))):
                dev[child[x][0].text] = child[x][1].text
        dev['File']= xmlfile
        data.append(dev)
    return(data)

def nnmXmlList2df(data,xmlcolumns):
    XML = pd.DataFrame(data, columns= xmlcolumns)
    #print(converting date to readable format)
    XML.loc[:, 'nodelaststatuschange'] = XML.nodelaststatuschange.apply(lambda x: lastchange(x))
    return(XML)

def nnmReachability(r):
    if r in ['MINOR','MAJOR','NORMAL']:
        return(1)
    if r in ['CRITICAL','UNKNOWN']:
        return(0)
