import re
import pandas as pd
import os
from datetime import datetime
import numpy as np

def tstate2Status(tstate):
    if tstate == 'good':
        return(1)
    elif tstate == 'error':
        return(0)

def sisLegacyFileProcessing(log_legacy):
    fileLegacy = open(log_legacy, 'r')
    linesLegacy = fileLegacy.readlines()

    logger,legacyStatus = [],[]
    for l in linesLegacy:
        s = l.split('#')
        if len(s) == 19:
            legacyStatus.append(s)

    legacyDf = pd.DataFrame(legacyStatus)
    legacyDf = legacyDf.drop([1,3,4,5,6,7,9,11,13,14,15,17,18], axis = 1)
    legacyDf = legacyDf.rename(columns = {0:'tname', 2:'texternal', 8:'tstatus', 10:'tstate',12:'tmetrics', 16:'tunique'})
    for c in list(legacyDf.columns):
        legacyDf[c].replace(regex = True, inplace = True, to_replace = '\\'+c, value =r'')

    legacyDf['tname'] = legacyDf.tname.apply(lambda c: datetime.strptime(c, '%H:%M:%S %m/%d/%Y'))
    legacyDf['rstatus'] = legacyDf.tstate.apply(lambda c: tstate2Status(c))
    return(legacyDf)

def sisDataFileProcessing(log_data):
    #Data file processing
    fileData = open(log_data, 'r')
    linesData = fileData.readlines()

    statusData = []
    for l in linesData:
        s = l.split('\t')
        if s[3] not in ['Log Event Checker','Monitor Load Checker','Connection Statistics Monitor','Dynamic Monitoring Statistics']:
            r = (s[3],s[1],s[4],s[0])
            statusData.append(r)

    dataSum = pd.DataFrame(statusData,columns =['texternal','tstate','tvalue','tname'])
    dataSum['tname'] = dataSum.tname.apply(lambda c: datetime.strptime(c, '%H:%M:%S %m/%d/%Y'))
    return(dataSum)

def cpuUtilDataFile(dataSum,legacyDf):
    legacyDfAttr = legacyDf[['texternal','tstatus','tunique']].drop_duplicates(subset=['texternal'])
    dataSum = dataSum.merge(legacyDfAttr, how = 'left',on = 'texternal')
    dataSum_CPU = dataSum.query('tunique == "CPU" & tstate != "disabled"' )
    dataSum_CPU = dataSum_CPU[~dataSum_CPU["tvalue"].str.contains("no data")]
    dataSum_CPU.loc[:,'tvalue'] = dataSum_CPU.tvalue.apply(lambda c:int(c.split('%')[0]))
    dataSum_CPU.loc[:,'Hour'] = dataSum_CPU['tname'].dt.hour
    dataSum_CPU = dataSum_CPU.groupby(['texternal','tstatus','tunique', 'Hour'], as_index=False).agg({"tvalue": "mean"})
    return(dataSum_CPU)

def getOnlyCpuUtil(log_legacy,log_data):
    legacyDf = sisLegacyFileProcessing(log_legacy)
    dataSum = sisDataFileProcessing(log_data)
    dataSum_CPU = cpuUtilDataFile(dataSum,legacyDf)
    return(dataSum_CPU)
