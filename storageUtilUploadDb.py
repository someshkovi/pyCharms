import paramiko
import sys
from paramiko import SSHClient
import pandas as pd
import pyodbc
import json
from datetime import date
from datetime import datetime
import time



def ispriamry(hostname):
    stdin, stdout, stderr = client.exec_command(r'. /usr/adic/.profile ;snhamgr status')
    status = str(stdout.read(), 'utf-8')
    print("snhamgr status:")
    print(status)
    if status.split("\n")[1] == 'LocalStatus=primary':
        print(hostname +" is primary")
        return(1)
    else:
        retrun(0)

def main():
    hostname1 = '10.66.47.11'
    hostname2 = '10.66.47.12'
    hostname = hostname2
    port = 22
    username = 'root'
    password = r'password'

    Date = datetime.now().strftime("%d-%m-%Y")
    start_time = time.time()

    cin = r'. /usr/adic/.profile ;'
    cmd0 = 'snhamgr status'
    cmd1 = 'fsstate'
    cmd2 = 'snbackup -s'
    cmd3 = 'df -k | grep -E '+'\'stornext|shared\''
    cmd4 = 'tail -n 10 /usr/adic/TSM/logs/trace/trace_01'
    cmd5 = 'tail -n 10 /usr/adic/TSM/logs/tac/tac_00'
    cmd6 = 'tail -n 10 /var/log/messages'

    #fetching usage data from mdc
    try:
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.WarningPolicy)

        client.connect(hostname, port=port, username=username, password=password)

        stdin, stdout, stderr = client.exec_command(cin+cmd0)
        status = str(stdout.read(), 'utf-8')
        #print("snhamgr status:")
        #print(status)

        if status.split("\n")[1] == 'LocalStatus=primary':
            print(hostname +" is primary")
        else:
            hostname = hostname1

        client.connect(hostname, port=port, username=username, password=password)

        stdin, stdout, stderr = client.exec_command(cmd3)
        disk = str(stdout.read(), 'utf-8')
        print("Disk Usage:")
        print(disk)

        print("retriving media usage")
        fill = []
        stdin, stdout, sterr = client.exec_command(r'. /usr/adic/.profile ;'+"vsmedqry -a |grep -i 'media id' |awk '{print $3}'")
        fsmedlist = str(stdout.read())
        fsmedlist = fsmedlist[2:].split('\\n')
        fsmedlist.pop()
        for f in fsmedlist:
            stdin, stdout, stderr = client.exec_command(cin+'fsmedinfo '+f+' -F JSON')
            data = json.loads(stdout.read())
            Media = pd.DataFrame(data["medias"])
            fill = fill + data["medias"]
        mdusage = pd.DataFrame(fill)

    finally:
        client.close()

    print('summaziing tape pollicy usage')
    mdusage['percentUsed'] = mdusage.percentUsed.apply(lambda c: float(c))
    dfpp = mdusage.groupby('classId', as_index=False).agg({"percentUsed": "mean"})
    dfpc = mdusage.groupby('classId', as_index=False).agg({"mediaId": "count"})
    dfArc = mdusage[mdusage['currentArchive'] == 'Archive'].groupby('classId', as_index=False).agg({"mediaId": "count"})
    dfArc = dfArc.rename(columns={"mediaId":"Archive"})
    dfAva = mdusage[mdusage['currentArchive'] == 'Archive']
    dfAva = dfAva[dfAva['mediaStatus'] == 'UNAVAIL'].groupby('classId', as_index=False).agg({"mediaId": "count"})
    dfAva = dfAva.rename(columns={"mediaId":"UNAVAIL"})
    result = dfpp.merge(dfpc, how = 'left', on='classId')
    result = result.merge(dfArc, how = 'left', on='classId')
    result = result.merge(dfAva, how = 'left', on='classId')
    result['percentUsed'] = result.apply(lambda percentUsed : round((result['percentUsed']*result['mediaId'])/(mdusage['mediaId'].count()),2))
    result['Date'] = result.apply(lambda row: Date, axis =1)
    result = result.fillna(0)


    #Convertiting disk data to df
    k = disk.split('\n')
    try:
        k.remove('')
    except ValueError as error:
        pass
    for i in range(0,len(k)):
        k[i] = k[i].split(' ')
        R = len(k[i])
        for r in range(0,R):
            try:
                k[i].remove('')
            except ValueError as error:
                pass

    print('summarizing disk usage')
    diskusage = pd.DataFrame(k, columns = ['Filesystem', 'Size', 'Used', 'Avail', 'UseP', 'Mount'])
    diskusage['Size'] = diskusage['Size'].astype('float')
    diskusage['Used'] = diskusage['Used'].astype('float')
    diskusage['Avail'] = diskusage['Avail'].astype('float')
    diskusage['UseP'] = diskusage['UseP'].str.rstrip('%').astype('float')
    diskusage['Size_TB'] = diskusage.Size.apply(lambda c: c/(1024*1024*1024))
    diskusage['Size_TB'] = diskusage.Size_TB.apply(lambda c: round(c))
    diskusage['Date'] = diskusage.apply(lambda row: Date, axis =1)
    diskusageS = diskusage[['Filesystem','Size_TB','UseP','Date']]
    diskusageS = diskusageS[diskusageS.Filesystem != 'Shared_HA_CX1652CAB00009']
    TMDCSpace = round(diskusage['Size'].sum()/(1024*1024*1024))
    TMDCSpaceUse = diskusage['Used'].sum()/(1024*1024*1024)
    TMDCSpaceUseP = round(TMDCSpaceUse*100/TMDCSpace,2)
    diskusageS = diskusageS.append({'Filesystem' : 'SNFStotal' , 'Size_TB' : TMDCSpace, 'UseP' : TMDCSpaceUseP, 'Date':Date} , ignore_index=True)


    print('summarizing disk and tape usage')
    avgmdusgP = round(mdusage['percentUsed'].mean(),2)
    avgmdsizeTB = round(5980033680870/(1024*1024*1024*1024),2)
    totalmdsizeTB = round(mdusage['mediaId'].count()*avgmdsizeTB,2)
    Summary = diskusageS.append({'Filesystem' : 'TapeLibrary' , 'Size_TB' : totalmdsizeTB, 'UseP' : avgmdusgP, 'Date':Date} , ignore_index=True)
    Summary = Summary.fillna(0)

    print('uploading tape policy summary to db')
    try:
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER=EMSENTSQL;DATABASE=STESTDB;UID=sa;PWD=EmsDB@dmIN8')
        cursor = conn.cursor()
        for index, row in result.iterrows():
            cursor.execute("Insert into storageTapePolicySummary([classId],[percentUsed],[mediaId],[Archive],[UNAVAIL],[Date]) values(?,?,?,?,?,?)",row['classId'],row['percentUsed'],row['mediaId'],row['Archive'],row['UNAVAIL'],row['Date'])
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    print('uploading util summary to db')
    try:
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER=EMSENTSQL;DATABASE=STESTDB;UID=sa;PWD=EmsDB@dmIN8')
        cursor = conn.cursor()
        for index, row in Summary.iterrows():
            cursor.execute("Insert into storageUtilSummary([Filesystem],[Size_TB],[UseP],[Date]) values(?,?,?,?)",row['Filesystem'],row['Size_TB'],row['UseP'],row['Date'])
        conn.commit()
    finally:
        cursor.close()
        conn.close()


if __name__=="__main__":
    main()
