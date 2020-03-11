import pandas as pd
import xlsxwriter
import sys
import glob

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

list_of_files = [f for f in glob.glob("2020-02-22_17.xml")]


devlist = []
for f in list_of_files:
    data = readNnmXmlFile(f)
    devlist = devlist+data

xmlcolumns = ['id','shortname','status','nodelaststatuschange','NPS Annotation','File']

XML = nnmXmlList2df(devlist,xmlcolumns)

XML.loc[:,'File'] = XML.File.apply(lambda c: file2dt(c))
