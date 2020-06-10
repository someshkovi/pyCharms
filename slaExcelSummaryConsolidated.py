import pandas as pd
import tkinter as tk
from tkinter import filedialog
import os
import glob
import numpy as np
import datetime

def Date_sheet(sheet):
    Day = str(sheet.loc[4, 'Unnamed: 1'])
    Month = sheet.loc[3, 'Unnamed: 1']
    Year = str(sheet.loc[2, 'Unnamed: 1'])
    Date = Day+Month+Year
    return Date


path = r'\\10.66.100.18\SLA_Reports\2020'
files = []
for r, d, f in os.walk(path):
    for file in f:
        if 'SLA Calculator' in file:
            files.append(os.path.join(r, file))

# Summary extract
i = 0
for f in files:
    try:
        #reading the excel
        SumD = pd.read_excel (f, sheet_name='SLA Daily')
        #fetch date in sheet
        Month = str(SumD.loc[0, 2020])
        Day = str(SumD.loc[1, 2020])
        Date = Day+Month+'2020'
        #fetch only the exclusion details
        Sdf = SumD.iloc[3:21,4:14]
        i = i+1
        new_header = Sdf.iloc[0] #grab the first row for the header
        Sdf = Sdf[1:] #take the data less the header row
        Sdf.columns = new_header #set the header row as the df header
        Value = Sdf.loc[Sdf.Parameter == r'Ratio of Live cameras v/s Total number of cameras at any point of time (To be measured every 1 hour)']
        Value.rename(index = {5: Date}, inplace = True)
        if i ==1:
            result = Value
        else:
            result = pd.concat([result,Value])
        print(Date+' added')
    except:
        #exceptions to be added manually
        print(Date+' Skipped')
        print(f)
        pass

result.to_excel("2020Ratio.xlsx")
