import pandas as pd
import time

data_folder = "E:\\nnm\\Data\\"

while True:
    timestr = time.strftime("%Y%m%d-%H")
    Status, = pd.read_html("http://hcsc-emsnnmi.tspd.telangana.gov.in/nnm/protected/nodegroupstatus.jsp?nodegroup=cameras&j_username=asset&j_password=asset", header=0)
    file_to_open = data_folder + timestr+".csv"
    Status.to_csv(file_to_open, index=False)
    time.sleep(60*60)
