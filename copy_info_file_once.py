import pandas as pd
import time
import os
import shutil

src_dir=r'\\10.66.100.14\Applications\EMS\Status\info.xml'

timestr = time.strftime("%Y-%m-%d_%H")
print(timestr)
dst_dir = "E:\\nnm\\Status\\"+timestr+".xml"
shutil.copy(src_dir,dst_dir)
