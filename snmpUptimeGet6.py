from pysnmp import hlapi
from pysnmp.hlapi.asyncore import UdpTransportTarget
import csv
from tkinter import filedialog
from tkinter import *


#definitions
def cast(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        try:
            return float(value)
        except (ValueError, TypeError):
            try:
                return str(value)
            except (ValueError, TypeError):
                pass
    return value
def fetch(handler, count):
    result = []
    for i in range(count):
        try:
            error_indication, error_status, error_index, var_binds = next(handler)
            if not error_indication and not error_status:
                items = {}
                for var_bind in var_binds:
                    items[str(var_bind[0])] = cast(var_bind[1])
                result.append(items)
            else:
                raise RuntimeError('Got SNMP error: {0}'.format(error_indication))
        except StopIteration:
            break
    return result
def construct_object_types(list_of_oids):
    object_types = []
    for oid in list_of_oids:
        object_types.append(hlapi.ObjectType(hlapi.ObjectIdentity(oid)))
    return object_types
def get(target, oids, credentials, port=161, engine=hlapi.SnmpEngine(), context=hlapi.ContextData()):
    handler = hlapi.getCmd(
        engine,
        credentials,
        hlapi.UdpTransportTarget((target, port), timeout = 1, retries =1),
        context,
        *construct_object_types(oids)
    )
    return fetch(handler, 1)[0]

# ticks to time converter
def convertTicks(ticks):
    seconds = ticks/100
    d = seconds // 86400
    h = (seconds - d*86400) // 3600
    m = (seconds - d*86400- h*3600) // 60
    s = (seconds - d*86400- h*3600 -m*60)
    ticktime = (d,h,m,s)
    return "%d days,%d:%02d:%02d" % (d, h, m, s)


# ip  address input selector
intype = input("press M for manual input of ip_address :  ")
if intype == 'M':
    alpha = input("Enter Ip address with spaces ")
    a = alpha.split(" ")
else:
    a = []
    root = Tk()
    print("select the input csv file containing ip_address")
    root.filename =  filedialog.askopenfilename(initialdir = "/",title = "Select file",filetypes = (("csv files","*.csv"),("all files","*.*")))
    print (root.filename)

    with open(root.filename,'rt')as f:
        data = csv.reader(f)
        for row in data:
            a = a+row
    a.pop(0)


#Coomunity string selector
communitystring = input("Enter Community String: ")

#uptime calculatorh
b = {}
for x in a:
    try:
        y = get(x, ['1.3.6.1.2.1.1.3.0'], hlapi.CommunityData(communitystring))
        ticks = y["1.3.6.1.2.1.1.3.0"]
        uptime = ticks/100  #uptime is in seconds
        b.update({x: uptime})
    except RuntimeError as error:
        b.update({x: "No SNMP response"})
        pass

#set output file name
print("select the output path and file name to store the uptime results")
root = Tk()
root.filename =  filedialog.asksaveasfilename(initialdir = "/",title = "Select file",filetypes = (("csv files","*.csv"),("all files","*.*")))
print (root.filename)
file_to_open = root.filename+".csv"


#output the values
with open(file_to_open, mode = 'w') as file:
    writer =csv.writer(file)
    writer.writerow(["ip_address", "Uptime : days, hh:mm:ss", "Uptime in seconds"])
    for key, value in b.items():
        if value == "No SNMP response":
            writer.writerow([key,value,value])
        else:
            writer.writerow([key, convertTicks(value*100), value])
