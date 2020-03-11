import threading
import paramiko
import subprocess
import sys
import csv

#ip
a = input("Enter IP with spaces: ")
a = a.split(" ")

#credentials
user = input("Enter Username: ")
passwd = input("Enter Password: ")


#fetch Code
b = {}

for ip in a:

    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(ip, username=user, password=passwd)
        print("SSH connection established to %s" % ip)

        remote_conn = client.invoke_shell()
        print("Interactive SSH session established")
        output = remote_conn.recv(1000)
        print(output)

        #enabling the promt
        command = "en"
        ssh_session = client.get_transport().open_session()
        ssh_session.exec_command(command)

        #running the command
        command = "show system | grep Current software"
        ssh_session = client.get_transport().open_session()
        ssh_session.exec_command(command)
        cmd_out = ssh_session.recv(1024)

        #converting input data to required format
        cmd_out = str(cmd_out, 'utf-8')
        version = cmd_out.split('\n')[0]
        version = version.split(':')[1]
        print(version)

        b.update({ip: version})

    except TimeoutError as error:
        print("Timeout")
        b.update({ip: "Time Out"})
        pass


#select output file & path
file_to_open = "Switches_Current_Version.csv"

#output the values
with open(file_to_open, mode= 'w') as file:
    writer = csv.writer(file)
    writer.writerow(['ip address', 'Current software'])
    for key, value in b.items():
        writer.writerow([key,value])
