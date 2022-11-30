import socket
import subprocess
from partition import *
import pickle
import os
def worker2_program():
    # get the hostname
    host = socket.gethostname()
    port_c = 22235 # initiate port no above 1024

    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port_c))  # bind host address and port together

    # configure how many client the server can listen simultaneously
    server_socket.listen(5)
    conn, address = server_socket.accept()  # accept new connection
    print("Connection from: " + str(address))
    while True:
        # receive data stream. it won't accept data packet greater than 1024 bytes
        data = conn.recv(1024)
        if not data:
            # if data is not received break
            break
        wok=pickle.loads(data)
        operation=wok[-1][-1]
        if operation==1:
            name=wok[-1][-2]
            with open('/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker2_data/'+name,mode='wt', encoding='utf-8') as myfile:
                for lines in wok[:-1]:
                    myfile.write(lines)
        elif int(operation)==2:
            fname=wok[0]
            path="/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker2_data/"+fname
            isExist= os.path.exists(path)
            if isExist==True:
                conn.send(pickle.dumps(("ACK")))
            else:
                conn.send(pickle.dumps(("NAK")))
                # arr=[]
                # f=open(path,"r")
                # file_cont=f.read()
                # conn.send(pickle.dumps(file_cont))
        elif int(operation)==3:
            # print(1)
            f_name=wok[2]
            map_path=wok[0]
            red_path=wok[1]
            arg=wok[3]
            path="/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker2_data/"
            path2="/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker1_data/"
            path3="/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker3_data/"
            isExist= os.path.exists(path+f_name)
            if isExist==True:
                f=open(path+f_name.split(".")[0]+"_mapped","w")
                ps = subprocess.Popen(('cat',path+f_name ), stdout=subprocess.PIPE)
                subprocess.call(('python3', map_path), stdin=ps.stdout,stdout=f)
                ps.wait()
                path_m=path+f_name.split(".")[0]+"_mapped"
                if(os.path.exists(path_m)):
                    count=1
                    # print(1)
                    # conn.send(pickle.dumps("ACK"))
                if os.path.exists(path2+f_name.split(".")[0]+"_mapped"):
                    count+=1
                if os.path.exists(path3+f_name.split(".")[0]+"_mapped"):
                    count+=1
                partition_fn(f_name.split(".")[0],path_m,count)
                fr=open(path+f_name.split(".")[0]+"_op","w")
                ps2 = subprocess.Popen("cat "+path+f_name.split(".")[0]+"_partition"+" | sort -k1,1",shell=True,stdout=subprocess.PIPE)
                # output2=subprocess.Popen(("sort -k1,1"), stdin=ps.stdout,stdout=subprocess.PIPE)
                subprocess.call(('python3', red_path), stdin=ps2.stdout,stdout=fr)
                ps2.wait()
                os.remove(path+f_name.split(".")[0]+"_partition")
    conn.close()  # close the connection


if __name__ == '__main__':
    worker2_program()
