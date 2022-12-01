import socket
import subprocess
from partition import *
import pickle
import os
def worker2_program():
    host = socket.gethostname()
    port_c = 22235

    server_socket = socket.socket() 
    server_socket.bind((host, port_c)) 
    server_socket.listen(5)
    conn, address = server_socket.accept()  
    print("Connection from: " + str(address))
    while True:
        data = conn.recv(1024)
        if not data:
            break
        wok=pickle.loads(data)
        operation=wok[-1][-1]
        if operation==1:
            name=wok[-1][-2]
            with open('Worker2_data/'+name,mode='wt', encoding='utf-8') as myfile:
                for lines in wok[:-1]:
                    myfile.write(lines)
        elif int(operation)==2:
            fname=wok[0]
            path="Worker2_data/"+fname
            isExist= os.path.exists(path)
            if isExist==True:
                conn.send(pickle.dumps(("ACK")))
            else:
                conn.send(pickle.dumps(("NAK")))
        elif int(operation)==3:
            f_name=wok[2]
            map_path=wok[0]
            red_path=wok[1]
            arg=wok[3]
            path="Worker2_data/"
            path2="Worker1_data/"
            path3="Worker3_data/"
            isExist= os.path.exists(path+f_name)
            if isExist==True:
                f=open(path+f_name.split(".")[0]+"_mapped","w")
                ps = subprocess.Popen(('cat',path+f_name ), stdout=subprocess.PIPE)
                subprocess.call(('python3', map_path), stdin=ps.stdout,stdout=f)
                ps.wait()
                path_m=path+f_name.split(".")[0]+"_mapped"
                if(os.path.exists(path_m)):
                    count=1
                if os.path.exists(path2+f_name.split(".")[0]+"_mapped"):
                    count+=1
                if os.path.exists(path3+f_name.split(".")[0]+"_mapped"):
                    count+=1
                partition_fn(f_name.split(".")[0],path_m,count)
                fr=open(path+f_name.split(".")[0]+"_op","w")
                ps2 = subprocess.Popen("cat "+path+f_name.split(".")[0]+"_partition"+" | sort -k1,1",shell=True,stdout=subprocess.PIPE)
                subprocess.call(('python3', red_path), stdin=ps2.stdout,stdout=fr)
                ps2.wait()
                os.remove(path+f_name.split(".")[0]+"_partition")
    conn.close()  # close the connection


if __name__ == '__main__':
    worker2_program()
