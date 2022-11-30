import socket
import pickle
import time
import os
def partition_data(path,no_workers):
    partitions=[]
    for i in range(no_workers):
        partitions.append([])
    file=open(path, "r")
    lines=len(file.readlines())
    lines_per_file=lines//no_workers
    line_no,i=1,0
    file.seek(0)
    for line in file:
        if i==no_workers-1:
            partitions[i].append(line)
        elif line_no>lines_per_file:
            line_no=1
            i+=1
            partitions[i].append(line)
            line_no+=1
        else:
            partitions[i].append(line)
            line_no+=1
    return partitions


def client_program():
    x,y,z=0,0,0
    host = socket.gethostname()  # as both code is running on same pc
    port = 5000  # socket server port number
    port_w=[22234,22235,22236]
    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server

    message = input(" Input: ")  # take input

    while message.strip() != '4':
        count=0
        if(message=='1'):
            ip1=input("No of worker(1-3):")
            ip2=input("Enter path of file:")
            name=ip2.split('/')[-1]
            for i in range(len(port_w)):
                if(os.path.exists("/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker"+str(i+1)+"_data/"+name)):
                    os.remove("/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker"+str(i+1)+"_data/"+name)
            client_socket.send(pickle.dumps((message)))  # send message
            data = pickle.loads(client_socket.recv(1024))  # receive response
            data=data[:int(ip1)]
            parts=partition_data(ip2,int(ip1))
            if(count<len(data)):
                if(x==0):
                    worker_conn1=socket.socket()
                    worker_conn1.connect((host,port_w[0]))
                    x=1
                parts[0].append((name,1))
                send_data=pickle.dumps(parts[0])
                worker_conn1.send(send_data)
                count+=1
            if(count<len(data)):
                if(y==0):
                    worker_conn2=socket.socket()
                    worker_conn2.connect((host,port_w[1]))
                    y=1
                parts[1].append((name,1))
                send_data=pickle.dumps(parts[1])
                worker_conn2.send(send_data)
                count+=1
            if(count<len(data)):
                if(z==0):
                    worker_conn3=socket.socket()
                    worker_conn3.connect((host,port_w[2]))
                    z=1
                parts[2].append((name,1))
                send_data=pickle.dumps(parts[2])
                worker_conn3.send(send_data)
                count+=1
        if message=='2':
            f_name=input("Input filename:")
            client_socket.send((pickle.dumps(message)))
            data = pickle.loads(client_socket.recv(1024))  # receive response
            if(x==0):
                worker_conn1=socket.socket()
                worker_conn1.connect((host,port_w[0]))
                x=1
            if(y==0):
                worker_conn2=socket.socket()
                worker_conn2.connect((host,port_w[1]))
                y=1
            if(z==0):
                worker_conn3=socket.socket()
                worker_conn3.connect((host,port_w[2]))
                z=1
            worker_conn1.send(pickle.dumps((f_name,"2")))
            worker_conn2.send(pickle.dumps((f_name,"2")))
            worker_conn3.send(pickle.dumps((f_name,"2")))
            f1=pickle.loads(worker_conn1.recv(1024))
            f2=pickle.loads(worker_conn2.recv(1024))
            f3=pickle.loads(worker_conn3.recv(1024))
            print(f1,f2,f3)
        if message=="3":
            mp=input("Enter mapper path:")
            rd=input("Enter reducer path:")
            fn=input("Enter file name:")
            client_socket.send(pickle.dumps((mp,rd,fn,message)))
            time.sleep(5)
            es1=os.path.exists("/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker1_data/"+fn.split(".")[0]+"_op")
            es2=os.path.exists("/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker2_data/"+fn.split(".")[0]+"_op")
            es3=os.path.exists("/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker3_data/"+fn.split(".")[0]+"_op")
            if es1==True:
                fes1=open("/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker1_data/"+fn.split(".")[0]+"_op","r")
                for line in fes1:
                    print(line)
            if es2==True:
                fes2=open("/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker2_data/"+fn.split(".")[0]+"_op","r")
                for line in fes2:
                    print(line)
            if es3==True:
                fes3=open("/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker3_data/"+fn.split(".")[0]+"_op","r")
                for line in fes3:
                    print(line)
        message = input(" -> ")  # again take input
        
    client_socket.close()  # close the connection


if __name__ == '__main__':
    client_program()
