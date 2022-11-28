import socket
import pickle
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
    host = socket.gethostname()  # as both code is running on same pc
    port = 5000  # socket server port number
    port_w=[22234,22235,5003]
    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server

    message = input(" Input: ")  # take input

    while message.strip() != '4':
        count=0
        if(message=='1'):
            ip1=input("No of worker(1-3):")
            ip2=input("Enter path of file:")
            name=ip2.split('/')[-1]
            client_socket.send(pickle.dumps((message)))  # send message
            data = pickle.loads(client_socket.recv(1024))  # receive response
            data=data[:int(ip1)]
            parts=partition_data(ip2,int(ip1))
            if(count<len(data)):
                parts[0].append((name,1))
                worker_conn1=socket.socket()
                worker_conn1.connect((host,port_w[0]))
                send_data=pickle.dumps(parts[0])
                worker_conn1.send(send_data)
                count+=1
            if(count<len(data)):
                parts[1].append((name,1))
                worker_conn2=socket.socket()
                worker_conn2.connect((host,port_w[1]))
                send_data=pickle.dumps(parts[1])
                worker_conn2.send(send_data)
                count+=1
            if(count<len(data)):
                parts[2].append((name,1))
                worker_conn3=socket.socket()
                worker_conn3.connect((host,port_w[2]))
                send_data=pickle.dumps(parts[2])
                worker_conn3.send(send_data)
                count+=1
            # parts=partition_data(ip2,int(ip1))
            # send_data=pickle.dumps(parts)
            # worker_conn1.send(send_data)
            # print(parts)
        if message=='2':
            f_name=input("Input filename:")
            client_socket.send((pickle.dumps(message)))
            data = pickle.loads(client_socket.recv(1024))  # receive response
            path1="/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker1_data/"+f_name
            path2="/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker2_data/"+f_name
            path3="/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker3_data/"+f_name
            isExist1 = os.path.exists(path1)
            isExist2 = os.path.exists(path2)
            isExist3 = os.path.exists(path3)
            if isExist1==True:
                with open(path1) as w1:
                    for line in w1:
                        print(line)
            if isExist2==True:
                with open(path2) as w2:
                    for line in w2:
                        print(line)
            if isExist3==True:
                with open(path3) as w3:
                    for line in w3:
                        print(line)
        if message=="3":
            mp=input("Enter mapper path:")
            rd=input("Enter reducer path:")
            fn=input("Enter file name:")
            exec=input("Enter execution command:")
            client_socket.send(pickle.dumps((message,mp,rd,fn,exec)))

        message = input(" -> ")  # again take input
        
    client_socket.close()  # close the connection


if __name__ == '__main__':
    client_program()
