import socket
import pickle
import os

workers=[1,2,3]
path=[1,2,3]
def server_program():
    # get the hostname
    host = socket.gethostname()
    port = 5000  # initiate port no above 1024
    port_w=[22234,22235,22236]
    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together

    # configure how many client the server can listen simultaneously
    server_socket.listen(2)
    conn, address = server_socket.accept()  # accept new connection
    print("Connection from: " + str(address))
    while True:
        # receive data stream. it won't accept data packet greater than 1024 bytes
        data = pickle.loads(conn.recv(1024))
        # print(data)
        if not data:
            # if data is not received break
            break
        if data[-1]=="1":
            wok=pickle.dumps(workers)
            conn.send(wok)
        elif data[-1]=="2":
            wok=pickle.dumps(workers)
            conn.send(wok)
        elif data[-1]=="3":
            path1="/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker1_data/"+data[2]
            path2="/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker2_data/"+data[2]
            path3="/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker3_data/"+data[2]
            isExist1 = os.path.exists(path1)
            isExist2 = os.path.exists(path2)
            isExist3 = os.path.exists(path3)
            worker_conn1=socket.socket()
            worker_conn2=socket.socket()
            worker_conn3=socket.socket()
            if isExist1==True:
                # print(1)
                worker_conn1.connect((host,port_w[0]))
                worker_conn1.send(pickle.dumps(data))
            if isExist2==True:
                # print(2)
                worker_conn2.connect((host,port_w[1]))
                worker_conn2.send(pickle.dumps(data))
            if isExist3==True:
                # print(3)
                worker_conn3.connect((host,port_w[2]))
                worker_conn3.send(pickle.dumps(data))
            ack=pickle.loads(worker_conn1.recv(1024))
            print(ack)
            if(ack=="ACK"):
                print("Mapping complete!!")
        # conn.send(data.encode())  # send data to the client

    conn.close()  # close the connection


if __name__ == '__main__':
    server_program()
