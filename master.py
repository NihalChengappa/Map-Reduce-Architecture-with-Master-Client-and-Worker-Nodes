import socket
import pickle
import os

workers=[1,2,3]
path=[1,2,3]
def server_program():
    # get the hostname
    host = socket.gethostname()
    port = 5000  # initiate port no above 1024
    port_w=[22234,22235,5003]
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
        if not data:
            # if data is not received break
            break
        if data[0]=="1":
            wok=pickle.dumps(workers)
            conn.send(wok)
        elif data[0]=="2":
            wok=pickle.dumps(workers)
            conn.send(wok)
        elif data[0]=="3":
            print(data)
            path1="/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker1_data/"+data[3]
            path2="/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker2_data/"+data[3]
            path3="/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker3_data/"+data[3]
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
        # conn.send(data.encode())  # send data to the client

    conn.close()  # close the connection


if __name__ == '__main__':
    server_program()
