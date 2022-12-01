import socket
import pickle
import os

workers=[1,2,3]
path=[1,2,3]
def server_program():
    x,y,z=0,0,0
    host = socket.gethostname()
    port = 5000  
    port_w=[22234,22235,22236]
    server_socket = socket.socket() 
    server_socket.bind((host, port))  
    server_socket.listen(2)
    conn, address = server_socket.accept() 
    print("Connection from: " + str(address))
    while True:
        data = pickle.loads(conn.recv(1024))
        if not data:
            break
        if data[-1]=="1":
            wok=pickle.dumps(workers)
            conn.send(wok)
        elif data[-1]=="2":
            wok=pickle.dumps(workers)
            conn.send(wok)
        elif data[-1]=="3":
            path1="Worker1_data/"+data[2]
            path2="Worker2_data/"+data[2]
            path3="Worker3_data/"+data[2]
            isExist1 = os.path.exists(path1)
            isExist2 = os.path.exists(path2)
            isExist3 = os.path.exists(path3)
            if isExist1==True:
                worker_conn1=socket.socket()
                worker_conn1.connect((host,port_w[0]))
                worker_conn1.send(pickle.dumps(data))
            if isExist2==True:
                worker_conn2=socket.socket()
                worker_conn2.connect((host,port_w[1]))
                worker_conn2.send(pickle.dumps(data))
            if isExist3==True:
                worker_conn3=socket.socket()
                worker_conn3.connect((host,port_w[2]))
                worker_conn3.send(pickle.dumps(data))

    conn.close() 


if __name__ == '__main__':
    server_program()
