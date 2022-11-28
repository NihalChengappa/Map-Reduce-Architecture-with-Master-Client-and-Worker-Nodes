import socket
import pickle
def worker1_program():
    # get the hostname
    host = socket.gethostname()
    port_c = 22234 # initiate port no above 1024

    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port_c))  # bind host address and port together

    # configure how many client the server can listen simultaneously
    server_socket.listen(2)
    conn, address = server_socket.accept()  # accept new connection
    print("Connection from: " + str(address))
    while True:
        # receive data stream. it won't accept data packet greater than 1024 bytes
        data = conn.recv(1024)
        if not data:
            # if data is not received break
            break
        wok=pickle.loads(data)
        name=wok[-1][-2]
        operation=wok[-1][-1]
        print(operation)
        if operation==1:
            with open('/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker1_data/'+name,mode='wt', encoding='utf-8') as myfile:
                for lines in wok[:-1]:
                    myfile.write(lines)

    conn.close()  # close the connection


if __name__ == '__main__':
    worker1_program()
