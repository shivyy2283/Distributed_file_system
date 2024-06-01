# datanode.py
import os
import threading
from concurrent.futures import ThreadPoolExecutor
import socket
import sys

class DataNode:
    def __init__(self, datanode_id, port, namenode_address):
        self.datanode_id = datanode_id
        self.port = port
        self.namenode_address = namenode_address
        self.data_blocks = {}
        self.data_folder = f"data_node_{datanode_id}"
        os.makedirs(self.data_folder, exist_ok=True)

    def handle_client(self, client_socket, addr):
        print(f"Connection from {addr}")
        operation_type = client_socket.recv(1024).decode()
        data = operation_type.split(',')

        if data[0] == "UPLOAD_BLOCK":
            self.handle_upload_block(client_socket, addr, data)
        elif data[0] == "DOWNLOAD_BLOCK":
            self.handle_download_block(client_socket, addr, data)
        else:
            print("Invalid operation type")

        client_socket.close()

    def handle_upload_block(self, client_socket, addr, data):
        file_name = data[1]
        block_id = data[2]
        block_data = data[3]

        file_folder = os.path.join(self.data_folder, file_name)
        os.makedirs(file_folder, exist_ok=True)

        block_file_path = os.path.join(file_folder, f"block_{block_id}.txt")
        with open(block_file_path, 'w') as block_file:
            block_file.write(block_data)

        self.data_blocks[block_id] = block_file_path
        acknowledgment = f"OK, Port: {self.port}"
        client_socket.send(acknowledgment.encode())

    

    def register_with_namenode(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as namenode_socket:
            namenode_socket.connect(self.namenode_address)
            namenode_socket.send(f"REGISTER_DATANODE,{self.datanode_id},{self.port}".encode())
            acknowledgment = namenode_socket.recv(1024).decode()
            print(f"Registration acknowledgment: {acknowledgment}")

    def start(self):
        self.register_with_namenode()

        with ThreadPoolExecutor(max_workers=10) as executor:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.bind(('localhost', self.port))
            server.listen(5)
            print(f"DataNode-{self.datanode_id} is listening for connections on port {self.port}...")

            while True:
                client_socket, addr = server.accept()
                executor.submit(self.handle_client, client_socket, addr)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python datanode.py <datanode_id> <port> <namenode_port>")
        sys.exit(1)

    datanode_id = int(sys.argv[1])
    port_number = int(sys.argv[2])
    namenode_port = int(sys.argv[3])
    namenode_address = ('localhost', namenode_port)

    datanode = DataNode(datanode_id=datanode_id, port=port_number, namenode_address=namenode_address)
    datanode.start()
