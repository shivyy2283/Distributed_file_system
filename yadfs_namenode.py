# namenode.py
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
import random
import os

class NameNode:
    def __init__(self):
        self.metadata = {}
        self.datanodes = []
        self.datanode_addresses = []
        self.lines_per_block_threshold = 5
        self.file_number_counter = 1

    def write_metadata_to_bin_file(self, file_path):
        with open(file_path, 'w') as bin_file:
            for file_name, file_info in self.metadata.items():
                blocks = file_info.get('blocks', [])
                replicas= file_info.get('replicas', [])

                file_number = self.file_number_counter
                self.file_number_counter += 1
                bin_file.write(f"{file_number}:{file_name}\n")

                # Write block and datanode information
                for block in blocks:
                    block_id = block.get('block_id', 0)
                    datanode = block.get('datanode', 0)
                    bin_file.write(f"{file_number},{datanode},{block_id}\n")

                for replica in replicas:
                    block_id = block.get('block_id', 0)
                    datanode = block.get('datanode', 0)
                    bin_file.write(f"{file_number},{datanode},{block_id}\n")

    def handle_client(self, client_socket, addr):
        print(f"Connection from {addr}")
        operation_type = client_socket.recv(1024).decode()
        data = operation_type.split(',')
        print(operation_type)

        if data[0] == "UPLOAD":
            self.handle_upload(client_socket, addr, data)
        elif data[0] == "DOWNLOAD":
            self.handle_download(client_socket, addr, data)
        elif data[0] == "REGISTER_DATANODE":
            self.register_datanode(client_socket, addr, data)
        else:
            print("Invalid operation type")

        client_socket.close()

    def handle_upload(self, client_socket, addr, data):
        print("in upload")
        file_name, file_size = data[1], data[2]
        print("File name:", file_name)
        print("File size:", file_size)

        self.metadata[file_name] = {'blocks': [], 'replicas': [], 'datanodes': []}
        client_socket.send("OK".encode())

        block_id = 1
        block_data = ""
        while True:
            line = client_socket.recv(1).decode()
            if not line:
                break

            block_data += line
            if line.strip() == "" and block_data.count('\n') >= self.lines_per_block_threshold:
                datanode_address = random.choice(self.datanode_addresses)
                datanode_address_dup= random.choice(self.datanode_addresses)
                while (datanode_address==datanode_address_dup):
                    datanode_address_dup= random.choice(self.datanode_addresses)


                for i in self.datanodes:
                    if i['port'] == datanode_address[1]:
                        datanode_info = i['datanode_id']
                block_path = f"data_node_{datanode_info}\\{file_name}\\block_{block_id}.txt"
                self.metadata[file_name]['blocks'].append({'block_id': block_id, 'size': len(block_data),
                                                           'datanode': datanode_info, 'path': block_path})
                
                self.metadata[file_name]['replicas'].append({'block_id': block_id, 'size': len(block_data),
                                                           'datanode': datanode_info, 'path': block_path})
                
                if datanode_info not in self.metadata[file_name]['datanodes']:
                    self.metadata[file_name]['datanodes'].append(datanode_info)
                self.send_block_to_datanode(file_name, block_id, block_data, datanode_address)
                self.send_block_to_datanode(file_name, block_id, block_data, datanode_address_dup)
                

                block_id += 1
                block_data = ""

        if block_data:
            datanode_address = random.choice(self.datanode_addresses)
            datanode_address_dup= random.choice(self.datanode_addresses)
            while (datanode_address==datanode_address_dup):
                datanode_address_dup= random.choice(self.datanode_addresses)
            for i in self.datanodes:
                if i['port'] == datanode_address[1]:
                    datanode_info = i['datanode_id']

            block_path = f"data_node_{datanode_info}\\{file_name}\\block_{block_id}.txt"
            self.metadata[file_name]['blocks'].append({'block_id': block_id, 'size': len(block_data),
                                                       'datanode': datanode_info, 'path': block_path})
            self.metadata[file_name]['replicas'].append({'block_id': block_id, 'size': len(block_data),
                                                           'datanode': datanode_info, 'path': block_path})
            if datanode_info not in self.metadata[file_name]['datanodes']:
                self.metadata[file_name]['datanodes'].append(datanode_info)
            self.send_block_to_datanode(file_name, block_id, block_data, datanode_address)
            self.send_block_to_datanode(file_name, block_id, block_data, datanode_address_dup)

        print("out upload")
        self.write_metadata_to_bin_file('metadata.bin')
        print("after write")
        print(self.metadata)

    def handle_download(self, client_socket, addr, data):
        print("in handle download")
        file_name = data[1]
        self.download_blocks(client_socket, file_name)

        print(f"File '{file_name}' downloaded successfully.")

    def download_blocks(self, client_socket, file_name):
        print("in down blocks dddd")
        paths=[]
        data=self.metadata[file_name]
        print("data ",data)

        p=data['blocks']
        print("p ", p)
        for j in p:
            print("j is   ", j)
            print("j path ",j['path'])
            paths.append(j['path'])
        print("list of paths", paths)


        with open('download.txt', 'w') as download_file:
        # Iterate over each path and read the content of the corresponding file
            for path in paths:
                with open(path, 'r') as block_file:
                    block_content = block_file.read()
                    # Write the block content to the download.txt file
                    download_file.write(block_content)

        
               

    def register_datanode(self, client_socket, addr, data):
        datanode_id = int(data[1])
        port = int(data[2])
        datanode = {'datanode_id': datanode_id, 'port': port}
        self.datanodes.append(datanode)
        self.datanode_addresses.append((addr[0], port))
        print(f"DataNode-{datanode_id} registered at {addr[0]}:{port}")
        client_socket.send("OK".encode())

    def send_block_to_datanode(self, file_name, block_id, block_data, datanode_address):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as datanode_socket:
            datanode_socket.connect(datanode_address)
            datanode_socket.send(f"UPLOAD_BLOCK,{file_name},{block_id},{block_data}".encode())
            acknowledgment = datanode_socket.recv(1024).decode()
            print(f"DataNode acknowledgment: {acknowledgment}")

    def get_active_datanodes(self):
        return self.datanodes

    def start(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.bind(('localhost', 5000))
            server.listen(5)
            print("NameNode is listening for connections...")

            while True:
                client_socket, addr = server.accept()
                executor.submit(self.handle_client, client_socket, addr)

if __name__ == "__main__":
    namenode = NameNode()
    namenode.start()
    print("before write")
    namenode.write_metadata_to_bin_file('metadata.bin')
    print("here end")
