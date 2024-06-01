import socket
import os
import concurrent.futures
import math
import pickle
import random
def print_hierarchy(data, indent=0):
    for key, value in data.items():
        print('  ' * indent + f'{key}/')
        if isinstance(value, dict):
            print_hierarchy(value, indent + 1)
        elif isinstance(value, set):
            for file in value:
                print('  ' * (indent + 1) + f'{file}')

def read_file_contents(filename):
        try:
            with open(filename, "r") as file:
                contents = file.read()
                print(f"Contents of '{filename}':\n{contents}")
        except FileNotFoundError:
            print(f"File '{filename}' not found.")

def add_directory(directory_name):
        hierarchy_file = "directories.dat"
        try:
            with open(hierarchy_file, "rb") as direc:
                hierarchy = pickle.load(direc)

            hierarchy[directory_name] = []

            with open(hierarchy_file, "wb") as direc:
                pickle.dump(hierarchy, direc)
            os.mkdir(directory_name)
            print(f"Directory '{directory_name}' added successfully.")
        except FileNotFoundError:
            print("Hierarchy not found. Please create a file first.")

def create_and_write_file(file_name, content):
    try:
        with open(file_name, 'w') as file:
            file.write(content)
        print(f"File '{file_name}' created and written successfully.")

        # Update the hierarchy
        update_hierarchy(file_name)
    except Exception as e:
        print(f"Error: {e}")

def update_hierarchy(file_name):
    # Update the hierarchy with the new file
    # Assuming hierarchy is a dictionary with directory names as keys and lists of files as values
    directory_path, _ = os.path.split(file_name)
    directory_name = os.path.basename(directory_path)
    hierarchy_file = "directories.dat"
    
    #with open(hierarchy_file, "rb") as direc:
            #hierarchy = pickle.load(direc)
    print("keys are ", hierarchy.keys())

    if directory_name not in hierarchy.keys():
        print("not in ")
        hierarchy[directory_name] = []

    hierarchy[directory_name].append(os.path.basename(file_name))

    print("hierachy is ", hierarchy)

    # Save the updated hierarchy to directories.dat
    with open("directories.dat", "wb") as direc:
        pickle.dump(hierarchy, direc)
    print("Hierarchy updated successfully.")

# Assuming 'hierarchy' is the initial hierarchy loaded from directories.dat
hierarchy = {}  # Replace this with your actual hierarchy

def print_hierarchy(hierarchy, indent=0):
    for directory, files in hierarchy.items():
        print("  " * indent + f"- {directory}/")
        for file in files:
            print("  " * (indent + 1) + f"- {file}")

def display_hierarchy():
    
    try:
        with open("directories.dat", "rb") as direc:
            hierarchy = pickle.load(direc)
            print("Current Hierarchy:")
            print_hierarchy(hierarchy)
            print("hierachy is ", hierarchy)
    except FileNotFoundError:
        print("Hierarchy not found. Please create a file first.")


def addrec():
    #new file creation only root take this out after running for the first time
    filepath=eval(input())
    l=filepath.split("\\")
    direc=open("directories.dat","rb")
    row=pickle.load(direc)
    direc.close()
    temp1=row["root"]
    temp_keys=list(temp1.keys())
    findfile=["root", ]
    a=0
    #print(temp_keys)
    temp=temp1
    for a in range(1, len(l)):
        if "." not in l[a]:
            #print(temp_keys)
            for b in temp_keys:
                if b==l[a]:
                    temp=temp[b]
                    try:
                        temp_keys=temp.keys()
                    except:
                        break
                    break
            else:
                temp[l[a]]={}
                #temp=temp[l[a]]      
        else:
            #print(temp_keys)
            try:
                #print(l[a-1])
                temp[l[a-1]]={l[a],}
                
            except:
                #print(temp)
                val=temp
                val.add(l[a])
                #temp[l[a-1]]=val
    fi=open("directories.dat","wb")
    d={}
    d["root"]=temp1
    pickle.dump(d,fi)
    #print(d)
    fi.close()

class Client:
    def __init__(self):
        self.namenode_address = ('localhost', 5000)
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lines_per_block = 100  # Set your desired lines per block

    def connect_to_namenode(self):
        self.client_socket.connect(self.namenode_address)

    def send_operation_type(self, operation_type, file_name="", file_size=""):
        message = f"{operation_type},{file_name},{file_size}"
        self.client_socket.send(message.encode())

    def upload_file(self, file_path):
        self.connect_to_namenode()
        
        # Send operation type
        #self.send_operation_type("UPLOAD")

        # Send file name and size
        file_name = file_path.split("/")[-1]
        file_size = str(os.path.getsize(file_path))
        self.send_operation_type("UPLOAD", file_name, file_size)

        # Receive acknowledgment
        acknowledgment = self.client_socket.recv(1024).decode()
        print(f"Upload acknowledgment: {acknowledgment}")

        # Read the file and send blocks to the NameNode for further processing
        with open(file_path, 'r') as file:
            lines = file.readlines()
            for i in range(0, len(lines), self.lines_per_block):
                block_data = ''.join(lines[i:i+self.lines_per_block])
                # Send block data to NameNode
                self.client_socket.send(block_data.encode())
                # Add logic to handle acknowledgment from NameNode if necessary

    def download_file(self, file_name):
        self.connect_to_namenode()
        
        # Send file name
        self.send_operation_type("DOWNLOAD", file_name)

    def close_connection(self):
        self.client_socket.close()

if __name__ == "__main__":
    
    
    #client.upload_file("sample.txt")
    #client.download_file("file.txt")
    #addrec()
    while True:
        print("\nMenu:")
        print("1. Display Hierarchy")
        print("2. Upload a file")
        print("3. Download a file")
        print("4. Create a file")
        print("5. Create a Directory")
        print("7. Read contents of a file")
        print("8. Exit")

        choice = input("Enter your choice (1-5): ")

        if choice == "1":
            #direc = open("directories.dat", "rb")
            #hierarchy = pickle.load(direc)
            #direc.close()
            #print_hierarchy(hierarchy)
            # Usage example
            display_hierarchy()

        if choice == "2":
            client = Client()
            uf = eval(input("Enter file name to upload: "))
            client.upload_file(uf)
            client.close_connection()

        if choice == "3":
            client = Client()
            df = eval(input("Enter file name to be downloaded: "))
            client.download_file(df)
            try:
                with open("download.txt", "r") as file:
                    contents = file.read()
                    print(f"Contents downloaded are:\n{contents}")
            except FileNotFoundError:
                print(f"File '{filename}' not found.")
            client.close_connection()

        if choice == "4":
            file_name = input("Enter the file name to create: ")
            content = input("Enter the content to write to the file: ")
            create_and_write_file(file_name, content)

        if choice=="5":
            directory_name = input("Enter directory name to add: ")
            add_directory(directory_name)

        if choice == "7":
            filename = input("Enter file name to read: ")
            read_file_contents(filename)

        if choice == "8":
            break