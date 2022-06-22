from hashlib import sha512
import threading
from Agent import *
from tools import *
import socket
import struct
import sys

class Node:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.address = (ip,port)

        self.id = getHash(ip+port)
        try:
            self.ServerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.ServerSocket.bind((IP, PORT))
            self.ServerSocket.listen()
        except socket.error:
            print("Socket error")
        print(self.id)


def main():
    print(sys.argv)
    node = Node(sys.argv[1],sys.argv[2])
    startClient(node)

def startClient(node):
    threading.Thread(target=listenThread, args=(node)).start()
    threading.Thread(target=pingSucc, args=(node)).start()
    conectClient(node)
    while True:
        print('Listening')
        depMenu(node)

def listenThread(node):
    while True:
        try:
            connection, address = node.ServerSocket.accept()
            connection.settimeout(120)
            threading.Thread(target= connectionThread, args=(node, connection, address)).start()                
        except socket.error:
            pass

def pingSucc(node):
    pass

def connectionThread(node, connection, address):
    pass

def conectClient(node):
    ip = input('Enter IP to connect: ')
    port = input('Enter port: ')
    name = input('Enter user name: ')
    return node

def depMenu(node):
    user_input = input()
    if user_input == '1':
        objective = input()
        agent = searchAgent(node,objective)
        try:
            print("El producto {0} se encuentra en existencia".format(agent.name))
        except:
            print("El producto {0} no se encuentra en existencia".format(objective))
    elif user_input == '2':
        pass   

def addAgent():
    inp = input().split(" ")
    data = struct
    data.address = (inp[0], inp[1])
    data.id = inp[2]
    data.admin = False
    return Agent(data)
    

def updAgentStatus(agent = Agent, status = bool):
    agent.admin = status
    pass

def searchAgent():
    pass


if __name__ == '__main__':
    main()