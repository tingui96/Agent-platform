from genericpath import exists
import os
import sys
import time
import pickle
import socket
import random
import hashlib
import threading

from tools import *
from collections import OrderedDict

class Node:

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.address = (ip, port)
        self.id = getHash(f'{ip}:{str(port)}')
        self.pred = (ip, port)
        self.predID = self.id
        self.succ = (ip, port)
        self.succID = self.id
        self.fingerTable = OrderedDict()
        self.servicio = None

    def escuchar(self):
        try:
            self.ServerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.ServerSocket.bind((IP, PORT))
            self.ServerSocket.listen()
        except socket.error:
            print('Socket not opened')

    def Cliente(self):
        self.menu()
        userChoice = input()
        if userChoice == '0':
            self.servicio = input("Que servicio desea brindar:")
            self.id = self.predID = self.succID = getHash(f'{self.servicio}')%1000 * 1000 + (getHash(f'{self.ip}:{str(self.port)}'))
            self.updateFingerTable()
            self.escuchar()
            self.start()
        elif userChoice == '1':
            self.servicio = input("Que servicio desea buscar")
            
       # elif userChoice == '3':
       #     self.printFingerTable()
       # elif userChoice == '4':
       #     print(f'My ID: {self.id}')
       #     print(f'Predecessor: {self.predID}')
       #     print(f'Successor: {self.succID}')
    
        
    def agente(self):
        print("1- Connect to the network\n3- Print Finger Table\n4- Node Information")    
        userChoice = input()
        if userChoice == '1':
            ip = input('Enter IP to connect: ')
            port = input('Enter port: ')
            self.sendJoinRequest(ip, int(port))
        elif userChoice == '3':
            self.printFingerTable()
        elif userChoice == '4':
            print(f'My ID: {self.id}')
            print(f'Predecessor: {self.pred , self.predID}')
            print(f'Successor: {self.succ, self.succID}')
        elif userChoice == '5':
            self.sendJoinRequest("127.0.0.1",8000)
        elif userChoice == '6':
            self.sendJoinRequest("127.0.0.1",8080)

    def printFingerTable(self):
        print('Printing Finger Table')
        for key, value in self.fingerTable.items(): 
            print(f'KeyID: {key}, Value: {value}')

    def menu(self):        
        print("0-Brindar Servicio\n1-Buscar Servicio")  
    
    def start(self):
        '''
        Accepting connections from other threads.
        '''
        threading.Thread(target=self.listenThread, args=()).start()
        threading.Thread(target=self.pingSucc, args=()).start()
        # In case of connecting to other clients
        while True:
            print('Listening to other clients')
            self.agente()

    def listenThread(self):
        '''
        Storing the IP and port in address and saving 
        the connection and threading.
        '''
        while True:
            try:
                connection, address = self.ServerSocket.accept()
                connection.settimeout(120)
                threading.Thread(target=self.connectionThread, args=(connection, address)).start()                
            except socket.error:
                pass

    def connectionThread(self, connection, address):
        '''
        datos[0] da el tipo de conexion
        Tipos de conecciones 0 : nodo nuevo
        '''
        datos = pickle.loads(connection.recv(BUFFER))
        connectionType = datos[0]
        if connectionType == 0:
            print(f'Connection with: {address[0]} : {address[1]}')
            print('Join network request recevied')
            self.joinNode(connection, address, datos)
        elif connectionType == 2:
            if datos[1] == 0:
                connection.sendall(pickle.dumps(self.pred))
            else:
                connection.sendall(pickle.dumps([2,1,self.succ]))
        elif connectionType == 3:
            recvData = self.SearchID(datos)
            connection.sendall(pickle.dumps(recvData))
        elif connectionType == 4:
            if datos[1] == 1:
                self.updateSucc(datos)
            else:
                self.updatePred(datos)
        elif connectionType == 5:
            self.updateFingerTable()
            connection.sendall(pickle.dumps(self.succ))   
        elif connectionType == 6:
            print("********??????????**********{}".format(datos))
            self.sendJoinRequest(datos[1][0], datos[1][1])
        else:
            print('Problem with connection type')

    def sendJoinRequest(self, ip, port):
        oldSucc = self.succ
        oldSuccID = self.succID
        print("*************{}***************".format((oldSucc,oldSuccID)))
        try:
            recvAddress = self.getSuccessor((ip, port), self.id)
            if recvAddress[0] != (self.address):
                print ((recvAddress == self.address))
                print("*********/////****{}*****/////*********".format(recvAddress))
                #print("mi sucesor es "+ str(recvAddress[1]))
                peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peerSocket.connect(recvAddress[0])
                #print("me conecto a mi sucesor")
                # 0 para que sepa que me quiero unir y le mando mi ip,puerto
                datos = [0, self.address,self.id]
                peerSocket.sendall(pickle.dumps(datos))

                #print("le envio mi direccion que es "+str(self.address[1]))
                #recibo en datos quien es mi antecesor
                datos = pickle.loads(peerSocket.recv(BUFFER))          
                self.pred = datos[0]
                self.predID = datos[1]
                #print("recibo mi antecesor "+ str(self.predID)) 
                self.succ = recvAddress[0]
                self.succID = recvAddress[1]
                #print("actualizo mi sucesor con "+str(self.succID))
                datos = [4, 1, self.address, self.id]
                pSocket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                #entra al antecesor para que actualice su sucesor conmigo
                pSocket2.connect(self.pred)
                pSocket2.sendall(pickle.dumps(datos))
                #print("Actualizo el sucesor de mi antecesor")
                pSocket2.close()
                peerSocket.close()
                if (oldSucc != self.address and oldSuccID != self.id):
                    pSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    pSocket.connect(oldSucc)
                    datos = [6, recvAddress[0]]
                    pSocket.sendall(pickle.dumps(datos))
                    pSocket.close()
            else:
                pass         
        except socket.error:
            print('Socket error. Recheck IP/Port.')

    def joinNode(self, connection, address, datos):
        '''
        Recibe la request del nodo nuevo
        '''
        if datos:
            #recibo la direccion del nodo
            peerAddr = datos[1]
            peerID = datos[2]
            #print("llego "+str(peerID))
            
            oldPred= self.pred
            oldPredID = self.predID
            self.pred = peerAddr
            self.predID = peerID
            #print("Actualizo mi predecesor con "+ str(self.predID))
            datos = [oldPred, oldPredID]
            #le envio a su antecesor
            #print("Le envio su predecesor que es "+str(datos))
            connection.sendall(pickle.dumps(datos))
            time.sleep(0.1)
            self.updateFingerTable()
            #self.printFingerTable()
            self.updateOtherFingerTables()       
            
    def getSuccessor(self, address, keyID):
        datos = [1, address]
        recvAddress = [address]
        while datos[0] == 1:
            peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                peerSocket.connect(recvAddress[0])
                #Buscar id
                datos = [3, keyID]
                print("*****{}*****".format(datos))
                peerSocket.sendall(pickle.dumps(datos))
                datos = pickle.loads(peerSocket.recv(BUFFER))
                print("--------{}-------".format(datos))
                recvAddress = [datos[1],datos[2]]
                peerSocket.close()
            except socket.error:
                print(recvAddress)
                print(address)
                print('Connection denied while getting Successor')
                exit()
        return recvAddress

    def updateFingerTable(self):
        for i in range(MAX_BITS):
            entryId = (self.id + (2 ** i)) % MAX_NODES
            #print("@@@@@@@@@@@  {}  @@@@@@@@@@@".format((entryId, self.succID, self.predID)))
            if self.succ == self.address:
                self.fingerTable[entryId] = (self.id, self.address)
                continue
            recvAddr = self.getSuccessor(self.succ, entryId)
            #print("@@@@@@@@@@@  {}  @@@@@@@@@@@".format(recvAddr))
            self.fingerTable[entryId] = (recvAddr[1], recvAddr[0])
        self.printFingerTable()


    def updateOtherFingerTables(self):
        here = self.succ
        
        while True:
            if here == self.address:
                break
            pSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                pSocket.connect(here)
                pSocket.sendall(pickle.dumps([5]))
                here = pickle.loads(pSocket.recv(BUFFER))
                pSocket.close()
                if here == self.succ:
                    break
            except socket.error:
                print('Connection denied')
        

    def updateSucc(self, datos):
        self.succ = datos[2]
        self.succID = datos[3]

    def updatePred(self, datos):
        self.pred = datos[2]
        self.predID = datos[3]


    def SearchID(self, datos):

        time.sleep(0.2)
        keyID = datos[1]
        datos = []
        print((self.id, keyID))
        # Caso 0: si soy yo
        # x = yo
        # y = pred
        # z = succ
        # w = keyID
        # a...b = pueden existir n nodos entre a y b
        # a,b = a y b son consecutivos 
        
        if self.id == keyID: # w = x
            print("entre1")
            time.sleep(0.2)
            datos = [0, self.address, self.id]
        # Caso 1: si nada mas existe 1 nodo
        elif self.succID == self.id: #[x,w,x]
            print("entre2")
            time.sleep(0.2)
            datos = [0, self.address, self.id]
        # Caso 2: si mi id es mayor que el keyID, preguntar al antecesor
        elif self.id > keyID: #[...w...x...]
            if self.predID < keyID: #[...,y,w,x,...]
                print("entre3")
                time.sleep(0.2)
                datos = [0, self.address, self.id]
            elif self.predID > self.id: #[w,x,...,y]
                print("entre4")
                time.sleep(0.2)
                datos = [0, self.address, self.id]
            else:   #[...,w,...,y,x,...]
                print("entre5")
                time.sleep(0.2)
                #datos = [1, self.pred, self.predID]
                datos = [1, self.succ, self.succID]
                for key, value in self.fingerTable.items():
                    if key < self.predID and key > keyID:
                        datos = [1, value[1], value[0]]
                        print(datos)
                        break
        # Case 3: si mi id es menor que el keyID, usar la fingertable para buscar al mas cercano
        else: #[...x...w...]
            if self.succID < self.id or keyID < self.succID: #[z,...,y,x,w] or [...y,x,w,z...]
                print("entre6")
                time.sleep(0.2)
                datos = [0, self.succ, self.succID]
            else: #[...x,z...w...]
                if self.predID < keyID and self.predID > self.id : #[x,z,...,y,w]
                    time.sleep(0.2)
                    datos = [0, self.address, self.id]
                elif self.succID < self.predID and self.predID < keyID: #[x,z,...,y,w]
                    datos = [0, self.address, self.id]
                else:
                      #[x,z,...,w,...,y]
                      #[...,y,x,z,...,w,...]
                      # en estos casos mi sucesor nunca es sucesor del nodo nuevo
                    time.sleep(0.2)
                    print("entre7")
                    self.printFingerTable()
                    datos = [1, self.succ, self.succID]
                    for key, value in self.fingerTable.items():
                        if key > keyID and value[0] > self.succID:
                            print((value[1]))
                            datos = [1, value[1], value[0]]
                            break
        return datos

    def buscarServicio(self):
        pass

    def pingSucc(self):
        while True:
            # Ping every 5 seconds
            time.sleep(5)
            if self.address == self.succ:
            # If only one node, no need to ping
                continue
            try:
                pSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                #print("abri el socket")
                pSocket.connect(self.succ)
                #print("me conecte")
                pSocket.sendall(pickle.dumps([2,0]))
                #print("envie ping")
                recvPred = pickle.loads(pSocket.recv(BUFFER))
                
            except:
                print('\nNode offline detected \nStabilizing...')
                
                if not self.succ == self.pred:
                    # Search for the next succ
                    print("$$$$$$$$$${}$$$$$$$$$$$".format((self.pred, self.succID)))
                    recvAddr = self.getSuccessor(self.pred, self.succID+1)
                    self.succ = recvAddr[0]
                    self.succID = recvAddr[1]

                    # Informa al nuevo sucesor para que actualice su predecesor conmigo
                    pSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    pSocket.connect(self.succ)
                    pSocket.sendall(pickle.dumps([4, 0, self.address, self.id]))
                    pSocket.close()

                else:
                    #creo que esto sobra ahora
                    self.pred = self.address
                    self.predID = self.id
                    self.succ = self.address
                    self.succID = self.id
                    
                self.updateFingerTable()
                self.updateOtherFingerTables()
                self.menu()  
   

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print('Arguments not supplied (defaults used)')
    else:
        IP = sys.argv[1]
        PORT = int(sys.argv[2])

    node = Node(IP, PORT)
    print(f'My ID is: {node.id}')
    node.Cliente()