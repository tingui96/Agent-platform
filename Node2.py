from fileinput import close
from genericpath import exists
from operator import mod
import os
from sqlite3 import connect
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

    def menu(self):        
        print("0-Brindar Servicio\n1-Buscar Servicio")  

    def Cliente(self):
        self.menu()
        userChoice = input()
        if userChoice == '0':
            self.servicio = input("Que servicio desea brindar:")
            self.id = self.predID = self.succID = getHashId((self.ip,self.port),self.servicio)
            self.escuchar()
            self.updateFingerTable()
            self.start()
        elif userChoice == '1':
            self.servicio = input("Que servicio desea buscar")
        
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
            print(f'Predecessor: {self.predID}')
            print(f'Successor: {self.succID}')

    def printFingerTable(self):
        print('Printing Finger Table')
        for key, value in self.fingerTable.items(): 
            print(f'KeyID: {key}, Value: {value}')

    
    
    def start(self):
        '''
        Accepting connections from other threads.
        '''
        threading.Thread(target=self.listenThread, args=()).start()
        #threading.Thread(target=self.pingSucc, args=()).start()
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
        if connectionType == "Unirme":
            print(f'Connection with: {address[0]} : {address[1]}')
            print('Join network request recevied')
            self.joinNode(connection, address, datos)
            #print("conection {0}",address)
        elif connectionType == "Sucesor":
            datos = self.mySucc()
            connection.sendall(pickle.dumps(datos))
        elif connectionType == "Predecesor":
            datos = self.myPred()
            connection.sendall(pickle.dumps(datos))
        elif connectionType == "GetPredecesor":
            recvAddr = self.closest_preceding_finger(datos[1])
            #self.printFingerTable()
            connection.sendall(pickle.dumps(recvAddr))
        elif connectionType == "ActualizaPredecesor":
            connection.sendall(pickle.dumps([self.pred,self.predID]))
            #print("Actualiza {0}",address)
            self.pred = datos[2]
            self.predID = datos[1]
        elif connectionType == "ActualizaSucesor":
            self.succ = datos[2]
            self.succID = datos[1]
            self.updateFingerTable()
        elif connectionType == "Ping":
            connection.sendall(pickle.dumps(["OK"]))

        #elif connectionType == 2:
        #    if datos[1] == 0:
        #        connection.sendall(pickle.dumps(self.pred))
        #    else:
        #        connection.sendall(pickle.dumps([2,1,self.succ]))
        #elif connectionType == 3:
        #    self.SearchID(connection, address, datos)
        #elif connectionType == 4:
        #    if datos[1] == 1:
        #        self.updateSucc(datos)
        #    else:
        #        self.updatePred(datos)
        elif connectionType == 5:
            self.updateFingerTable()  
        else:
            print('Problem with connection type')

    def sendJoinRequest(self, ip, port):
        try:
            
            peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peerSocket.connect((ip,port))
            # 0 para que sepa que me quiero unir y le mando mi ip,puerto y mi servicio
            datos = ["Unirme", self.address,self.servicio]
            peerSocket.sendall(pickle.dumps(datos))
            #recibo en datos quien es mi sucesor
            datos = pickle.loads(peerSocket.recv(BUFFER)) 
            peerSocket.close()                      
            self.succ = datos[0]
            self.succID = datos[1]            
            #actualizo mi sucesor
            datos = ["ActualizaPredecesor",self.id,self.address]
            pSocket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #me conecto al mi sucesor y le digo que se actualice conmigo
            pSocket2.connect(self.succ)
            pSocket2.sendall(pickle.dumps(datos))
            #recibo mi predecesor
            datos = pickle.loads(pSocket2.recv(BUFFER))
            pSocket2.close()
            self.pred = datos[0]
            self.predID = datos[1]
            #me conecto a mi predecesor y le digo que se actualice conmigo
            datos = ["ActualizaSucesor",self.id,self.address]
            pSocket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            pSocket3.connect(self.pred)
            pSocket3.sendall(pickle.dumps(datos))
            pSocket3.close()
            time.sleep(0.1)
            self.updateFingerTable()
            self.printFingerTable()
            self.updateOtherFingerTables(self.id)
                        
        except socket.error:
            print('Socket error. Recheck IP/Port.')

    def joinNode(self, connection, address, datos):
        '''
        Recibe la request del nodo nuevo
        '''
        if datos:
            #recibo la direccion del nodo
            peerAddr = datos[1]

            #recibo el servicio del nodo
            peerServ = datos[2]
            peerID = getHashId(peerAddr,peerServ)
            #print("llego "+str(peerID))
            recvAddr = self.getSuccessor(peerID)
            print("join {0}",recvAddr)
            #le mando a su sucesor para que se conecte
            connection.sendall(pickle.dumps(recvAddr))             
            
    def getSuccessor(self, keyID):
        n = self.getPredecessor(keyID)
        try:
            peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #print("SUcesor {0}", n)
            peerSocket.connect(n[0])
            datos = ["Sucesor"]
            peerSocket.sendall(pickle.dumps(datos))
            #(direccion, id del sucesor)
            n = pickle.loads(peerSocket.recv(BUFFER))
            peerSocket.close()
        except socket.error:
            print('Connection denied while getting Successor')
        return n

    def updateFingerTable(self):
        for i in range(MAX_BITS):
            entryId = (self.id + (2 ** i)) % MAX_NODES
            if self.succ == self.address:
                self.fingerTable[entryId] = (self.address,self.id)
                continue
            recvAddr = self.getSuccessor(entryId)
            self.fingerTable[entryId] = (recvAddr[0], recvAddr[1])

    def updateOtherFingerTables(self,id):
        for i in range(1,21):
            p = self.getPredecessor(id-2**(i-1)%MAX_NODES)
            peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peerSocket.connect(p[0])
            peerSocket.sendall(pickle.dumps([5]))
            peerSocket.close()
 
    def getPredecessor(self,id):
        address = [self.address, self.id] 
        #caso base para 1 nodo
        
        if self.succID == self.id:
            address = [self.address,self.id]
        #caso base para 2 nodos
        elif self.succID == self.predID:
            if self.id > self.succID:
                if self.succID < id and id < self.id:
                    address = [self.succ,self.succID]
                else:
                    address = [self.address,self.id]
            else:
                if self.id<id and id<self.succID:
                    address = [self.address,self.id]
                else:
                    address = [self.succ,self.succID]
        #mas de 2
        else: 
            if id == self.id:
                address = [self.pred, self.predID]
            elif self.id < id and id < self.succID :
                address = [self.address,self.id]
            #address = self.closest_preceding_finger(id)        
            elif self.id > self.succID and id < self.succID :
                address = [self.address,self.id]
            elif self.id > self.succID and id > self.id:
                address = [self.address, self.id]
            elif self.id > self.succID and id >self.succID:
                tempaddr = self.address
                tempid = self.id
                peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peerSocket.connect(self.succ)
                peerSocket.sendall(pickle.dumps(["GetPredecesor",id]))
                address = pickle.loads(peerSocket.recv(BUFFER))
                peerSocket.close()
                peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peerSocket.connect(address[0])
                peerSocket.sendall(pickle.dumps(["Sucesor"]))
                succesor = pickle.loads(peerSocket.recv(BUFFER))
                peerSocket.close()
                while (address[1]<id and id > succesor[1]):
                    address = self.closest_preceding_finger(id)
                    if tempid == address[1]:
                        return [tempaddr,tempid]
                    tempid = address[1]
                    tempaddr = address[0]
                    peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    peerSocket.connect(address[0])
                    peerSocket.sendall(pickle.dumps(["GetPredecesor",id]))
                    address = pickle.loads(peerSocket.recv(BUFFER))
                    peerSocket.close()
                    peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    peerSocket.connect(address[0])
                    peerSocket.sendall(pickle.dumps(["Sucesor"]))
                    succesor = pickle.loads(peerSocket.recv(BUFFER))
                    peerSocket.close()
                    print("1",address[1], succesor[1], id)
                    if address[1]<id and id < succesor[1]:
                        return address
                    #elif address[1]
            elif self.id < self.succID and id<self.id:
                tempaddr = self.address
                tempid = self.id
                address = self.mas_pequeno_menor()                
                peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peerSocket.connect(address[0])
                peerSocket.sendall(pickle.dumps(["Sucesor"]))
                succesor = pickle.loads(peerSocket.recv(BUFFER))
                peerSocket.close()
                while (address[1]<id and id > succesor[1]):                    
                    address = self.closest_preceding_finger(id)
                    if tempid == address[1]:
                        return [tempaddr,tempid]
                    tempid = address[1]
                    tempaddr = address[0]
                    peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    peerSocket.connect(address[0])
                    peerSocket.sendall(pickle.dumps(["GetPredecesor",id]))
                    address = pickle.loads(peerSocket.recv(BUFFER))
                    peerSocket.close()
                    peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    peerSocket.connect(address[0])
                    peerSocket.sendall(pickle.dumps(["Sucesor"]))
                    succesor = pickle.loads(peerSocket.recv(BUFFER))
                    print("2",address[1], succesor[1],id)
                    peerSocket.close()
                    if address[1]<id and id < succesor[1]:
                        return address
                #peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                #peerSocket.connect(address[0])
                #peerSocket.sendall(pickle.dumps(["GetPredecesor",id]))                
                #address = pickle.loads(peerSocket.recv(BUFFER))
                #print("addres que devuelve al nodo que llame ",address)
                #peerSocket.close()
            elif self.id < self.succID: #and id > self.id:
                print("self id y succesor id y id ",self.id, self.succID , id)
                tempaddr = self.address
                tempid = self.id
                address = self.closest_preceding_finger(id)
                peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peerSocket.connect(address[0])
                peerSocket.sendall(pickle.dumps(["Sucesor"]))
                succesor = pickle.loads(peerSocket.recv(BUFFER))
                peerSocket.close()
                while (address[1]<id and id > succesor[1]):
                    address = self.closest_preceding_finger(id)
                    if tempid == address[1]:
                        return [tempaddr,tempid]
                    tempid = address[1]
                    tempaddr = address[0]
                    peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    peerSocket.connect(address[0])
                    peerSocket.sendall(pickle.dumps(["GetPredecesor",id]))
                    address = pickle.loads(peerSocket.recv(BUFFER))                    
                    peerSocket.close()
                    peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    peerSocket.connect(address[0])
                    peerSocket.sendall(pickle.dumps(["Sucesor"]))
                    succesor = pickle.loads(peerSocket.recv(BUFFER))
                    peerSocket.close()
                    print("3",address[1], succesor[1],id)
                    #self.printFingerTable()
                    if address[1]<id and id < succesor[1]:
                        return address



                #if address[1] == self.id:
                #    return address
                #print("address que devuelve el closest ",address)
                #peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                #peerSocket.connect(address[0])
                #peerSocket.sendall(pickle.dumps(["GetPredecesor",id]))                
                #address = pickle.loads(peerSocket.recv(BUFFER))
                #print("addres que devuelve al nodo que llame ",address)
                #peerSocket.close()
            #elif self.id < self.succID:# and id < self.id:
                #[sucesor, id , yo]
                #if self.succID<id and id < self.id:
                #    peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                #    peerSocket.connect(self.succ)
                #    peerSocket.sendall(pickle.dumps(["GetPredecesor",id]))
                #    address = pickle.loads(peerSocket.recv(BUFFER))
                #    peerSocket.close()
                ## ] sucesor , yo [
            #else: 
            #        address = [self.address,self.id]
            #else:
            #    if self.id < id and id < self.succID:
            #        address = [self.address,self.id]                    
            #    else: 
#
            #        peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #        peerSocket.connect(self.succ)
            #        peerSocket.sendall(pickle.dumps(["GetPredecesor",id]))
            #        address = pickle.loads(peerSocket.recv(BUFFER))
            #        peerSocket.close()
            ##elif ( self.id<id and id < self.succID):
            ##    address = [self.address,self.id]
            ###elif (id>self.id and id>self.succID and self.predID == self.succID):
            ###    pass
            ##else:
            ##    recvaddress = self.closest_preceding_finger(id)
            ##    newaddr = recvaddress[0]
            ##    print("new addres ",newaddr)
            ##    print("recv addres ",recvaddress)
            ##    peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)              
##
            ##    print("nueva {0}",newaddr,recvaddress[1])
            ##    peerSocket.connect(newaddr)
            ##    peerSocket.sendall(pickle.dumps(["GetPredecesor",id]))
            ##    address = pickle.loads(peerSocket.recv(BUFFER))
            ##    peerSocket.close()
            ##    print("loop")
        return address
    
    def closest_preceding_finger(self,id):
        for key,value in reversed(self.fingerTable.items()):
            if self.id < key and key < id:
                return value 
        return [self.address,self.id]

    def mas_pequeno_menor(self):
        keyid = [self.address,self.id]
        for key,value in self.fingerTable.items():
            if self.id > key:                
                return value
            keyid = value            
        return keyid

    ##    # Caso 0: si soy yo
    ##    if self.id == keyID:
    ##        datos = [0, self.address]
    ##    # Caso 1: si nada mas existe 1 nodo
    ##    elif self.succID == self.id:
    ##        datos = [0, self.address]
    ##    # Caso 2: si mi id es mayor que el keyID, preguntar al antecesor
    ##    elif self.id > keyID:
    ##        if self.predID < keyID:
    ##            datos = [0, self.address]
    ##        elif self.predID > self.id:
    ##            datos = [0, self.address]
    ##        else:
    ##            datos = [1, self.pred]
    ##    # Case 3: si mi id es menor que el keyID, usar la fingertable para buscar al mas cercano
    ##    else:
    ##        if self.id > self.succID:
    ##            datos = [0, self.succ]
    ##        else:
    ##            value = ()
    ##            for key, value in self.fingerTable.items():
    ##                if key >= keyID:
    ##                    break
    ##            value = self.succ
    ##            datos = [1, value]
    ##    connection.sendall(pickle.dumps(datos))

    def mySucc(self):
        return [self.succ,self.succID]

    def myPred(self):
        return [self.pred,self.predID]

    def SearchID(self, ID):
        keyID = ID
        result = self.id
        address = self.address
        for key,value in self.fingerTable.items():
            if key > result and key < keyID:
                result = key
                address = value
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
                pSocket.sendall(pickle.dumps(["Ping"]))
                #print("envie ping")
                recvPred = pickle.loads(pSocket.recv(BUFFER))
                pSocket.close()
                
            except:
                print('\nNode offline detected \nStabilizing...')
                
                if not self.succ == self.pred:
                    # Search for the next succ
                    recvAdd = self.getSuccessor(self.succID+1)
                    self.succ = recvAdd[0]
                    temp = self.succID
                    self.succID = recvAdd[1]
                    # Informa al nuevo sucesor para que actualice su predecesor conmigo
                    pSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    datos = ["ActualizaPredecesor",self.id]
                    pSocket.connect(self.succ)
                    pSocket.sendall(pickle.dumps(datos))
                    datos = pickle.loads(pSocket.recv(BUFFER))
                    pSocket.close()
                    time.sleep(0.1)
                    self.updateFingerTable()
                    self.updateOtherFingerTables(temp)
                else:
                    self.pred = self.address
                    self.predID = self.id
                    self.succ = self.address
                    self.succID = self.id
   

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print('Arguments not supplied (defaults used)')
    else:
        IP = sys.argv[1]
        PORT = int(sys.argv[2])

    node = Node(IP, PORT)
    print(f'My ID is: {node.id}')
    node.Cliente()