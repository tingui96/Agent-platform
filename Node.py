from genericpath import exists
from multiprocessing import connection
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
from unittest import result
from Agent import *

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
        self.succList = [(self.address,self.id)]

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
            self.succList = [(self.address, self.id)]
            self.agent = Agent(self.address, self.id, self.servicio)
            self.escuchar()
            self.updateFingerTable()
            self.start()
        elif userChoice == '1':
            self.servicio = input("Que servicio desea buscar")
            self.BuscarServicioCliente()
            self.MenuCliente()
        
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
        elif userChoice == '6':
            self.sendJoinRequest("127.0.0.1",8080)
        elif userChoice == '7':
            self.sendJoinRequest("127.0.0.1",8000)
        elif userChoice == '5':
            servicio = input("Servicio a buscar: ")
            self.BuscarServicio(servicio)


    def requestExecPred(self, address):
        datos = ["Predecesor"]
        peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peerSocket.connect((address))
        peerSocket.sendall(pickle.dumps(datos))
        datos = pickle.loads(peerSocket.recv(BUFFER)) 
        peerSocket.close()
        print(datos)
        return(datos)    


    def requestExec(self, address, arg):
        datos = ["ExecAgent", arg]
        peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peerSocket.connect((address))
        peerSocket.sendall(pickle.dumps(datos))
        datos = pickle.loads(peerSocket.recv(BUFFER)) 
        peerSocket.close()
        return(datos)    


    
    def printFingerTable(self):
        print('Printing Finger Table')
        for key, value in self.fingerTable.items(): 
            print(f'KeyID: {key}, Value: {value}')

    
    
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
            recvAddr = self.getPredecessor(datos[1])
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
        elif connectionType == "RequestSuccList":
            connection.sendall(pickle.dumps(self.succList))
        elif connectionType == "ExecAgent":
            sendRes = self.agent.Exectute(datos[1])
            connection.sendall(pickle.dumps(sendRes))
        elif connectionType == "RequestAgent":
            time.sleep(0.2)
            print(f"Llega la petición")
            connection.close()
            pSocket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            pSocket3.connect(datos[1])
            datos1 = ["RecibirAgente"]
            pSocket3.sendall(pickle.dumps(datos1))
            self.agent.SendAgent(pSocket3)
        elif connectionType == "RecibirAgente":
            time.sleep(0.2)
            print("Comienza a recibir")
            self.agent.RecibirAgente(connection)
            print("Se recibio el agente con éxito")
        elif connectionType == 2:
            if datos[1] == 0:
                connection.sendall(pickle.dumps(self.pred))
            else:
                connection.sendall(pickle.dumps([2,1,self.succ]))
        elif connectionType == "Buscar":
            recvAddr = self.getPredecessor(datos[1])
            connection.sendall(pickle.dumps(recvAddr))            
            pass
        elif connectionType == 3:
            self.SearchID(connection, address, datos)
        elif connectionType == 4:
            if datos[1] == 1:
                self.updateSucc(datos)
            else:
                self.updatePred(datos)
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
            #actualizo mi sucesor
            self.succ = datos[0]
            self.succID = datos[1]
            #le pido el succList
            peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peerSocket.connect((ip,port))
            datos = ["RequestSuccList"]
            peerSocket.sendall(pickle.dumps(datos))
            recvData = pickle.loads(peerSocket.recv(BUFFER))
            peerSocket.close()
            self.succList = [(self.succ, self.succID)]
            for succ in recvData:
                if not (succ in self.succList):
                    self.succList.append(succ)
                    if len(self.succList) == 20: break
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
            self.updateFingerTable()
            self.updateOtherFingerTables(self.id)
            time.sleep(0.5)
            print("Comienza el envio")
            serv = getHash(self.servicio)
            if(int(self.succID/1000) != serv and int(self.predID/1000) != serv):
                print("Se ha creado un archivo en la carpeta Agent del proyecto, por favor llene los campos correspondientes")
                self.CrearAgente()
            elif (int(self.succID/1000) == serv):
                self.RequestAgent(self.succ)
            else:
                self.RequestAgent(self.pred)
        except socket.error:
            print('Socket error. Recheck IP/Port.')


    def CrearAgente(self):
        pass

    def RequestAgent(self, address):
        print("Pide el agente")
        pSocket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        pSocket3.connect(address)
        datos = ["RequestAgent", self.address]
        pSocket3.sendall(pickle.dumps(datos))


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
            #print("join {0}",recvAddr)
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
        #print("my id {0} mi succesor{1}",self.id,self.succID)
        #print("ID buscado{0}",id)
        #print("{0},{1}".format(self.id, self.succID))
        if (self.id < id and id < self.succID and self.id < self.succID) or (self.succID < self.id and (id < self.succID or id > self.id)) or self.succID == self.id or self.succID == id:       
            return address
        recvaddress = self.closest_preceding_finger(id)
        peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)                
        newaddr = recvaddress[0]
        #print("nueva {0}",newaddr,recvaddress[1])
        peerSocket.connect(newaddr)
        peerSocket.sendall(pickle.dumps(["GetPredecesor",id]))
        address = pickle.loads(peerSocket.recv(BUFFER))
        peerSocket.close()
        return address

    def closest_preceding_finger(self,id):
        for key,value in reversed(self.fingerTable.items()):
            if (self.id < value[1] and value[1] < id) or (id < self.id and (value[1]< id or value[1]>self.id)):
                #print("---{}---".format((key,value)))
                return [value[0],value[1]]

        return [self.address,self.id]


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
    
    #####################################################################################
    ###################################CLIENTE###########################################
    #####################################################################################
    def MenuServicio(self):
        print("Connect to the network\n")    
        ip = input('Enter IP to connect: ')
        port = int(input('Enter port: '))
        self.server = (ip,port)
        self.succList = []

    def MenuCliente(self):
        while 1:
            print("1- Buscar otro Servicio\n2-Brindar algun servicio")
            choice = input()
            if choice == "0":
                self.servicio = input("Que servicio desea buscar:")
                self.ConnectServer()
                self.BuscarServicio(self.servicio)
            else:
                self.servicio = input("Que servicio desea brindar:")
                self.id = self.predID = self.succID = getHashId((self.ip,self.port),self.servicio)
                self.succList = [(self.address, self.id)]
                self.agent = Agent(self.address, self.id, self.servicio)
                self.escuchar()
                self.updateFingerTable()
                self.start()

    def BuscarServicioCliente(self):
        self.MenuServicio()
        self.ConnectServer()
        self.BuscarServicio(self.servicio)

    def ConnectServer(self):
        try:
            peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peerSocket.connect(self.server)
            peerSocket.sendall(pickle.dumps(["Buscar",self.servicio]))
            recvAddr = pickle.loads(peerSocket.recv(BUFFER))
            self.server = recvAddr
            peerSocket.close()
            datos = ["requestSuccList"]
            peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peerSocket.connect(recvAddr)
            peerSocket.sendall(pickle.dumps(datos))
            self.succList = pickle.loads(peerSocket.recv(BUFFER))
            peerSocket.close()
        except:
            if len(self.succList)>0:
                self.server = self.succList.pop(0)
                self.ConnectServer()
            else:
                print("No se encontro el servidor")

    def BuscarServicio(self,servicio):
        searchId = getHashId(self.address, servicio)
        recAddress=self.getSuccessor(searchId)
        serv = getHash(servicio)
        print(recAddress)
        predAddress = self.requestExecPred(recAddress[0])
        print((recAddress[1],predAddress[1], serv))
        if (int(recAddress[1] / 1000) != serv and int(predAddress[1] / 1000) != serv):
            print(f"No se ha encontrado ese servicio en el servidor")
        else: 
            print(f"Se ha encontrado ese servicio\n1-Descripcion\n2-Ejecutar")
            if(int(recAddress[1] / 1000) == serv):
                res = self.requestExec(recAddress[0], input())
            else:
                res = self.requestExec(predAddress[0], input())
            print(res)


   


    
    #########################################################################################


    def pingSucc(self):
        while True:
            # Ping every 5 seconds
            time.sleep(2)
            if self.address == self.succ:
            # If only one node, no need to ping
                continue
            try:
                pSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                #print("abri el socket")
                pSocket.connect(self.succ)
                #print("me conecte")
                pSocket.sendall(pickle.dumps(["RequestSuccList"]))
                #print("envie ping")
                recvData = (pickle.loads(pSocket.recv(BUFFER)))
                pSocket.close()
                self.succList = [(self.succ, self.succID)]
                for succ in recvData:
                    if not (succ in self.succList):
                        self.succList.append(succ)
                        if len(self.succList) == 20: break
            except:
                print('\nNode offline detected \nStabilizing...')
                while(True):
                    try:
                        if not self.succ == self.pred:
                            self.succList.pop(0)
                            self.succ = self.succList[0][0]
                            self.succID = self.succList[0][1]
                            #print(self.succ)
                            # # Search for the next succ
                            # recvAdd = self.getSuccessor(self.succID+1)
                            # self.succ = recvAdd[0]
                            # temp = self.succID
                            # self.succID = recvAdd[1]
                            
                            # Informa al nuevo sucesor para que actualice su predecesor conmigo
                            pSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            datos = ["ActualizaPredecesor",self.id, self.address]
                            pSocket.connect(self.succ)
                            pSocket.sendall(pickle.dumps(datos))
                            datos = pickle.loads(pSocket.recv(BUFFER))
                            pSocket.close()
                            time.sleep(0.1)
                            self.updateFingerTable()
                            self.updateOtherFingerTables(self.succID)
                            break
                        else:
                            self.pred = self.address
                            self.predID = self.id
                            self.succ = self.address
                            self.succID = self.id
                            break
                    except socket.error:
                            print("error aqui por alguna razon")

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print('Arguments not supplied (defaults used)')
    else:
        IP = sys.argv[1]
        PORT = int(sys.argv[2])

    node = Node(IP, PORT)
    print(f'My ID is: {node.id}')
    node.Cliente()