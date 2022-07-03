from genericpath import exists
from multiprocessing import connection
from operator import mod
import os
import queue
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
import streamlit as st
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
            st.error('Socket not opened')

    def menu(self):  
        option = st.selectbox('', ('What would you like to do', 'Provide Service', 'Search Service'))
        if option == 'Provide Service':
            return 0
        elif option == 'Search Service':
            return 1    

    def Cliente(self):
        userChoice = self.menu()
        if userChoice == 0:
            self.servicio = st.text_input("What service do you want to provide?: ", '')
            self.id = self.predID = self.succID = getHashId((self.ip,self.port),self.servicio)
            self.succList = [(self.address, self.id)]
            self.agent = Agent(self.address, self.id, self.servicio)
            self.escuchar()
            self.updateFingerTable()
            self.start()
        elif userChoice == 1:
            self.servicio = st.text_input("What service do you want to search for?: ")
            self.BuscarServicioCliente()
            self.MenuCliente()
        
    def agente(self):
        option = st.selectbox('', ('What would you like to do', 'Connect to the network', 'Search Service'))
        if option == 'Connect to the network':
            ip = st.text_input('Enter IP to connect: ')
            port = st.text_input('Enter port: ')
            self.sendJoinRequest(ip, int(port))
        elif option == 'Search Service':
            servicio = st.text_input('What service do you want to search for?', '')
            self.BuscarServicio(servicio)

    def requestPred(self, address):
        datos = ["Predecesor"]
        peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peerSocket.connect((address))
        peerSocket.sendall(pickle.dumps(datos))
        datos = pickle.loads(peerSocket.recv(BUFFER)) 
        peerSocket.close()
        st.write(datos)
        return(datos)    

    def requestExec(self, address, arg):
        datos = ["ExecAgent", arg]
        peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peerSocket.connect((address))
        peerSocket.sendall(pickle.dumps(datos))
        st.write("Request execute")
        time.sleep(0.2)
        datos = pickle.loads(peerSocket.recv(BUFFER)) 
        st.write("I received the results")
        peerSocket.close()
        return(datos)    

    def printFingerTable(self):
        #print('Printing Finger Table')
        #for key, value in self.fingerTable.items(): 
        #    print(f'KeyID: {key}, Value: {value}')
        st.write(self.fingerTable)
        
    def start(self):
        '''
        Accepting connections from other threads.
        '''
        threading.Thread(target=self.listenThread, args=()).start()
        threading.Thread(target=self.pingSucc, args=()).start()
        # In case of connecting to other clients
        while True:
            st.write('Listening to other clients')
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
            st.write(f'Connection with: {address[0]} : {address[1]}')
            st.write('Join network request recevied')
            self.joinNode(connection, address, datos)
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
            self.agent.Exectute(datos[1],connection)
        elif connectionType == "RequestAgentState":
            st.write("They asked about my status")
            state = self.agent.state
            queue = len(self.agent.queue)
            connection.sendall(pickle.dumps([state, self.succ, self.succID, queue]))
        elif connectionType == "RequestAgent":
            time.sleep(0.2)
            st.write("The request arrives")
            connection.close()
            pSocket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            pSocket3.connect(datos[1])
            datos1 = ["RecibirAgente"]
            pSocket3.sendall(pickle.dumps(datos1))
            self.agent.SendAgent(pSocket3)
        elif connectionType == "RecibirAgente":
            time.sleep(0.2)
            st.write("Start receiving")
            time.sleep(0.5)
            st.write(self.agent.RecibirAgente(connection))
            time.sleep(0.5)
            st.write("The agent was received successfully")
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
            st.error('Problem with connection type {}'.format(connectionType))

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
            st.write("Shipping begins")
            serv = getHash(self.servicio)
            if(int(self.succID/1000) != serv and int(self.predID/1000) != serv):
                if("{}.py".format(self.servicio) not in os.listdir("./Agent/")):
                    st.write("A file has been created in the Agent folder of the project, please fill in the corresponding fields")
                    self.CrearAgente()
                else:
                    st.write("Good")
            elif (int(self.succID/1000) == serv):
                self.RequestAgent(self.succ)
            else:
                self.RequestAgent(self.pred)
        except socket.error:
            st.error('Socket error. Recheck IP/Port.')

    def CrearAgente(self):
        description = st.text_input('Add the description to the service', '')
        execute = st.text_input('Add code in python', '')
        code = 'import sys'
        code += '\ndef Description():'
        code += '\n\treturn ' + description
        code += '\ndef Execute():'
        code += '\n\treturn eval('+ execute +')'
        code += "\nif __name__ == '__main__':"
        code += '\n\targ = (sys.argv[1])'
        code += '\n\tif arg == "2":'
        code += '\n\t\tExecute()'
        code += '\n\telif arg == "1":'
        code += '\n\t\tDescription()'
        code += '\n\telse:'
        code += '\n\t\tpass'
        with open("./Agent/" + self.servicio + ".py","w") as file:
            file.write(code)

    def RequestAgent(self, address):
        #print("Pide el agente")
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
            st.error('Connection denied while getting Successor')
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
        peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peerSocket.connect(self.pred)
        peerSocket.sendall(pickle.dumps([5]))
        peerSocket.close()
        for i in range(1,21):
            p = self.getPredecessor((id-2**(i-1))%MAX_NODES)
            print(p)
            peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peerSocket.connect(p[0])
            peerSocket.sendall(pickle.dumps([5]))
            peerSocket.close()
 
    def getPredecessor(self,id):
        address = [self.address, self.id] 
        if (self.id < id and id < self.succID and self.id < self.succID) or (self.succID < self.id and (id < self.succID or id > self.id)) or self.succID == self.id or self.succID == id:       
            return address
        recvaddress = self.closest_preceding_finger(id)
        peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)                
        newaddr = recvaddress[0]
        #print("nueva {0} {1} {2}".format(newaddr,recvaddress[1], id))
        peerSocket.connect(newaddr)
        peerSocket.sendall(pickle.dumps(["GetPredecesor",id]))
        address = pickle.loads(peerSocket.recv(BUFFER))
        #print("&&&&&&{}&&&&&&&".format(address))
        peerSocket.close()
        return address

    def closest_preceding_finger(self,id):
        for key,value in reversed(self.fingerTable.items()):
            if (self.id < value[1] and value[1] < id) or (id < self.id and (value[1]< id or value[1]>self.id) or (self.id == id and value[1] != self.id)):
                return [value[0],value[1]]

        return [self.address,self.id]

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
        st.write("Connect to the network\n")    
        ip = st.text_input('Enter IP to connect: ', '')
        port = int(st.text_input('Enter port: ', ''))
        self.server = (ip,port)
        self.succList = []

    def MenuCliente(self):
        while 1:
            choice = -1
            option = st.selectbox('', ('Que desea hacer', 'Brindar Servicio', 'Buscar Servicio'))
            if option == 'Brindar Servicio':
                choice = 1
            elif option == 'Buscar Servicio':
                choice = 0
            if choice == 0:
                self.servicio = st.text_input("What service do you want to search for?: ")
                self.ConnectServer()
                self.GetServicio()
            elif choice == 1:
                self.servicio = st.text_input("What service do you want to provide?:")
                self.id = self.predID = self.succID = getHashId((self.ip,self.port),self.servicio)
                self.succList = [(self.address, self.id)]
                self.agent = Agent(self.address, self.id, self.servicio)
                self.escuchar()
                self.updateFingerTable()
                self.start()

    def BuscarServicioCliente(self):
        self.MenuServicio()
        self.ConnectServer()
        self.GetServicio()

    def ConnectServer(self):
        try:
            peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peerSocket.connect(self.server)
            x = getHash(self.servicio) * 1000 + 999
            peerSocket.sendall(pickle.dumps(["Buscar",x]))
            recvAddr = pickle.loads(peerSocket.recv(BUFFER))
            self.server = recvAddr[0]
            st.write("Received server ", self.server)
            peerSocket.close()
            datos = ["RequestSuccList"]
            peerSocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peerSocket1.connect(self.server)
            peerSocket1.sendall(pickle.dumps(datos))
            self.succList = pickle.loads(peerSocket1.recv(BUFFER))
            #print("recibi lista de server",self.succList)
            peerSocket1.close()
        except:
            if len(self.succList)>0:
                self.server = self.succList.pop(0)[0]
                self.ConnectServer()
            else:
                st.write("Server not found")
                pass

    def GetServicio(self):
        serv = getHash(self.servicio)
        print("get service to server ",self.server)
        try:
            predAddress = self.requestPred(self.server)
        except:
            exit()
        recAddress = getHashId(self.server,self.servicio)
        #print( "predddecesor addres",predAddress)
        #print("recvadress", recAddress)
        print("ServiceID ", serv)
        self.FindBestAgent(serv, [self.server,recAddress], predAddress, 0, self.servicio)

#########################################################################################

    def BuscarServicio(self,servicio):
        searchId = getHashId(self.address, servicio)
        recAddress=self.getSuccessor(searchId)
        serv = getHash(servicio)
        #print(recAddress)
        predAddress = self.requestPred(recAddress[0])
        self.FindBestAgent(serv, recAddress, predAddress, 1, servicio)

##########################################################################################

    def FindBestAgent(self, servicioID, succ, pred, type, servicio):
        #print("servicio:{0}, succ{1}, pred{2}".format(servicioID,succ,pred))
        servToExec = pred
        _serv = succ if int(succ[1]/1000) == servicioID else pred 
        free = False
        _queue = sys.maxsize
        timer = sys.maxsize
        if (int(pred[1]/1000) != servicioID and int(succ[1]/1000) != servicioID):
            st.write(f"That service was not found on the server")
        else:
            try:
                st.write("That service was found")
                inpt = 0
                option = st.selectbox('', ('Que desea hacer', 'Description', 'Execute'))
                if option == 'Description':
                    inpt = 1
                elif option == 'Execute':
                    inpt = 2
                datos = ["RequestAgentState"]
                while(True):
                    #print("*********{}*********".format(_serv))
                    peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    t0 = time.time()
                    peerSocket.connect(_serv[0])
                    peerSocket.sendall(pickle.dumps(datos))
                    state = pickle.loads(peerSocket.recv(BUFFER))
                    t1 = time.time() - t0
                    peerSocket.close()
                    print(state)
                    if (not (state[0] or free) and state[3] < _queue):
                        servToExec = _serv 
                        _queue = state[3]
                    elif (state[0] and t1 < timer):
                        timer = t1 
                        servToExec = _serv
                        free = True
                        _queue = 0
                    _serv = [state[1],state[2]]
                    #print("*********{}*********".format(_serv))
                    #print("############ {} ############".format(int(_serv[1]/1000) != servicioID))
                    if ((int(_serv[1]/1000) != servicioID) or _serv == succ): break
                if free:
                    st.write(servToExec)
                    res = self.requestExec(servToExec[0],inpt)
                    st.write(res)
                else:
                    st.write("All agents performing the service are busy")
                    inputq = 0
                    option = st.selectbox('', ('What would you like to do', 'Enter queue', 'Back to menu'))
                    if option == 'Enter queue':
                        inputq = 1
                    elif option == 'Back to menu':
                        inputq = 2
                    #print(f"Todos los agentes que realizan el servicio se encuentran ocupados, desea:\n1-Entrar en cola\n2-Volver al menu")
                    if (inputq == 1):
                        res = self.requestExec(servToExec[0],inpt)    
                        st.write(res)
            except:
                if (type):
                    time.sleep(2)
                    self.BuscarServicio(servicio)
                else:
                    #print("esperando 1 segundo")
                    time.sleep(2)
                    self.ConnectServer()
                    self.GetServicio()
    
    def requestQueue(self, address):
        datos = ["QueueInAgent", address]
        peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peerSocket.connect((address))
        peerSocket.sendall(pickle.dumps(datos))
        datos = pickle.loads(peerSocket.recv(BUFFER)) 
        peerSocket.close()
        return(datos)  

    def pingSucc(self):
        while True:
            # Ping every 5 seconds
            time.sleep(1)
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
                st.write('Node offline detected')
                st.write('Stabilizing...')
                while(True):
                    try:
                        if not self.succ == self.pred:
                            self.succList.pop(0)
                            self.succ = self.succList[0][0]
                            self.succID = self.succList[0][1]
                            
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
                            st.write("Socket error")

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print('Arguments not supplied (defaults used)')
    else:
        IP = sys.argv[1]
        PORT = int(sys.argv[2])

    node = Node(IP, PORT)
    print(f'My ID is: {node.id}')
    node.Cliente()