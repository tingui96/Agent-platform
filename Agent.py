import subprocess
import sys
from tools import *
import pickle
import socket
import os
import time
import Node2

class Agent():
    def __init__(self, address,id,service):
        self.ip,self.port = address
        self.id = id
        self.service = service

    def Negotiation(self, agent, node = Node2.Node):
        pass


    def SubcribeRequest(self, agent, node):
        pass



    def Exectute(self, argv):
        s2_out = (subprocess.check_output([sys.executable, f"./Agent/{self.service}.py", argv])).decode("utf_8")
        return(s2_out)

    def SendAgent(self, connection):
        time.sleep(0.2)
        print("Empezamos a enviar")
        file_open=0
        try:
            file = open( f"./Agent/{self.service}.py","rb")
            file_open=1
            fileData = file.read(BUFFER)
            while fileData:                
                connection.send(fileData)
                fileData = file.read(BUFFER)
            return ("El agente fue enviado")
        except:
            if file_open:
                file.close()
            return ("El agente no se pudo enviar")


    def RecibirAgente(self,connection):
        try:
            file = open( f"./Agent/{self.service}.py","wb")
            while True:                
                fileData = connection.recv(BUFFER)
                if fileData:                    
                    if isinstance(fileData, bytes):
                        end = fileData[0] == 1
                    else:
                        end = fileData == chr(1)
                    if not end:
                        file.write(fileData)
                    else:
                        break
                else:
                    break
            file.close()
            connection.close()
            print("All written on file")
        except ConnectionResetError:
            print('Interrupted data transfer')
            print('Waiting for the system to stabilize')
            print('Try again in 10 seconds')
            os.remove(self.service)
            
    