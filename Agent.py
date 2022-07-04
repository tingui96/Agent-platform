import queue
import subprocess
import sys

from tools import *
import pickle
import socket
import os
import time
import threading

class Agent():
    def __init__(self, address,id,service):
        self.ip,self.port = address
        self.id = id
        self.service = service
        self.state = True
        self.queue = []
        self.startQueueThread()

    def Exectute(self, argv, connection):
        if self.state:
            self.state = False
            result = (subprocess.check_output([sys.executable, f"./Agent/{self.service}.py", argv])).decode("utf_8")
            self.state = True
            connection.sendall(pickle.dumps(result))

        else:
            self.queue.append([argv, connection])
            return

    def startQueueThread(self):
        threading.Thread(target=self.CheckQueue, args=()).start()


    def CheckQueue(self):
        while(True):
            if len(self.queue) and self.state:
                elem = self.queue.pop()
                self.Exectute(elem[0], elem[1])



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
            print("Se recibió con éxito")
        except ConnectionResetError:
            print('Transferencia interrumpida')
            print('Esperando porque el sistema se estabilice')
            print('Intentar de nuevo en 10 segundos')
            os.remove(self.service)
            
    