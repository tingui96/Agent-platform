from encodings import utf_8
import time
import sys
import subprocess
from tools import *
import ast

class Agent():
    def __init__(self, address, id, servicio): #data
        self.address = address
        self.id = id
        self.service = servicio
        #self.password = data.password
        #self.description = data.description
    
    def Execute(self):
        s2_out = (subprocess.check_output([sys.executable, f"./Agent/{self.service}.py", "34"])).decode("utf_8")
        return(s2_out)

    def SendAgent(self, connection, fileID):
        file_open=0
        try:
            file = open("./Agent/"+str(fileID),"rb")
            file_open=1
            fileData = file.read(BUFFER)
            while fileData:                
                connection.send(fileData)
                fileData = file.read(BUFFER)
        except:
            if file_open:
                file.close()
            connection.close()
            print("The file has not been sent")
            return

    def RecvAgent():
        pass

if __name__ == '__main__':
    agent = Agent("",0, "hola")
    agent.Execute()