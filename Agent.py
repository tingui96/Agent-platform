
class Agent():
    def __init__(self, address, id, servicio): #data
        self.address = address
        self.id = id
        self.service = servicio
        #self.password = data.password
        #self.description = data.description
    
    def Execute(self):
        print("El agente se ejecuta")