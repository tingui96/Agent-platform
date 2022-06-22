
class Agent():
    def __init__(self, data):
        self.ip,self.port = data.address
        self.id = data.id
        self.name = data.name
        self.service = data.service
        self.password = data.password
        self.description = data.description
