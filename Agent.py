funcionalities = []

class Agent(object):
    def __init__(self, data):
        self.ip,self.port = data.address
        self.id = data.id
        self.admin = False
        # self.service = data.serv
        # self.password = data.passw
        # self.description = data.desc
        # self.time = data.time
        # self.paddress = data.paddr



