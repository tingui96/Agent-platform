import struct
from xmlrpc.client import Boolean
from Agent import *
import argparse



def main():
    agents = []
    categories = []
    while(True):
        print("Ingrese Agente")
        addAgent(agents)
        if len(agents) == 3:
            break
    for agent in agents:
        print(agent.id)
    

def addAgent(agents = list):
    inp = input().split(" ")
    data = struct
    data.address = (inp[0], inp[1])
    data.id = inp[2]
    data.admin = False
    agent = Agent(data)
    agents.append(agent)

def updAgentStatus(agent = Agent, status = bool):
    agent.admin = status
    pass

def searchAgent():
    pass


if __name__ == '__main__':
    main()