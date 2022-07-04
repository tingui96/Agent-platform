import sys
import time 

def Description():
    return "Turquino del bueno"

def Execute():
    time.sleep(20)
    return "Cafesito "    


if __name__ == '__main__':
    arg = (sys.argv[1])
    if arg == "2":
        Execute() 
    elif arg == "1":
        Description()
    else:
        pass