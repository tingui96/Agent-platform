import sys

def Description():
    return " Jugo 100% natural "

def Execute():
    return " Jugo "    


if __name__ == '__main__':
    arg = (sys.argv[1])
    if arg == "2":
        print(Execute()) 
    elif arg == "1":
        print(Description())
    else:
        pass