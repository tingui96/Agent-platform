import sys

def Description():
    return " Descripcion aqui "

def Execute():
    return " 2 + 2 = 4 "    


if __name__ == '__main__':
    arg = (sys.argv[1])
    if arg == "2":
        print(Execute()) 
    elif arg == "1":
        print(Description())
    else:
        pass

    a = int(321567/1000)
    print(a)
    
