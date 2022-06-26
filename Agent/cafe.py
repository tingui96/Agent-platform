import sys

def Description():
    return "Turquino del bueno"

def Execute():
    return "Cafesito "    


if __name__ == '__main__':
    arg = (sys.argv[1])
    if arg == "2":
        print(Execute()) 
    elif arg == "1":
        print(Description())
    else:
        pass