import hashlib

# Default values if command line arguments not given
IP = '127.0.0.1'
PORT = 8080
BUFFER = 4096
MAX_BITS = 20
MAX_NODES = 2**MAX_BITS

def getHash(key):
    '''
    Takes key string, uses SHA-1 hashing and returns 
    a 10-bit (1024) compressed integer.
    '''
    result = hashlib.sha1(key.encode())
    return int(result.hexdigest(), 16) % 1000

def getHashId(address,servicio):
    return getHash(f'{servicio}')%1000*1000+getHash(f'{address[0]}:{str(address[1])}')