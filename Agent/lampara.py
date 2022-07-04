import sys
def Description():
	return "Aqui se producen lamparas"
def Execute():
	return eval("2+2")
if __name__ == '__main__':
	arg = (sys.argv[1])
	if arg == "2":
		print(Execute())
	elif arg == "1":
		print(Description())
	else:
		pass