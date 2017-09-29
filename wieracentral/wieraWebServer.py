import wieraWebUserInterface

if __name__ == '__main__':
	web_server = wieraWebUserInterface.WieraWebUserInterface(0, 8080)
	web_server.run_forever()

