import sys
import BaseHTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler

class WieraHandler(BaseHTTPServer.BaseHTTPRequestHandler):
	def do_HEAD(s):
		s.send_response(200)
		s.send_header("Content-type", "text/html")
		s.end_headers()
	def do_GET(s):
		"""Respond to a GET request."""
		s.send_response(200)

		if s.path == 'favicon.ico':
			s.send_header('Content-type', 'image/ico')
			read_type = 'rb'
		else:
			s.send_header('Content-type', 'text/html')
			read_type = 'r'

		s.end_headers()


		if s.path == '/':
			request_path = 'html/index.html'
		else:
			request_path = 'html' + s.path

		request_path = request_path.rstrip('/')
			
		print request_path
	
		try:
			with open (request_path, read_type) as wiera_map:
				data = wiera_map.read()
	
			s.wfile.write(data)
		except:
			print "Unexpected error:", sys.exc_info()[0]
			raise
#		s.wfile.write("<html><head><title>Title goes here.</title></head>")
#		s.wfile.write("<body><p>This is a test.</p>")
		# If someone went to "http://something.somewhere.net/foo/bar/",
		# then s.path equals "/foo/bar/".
#		s.wfile.write("<p>You accessed path: %s</p>" % s.path)
#		s.wfile.write("</body></html>")

class WieraWebUserInterface:
	def __init__ (self, wiera, port):
		self.wiera = wiera
		self.port = port

	def run_forever(self):
		server_address = ('0.0.0.0', self.port)
		protocol = "HTTP/1.1"

		HandlerClass = WieraHandler
		ServerClass  = BaseHTTPServer.HTTPServer

		HandlerClass.protocol_version = protocol
		httpd = ServerClass(server_address, HandlerClass)

		sa = httpd.socket.getsockname()
		print "Wiera Web User Interface is running on", sa[0], "port", sa[1]
		httpd.serve_forever()
