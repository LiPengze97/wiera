import commands
import threading
import socket
import time
import sys
import json
import select

sys.path.append('./gen-py')

from WieraTieraServerIface import *
from WieraTieraServerIface.ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from TieraServerWieraIface import * 
from TieraServerWieraIface.ttypes import *

class DCsMonitor:
	#this info will come from Tiera Server
	#this moudle will agreegate information which will be used for PGA
	#this will be updsed by Tiera Server
	update_thread = None

	def __init__(self, tiera_server_manager):
		self.tiera_server_manager = tiera_server_manager
		
		self.update_thread = threading.Thread(target=self._update, args=[5, ])
		self.update_thread.daemon = True
		self.update_thread.start()

	def _update(self, ping_interval):
		while True:
			server_list = self.tiera_server_manager.get_tiera_server_list()
			
			for server_info in server_list:
				hostname = server_info[0]
				tiera_server_client, port = self.tiera_server_manager.get_tiera_server_client(hostname, 0)

#				print tiera_server_client

				try:
					if tiera_server_client != None:
						piggy_back_info = tiera_server_client.ping()
						req_json = json.loads(piggy_back_info)
	
#						print req_json
						#update info
						self.tiera_server_manager.update_server_info(hostname, port, req_json)
				except Exception, e:
					print 'TSM: Ping to Tiera Server ' + hostname + ':' + str(port) + ' has been failed.'
					self.remove_tiera_server(hostname, port)

			time.sleep(ping_interval)
			outdated_server_list = self.tiera_server_manager.check_latest_updated_time()
		
#			if len(outdated_server_list) > 0:
#				print 'TSM: These servers are outdated :' + str(outdated_server_list)

class WieraTieraServerHandler:
	def __init__(self, tiera_server_manager):
		self.tiera_server_manager = tiera_server_manager

	def registerTieraServer(self, server_info, callback_ip):
		tiera_info = json.loads(server_info)

		result = {}

		try:
			self.tiera_server_manager.lock.acquire()

			if tiera_info != None:
				hostname = tiera_info['hostname']

				if 'ip' not in tiera_info:
					ip = callback_ip.strip('::ffff:')
					print ip + ' from callback_ip from thrift'
				else:
					ip = tiera_info['ip']
				
				port = tiera_info['tiera_server_port']
				self.tiera_server_manager.add_tiera_server(hostname, ip, port)
				result['result'] = True
				result['value'] = 'Add Tiera info into the list successfully'

				print '[TSM] '+ hostname + '(' + ip + ':' + str(port) +') is registered.'
			else:
				result['result'] = False
				result['value'] = 'Failed to load request to json'
		finally:
			self.tiera_server_manager.lock.release()

		
		return json.dumps(result)
	
class TieraServerManager:
	def __init__(self, port, ping_interval):
		self.server_list = {}
		self.wiera_server_manager_port = port
		self.lock = threading.Lock()

		#run server for Tiera Server
#	self.wiera_tiera_server = threading.Thread(target=self._run_tiera_server, args=([port,]))
#	self.wiera_tiera_server.daemon = True
#	self.wiera_tiera_server.start()
		self.ping_interval = ping_interval

		#set ping thread
#		self.DCsMonitor = DCsMonitor(self)

	def add_tiera_server(self, hostname, ip, port):
		if hostname not in self.server_list:
			self.server_list[hostname] = {}
			self.server_list[hostname]['ip'] = ip
			self.server_list[hostname]['ports'] = {}

		if port in self.server_list[hostname]['ports']:
			self.server_list[hostname]['ports'][port].close()

		transport = TSocket.TSocket(ip, port)
		transport = TTransport.TFramedTransport(transport)
  		protocol = TBinaryProtocol.TBinaryProtocol(transport)
		client = TieraServerWieraIface.Client(protocol)

		# Connect!
		transport.open()
			
		self.server_list[hostname]['ports'][port] = {}
		self.server_list[hostname]['ports'][port]['update_time'] = time.time()
		self.server_list[hostname]['ports'][port]['thrift_client'] = client

		self.server_list[hostname]['ports'][port]['aggregated'] = {}
#		self.server_list[hostname]['ports'][port]['aggregated']['latency'] = {}
#		self.server_list[hostname]['ports'][port]['aggregated']['bandwidth'] = {}

		#will store get and put history and latency for each request
#		self.server_list[hostname]['ports'][port]['aggregated']['workload'] = {}

	def get_tiera_server_client(self, hostname, port):
		if hostname in self.server_list:
			if port in self.server_list[hostname]['ports']:
				return (self.server_list[hostname]['ports'][port]['thrift_client'], port)
			else:
				for port in self.server_list[hostname]['ports']:
					return (self.server_list[hostname]['ports'][port]['thrift_client'], port)
		return None
			
	def run_forever(self):
		# set handler to our implementation
		handler = WieraTieraServerHandler(self)

		processor = WieraTieraServerIface.Processor(handler)
		transport = TSocket.TServerSocket(port=self.wiera_server_manager_port)
		tfactory = TTransport.TFramedTransportFactory()
		pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        # set server
		server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory, daemon=True)

		#set socket thread 20 min
		server.setNumThreads(32)

		print '[TSM] Tiera Server Manager is ready for Tiera Server port:' + str(self.wiera_server_manager_port)
		server.serve()

	def check_latest_updated_time(self):
		outdated_server = []

		for hostname in self.server_list:
			for port in self.server_list[hostname]['ports']:
				latest = self.server_list[hostname]['ports'][port]['update_time']
				elapse = time.time() - latest

				if elapse > self.ping_interval+1:
					outdated_server.append((hostname, port))

		return outdated_server

	def update_server_info(self, hostname, port, server_info):
		self.server_list[hostname]['ports'][port]['aggregated'] = server_info
		self.server_list[hostname]['ports'][port]['update_time'] = time.time()

	def find_info_by_hostname(self, hostname):
		if hostname in self.server_list:
			ip = self.server_list[hostname]['ip']

			for port in self.server_list[hostname]['ports']:
				return ip, port
		return None, None

	def remove_tiera_server(self, hostname, port):
		if port in self.server_list[hostname]['ports']:
			del self.server_list[hostname]['ports'][port]

	def get_tiera_server_list(self):
		#return as a list
		server_list = []

		for hostname in self.server_list:
			ip = self.server_list[hostname]['ip']
			server_info = (hostname, ip)
			server_list.append(server_info)

		return server_list

	def get_tiera_server(self, hostname):
		if hostname not in self.server_list:
			return None

		return self.server_list[hostname]
