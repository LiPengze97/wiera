import commands
import socket
import time
import sys
import json
import threading
import localServerManager
import conf
import policyManager 
import wieraCommon
import wieraWebUserInterface

sys.path.append('./gen-py')

from ApplicationToWieraIface import *
from ApplicationToWieraIface.ttypes import *
  
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from operator import itemgetter

class ApplicationToWieraHandler:
	def __init__(self, wiera):
		self.wiera = wiera
		
	def startWieraInstance(self, req_policy):
		req_policy = req_policy.replace("'", '"')
		policy = json.loads(req_policy)
		return self.wiera.startInstance(policy)

	def stopWieraInstance(self, req_policy):
		req_policy = req_policy.replace("'", '"')
		policy = json.loads(req_policy)
		return self.wiera.stopInstance(policy);

	def getWieraInstance(self, policy_id, client_ip):
		result = self.wiera.getInstance(policy_id, client_ip);
#		need to be sorted based on distance
#		instance_list.sort(self.sort_by_distance)
		return result

	def getLocalServerList(self):
		result = self.wiera.getLocalServerList()
		return result
	
		#need to sort. 

class GlobalPolicyManager:
	def __init__(self):
		print 'to be implemented'

class LocalInstanceMonitor:
	instance_list = []
	def __init__(self):
		print 'to be implemented'
				
class MetaStore:
	def __init__(self):
		print 'to be implemented'

class NetworkMonitor:
	def __init__(self):
		print 'to be implemented'

class WieraServer:
	def __init__(self):
		# Threading HTTP-Server
		self.conf = conf.Conf('wiera.conf')

		local_server_manager_port = self.conf.get('local_server_port')
		local_server_manager_ping_interval = self.conf.get('ping_interval')
		applications_port = self.conf.get('applications_port')

		#LocalServerList #not instance
		self.local_server_manager = localServerManager.LocalServerManager(local_server_manager_port, local_server_manager_ping_interval)

		#web server
#		self.web_server = wieraWebUserInterface.WieraWebUserInterface(self, 8080)

		#policy manager
		self.policy_manager = policyManager.PolicyManager()
		self.request_lock = threading.Lock()

		if self.conf.get('ui_command') == True:
			self.user_input_thread = threading.Thread(target=self._ui_handler)
			self.user_input_thread.daemon = True
			self.user_input_thread.start()

		#web server

		#Thrift server
		self.applications_server = threading.Thread(target=self._run_applications_server, args=([applications_port,]))
		self.applications_server.daemon = True
		self.applications_server.start()

		#web Server
#		web_server_thread = threading.Thread(target=self.web_server.run_forever(), args=())
#		web_server_thread.daemon = True
#		web_server_thread.start() 

	def _run_applications_server(self, server_port):
		# set handler to our implementation
		handler = ApplicationToWieraHandler(self)
  
		processor = ApplicationToWieraIface.Processor(handler)
		transport = TSocket.TServerSocket(port=server_port)
		tfactory = TTransport.TFramedTransportFactory()
		pfactory = TBinaryProtocol.TBinaryProtocolFactory()
  
		# set server
		server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory, daemon=True)
  
		print '[Wiera] Starting applications server port:' + str(server_port)
		server.serve()

	def _ui_handler(self):
		time.sleep(0.5)
		
		while True:
			in_val = raw_input("")
			in_list = in_val.split()
			
			if len(in_list) <= 0:
				continue
			
			cmd = in_list[0].upper()

			if cmd == 'EXIT':
				break
			elif cmd == 'CONF':
				if len(in_list) == 1:
					self.conf.show()
				elif len(in_list) == 2:
					self.conf.show(in_list[1])
				else:
					self.conf.set(in_list[1], in_list[2])
			elif cmd == 'CONSISTENCY' or cmd == 'C':
				if len(in_list) >= 3:
					policy_id = in_list[1]
					data_distribution = int(in_list[2])

					policy = self.policy_manager.get_policy(policy_id)
	
					if policy != None:
						supported = False
						req = {}
						req['type'] = 'data_distribution'
						req['policy_id'] = policy_id
						req['data_distribution'] = data_distribution

						if data_distribution == 0: #MultiMaster
							supported = True
						elif data_distribution == 1: #Primary Backup
							if len(in_list) != 4:
								print "Need to set primary node"
								continue
							supported = True

							if wieraCommon.is_valid_ip(in_list[3]) == False:
								#try to find host name to get ip
								primary_ip = policy.find_ip_by_hostname(in_list[3])

								if primary_ip == None:
									print "Invalid IP format or not existed Hostname"
									continue
							else:
								primary_ip = in_list[3]

							#check primary_ip is existed in policy. 
							if policy.find_host_by_ip(primary_ip) == None:
								print "Failed to find a host with ip: " + primary_ip
								continue
		
							req['primary'] = primary_ip
						elif data_distribution == 2: #casual
							print "Casual is not supported yet"
							continue
						elif data_distribution == 3: #Eventual
							primary_ip = None

							if len(in_list) != 4:
								print "Need to set period"
								continue
							supported = True
							req['period'] = int(in_list[3])
						elif data_distribution > 6:
							print "Not supported yet"

						if supported == True:
							start = time.time()
							failed_list = policy.broadcast(json.dumps(req), "data_distribution")
							if failed_list == None:
								print "Broadcast failed. (timeout)"
							elif len(failed_list) > 0:
								print "Broadcast failed info"
								print "--------------------------------"
								 # 5seconds timeout
								for hostname in failed_list:
									print hostname + " :" + failed_list[hostname]['value']
							else:
								print 'done to change data distribution'
								elapse = time.time() - start
								print str(elapse*1000) + ' ms takes to change data distribution'
					else:
						print "Can not find policy id: " + policy_id
										
			elif cmd == 'POLICY' or cmd == 'P':
				if len(in_list) == 1:
					policy_id = 'test'
				else:
					policy_id = in_list[1]

				policy, req = self.simulate_policy(policy_id)
				policy_server_list = policy.get_local_hostname_list()

				print req

				for hostname in policy_server_list:
					for port in policy_server_list[hostname]['ports']:
						self.local_server_manager.send(hostname, port, req)

	def check_available_host(self, host_info):
		available_hostname_list = {}

		for hostname in host_info:
			ip, port = self.local_server_manager.find_info_by_hostname(hostname)

			if ip != None and port != None:
				available_hostname_list[hostname] = {}
				available_hostname_list[hostname]['ip'] = ip
				available_hostname_list[hostname]['ports'] = port
			else:
				print 'Hostname ' + hostname + ' is not available. IP is not exist'

		return available_hostname_list

	def startInstance(self, policy_spec):
		self.request_lock.acquire()

		try:
			start = time.time()
			result = {}

			policy = self.policy_manager.add_new_policy(self, policy_spec)

			if policy == None:
				result['result'] = False
				result['value'] = 'Failed to create a new policy object'
				print 'failed create new policy'
			else:
				if policy.is_running() == False:
					result = policy.start_instances()
					elapse = time.time() - start
					print str(elapse*1000) + ' ms takes to create instances'
				else:
					result['result'] = True
					result['value'] = policy.policy_id
		finally:
			self.request_lock.release()

		return json.dumps(result)

	def stopInstance(self, policy):
		result = {}
		policy_id = policy['policy_id']
		policy_found = self.policy_manager.get_policy(policy_id)

		if policy_found != None:
			server_list = policy_found.get_connected_instances()
			req = json.dumps({'type':'stop_instance',
                                'policy_id': policy_id,})

			for local_server in server_list:
				hostname = local_server[0]

				ip, port = self.local_server_manager.find_info_by_hostname(hostname)
					
				local_server_client = self.local_server_manager.get_local_server_client(hostname, port)

				if local_server_client != None:
					try:
						local_server_client.stopInstance(req)
					except TException, ex:
						print ex.message

		#reove for now. 
		self.policy_manager.remove_policy(policy_id)

		result['type'] = 'stop_instance'
		result['result'] = True
		result['value'] = policy_id

		return json.dumps(result)

	def getLocalServerList(self):
		server_list = self.local_server_manager.get_local_server_list()
		
		result = {}
		result['result'] = True
		result['value'] = server_list

		return json.dumps(result)
		
	def getInstance(self, policy_id, client_ip):
		policy = self.policy_manager.get_policy(policy_id)
		result = {}

		if policy != None:
			result['result'] = True
	
			#sort
			instance_list = policy.get_connected_instances()

			if client_ip != None and len(instance_list) > 0:
				if client_ip.startswith('::ffff:'):
 					client_ip = client_ip[7:]

				#sort
				instance_list = self._sort_by_distance(instance_list, client_ip)

			result['value'] = instance_list
#			print '[debug] ' + str(instance_list)
			print '[debug] getInstance called'
		else:
			result['result'] = False
			result['value'] = 'Cannot find policy with id: ' + policy_id
			print '[debug] ' + result['value']

		return json.dumps(result)

	def _sort_by_distance(self, instance_list, client_ip):
		cnt = len(instance_list)
		lat1, long1 = wieraCommon.get_location_info(client_ip)
		
		for i in range(0, cnt):
			instance_ip = instance_list[i][1]
			lat2, long2 = wieraCommon.get_location_info(instance_ip)
			distance = wieraCommon.get_distance(lat1, long1, lat2, long2)

			instance_list[i] = instance_list[i] + (distance,)

#		print instance_list
		return sorted(instance_list, key=itemgetter(3), reverse=True)
		
	def get_localip(self):
		return commands.getoutput("/sbin/ifconfig").split("\n")[1].split()[1    ][5:]

	def run_forever(self):
		#run Local Server manager
		ping_thread = threading.Thread(target=self.local_server_manager.run_forever(), args=())
		ping_thread.daemon = True
		ping_thread.start()
		#run Policy (Local Instance) manager
		ping_thread.join()
#		web_server_thread.join()

	def get_cost_info():
		#store
		with open(conf.get('setting', 'dc_info')) as f:
			dc_info = json.loads(f.read())

		return dc_info

######################
#main
if __name__ == '__main__':
	server = WieraServer()
	server.run_forever()
