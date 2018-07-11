import time
import sys
import json
import threading
import wieraCommon
from pprint import pprint

from LocalInstanceToWieraIface import *
from LocalInstanceToWieraIface.ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from WieraToLocalInstanceIface import *
from WieraToLocalInstanceIface.ttypes import *

class LocalInstanceToWieraHandler:
	def __init__(self, policy, local_instance_manager):#iera_instance_manager):
		self.policy = policy
		self.local_instance_manager = local_instance_manager
		self.lock = threading.Lock()

	def updateMonitoringData(self, monitoring_info):
		json_data = json.loads(monitoring_info)
		result = self.local_instance_manager.update_monitoring_info(json_data)

		return json.dumps(result)

	def requestNewDataPlacementPolicy(self, request):
		try:
			self.lock.acquire()

			json_request = json.loads(request)
			result = self.policy.re_evaluate_data_placement(json_request)
		finally:
			self.lock.release()

		return json.dumps(result)

	def _send_peers_info(self, peer_info):
		#empty query for first dataplacement. 
		
		bDataPlacement = False

		if bDataPlacement == True and 'trips' in self.policy.policy_spec:
#			monitoring_info = self.local_instance_manager.get_monitoring_info(['ebs-st1', 'ebs-gp2', 's3', 'standard-disk', 'premium-p10'])
			monitoring_info = self.local_instance_manager.get_monitoring_info(['ebs-st1', 'ebs-gp2', 's3'])
#<F12			pprint(monitoring_info)
			dummy_query = {}

			dummy_query['access_info'] = {}
			dummy_query['object_size'] = 8192
		
			for hostname in self.local_instance_manager.instance_list:
				dummy_query['access_info'][hostname] = {}
				dummy_query['access_info'][hostname]['get_access_cnt'] = 1000000
				dummy_query['access_info'][hostname]['put_access_cnt'] = 1000000

			data_placement = self.policy.evaluate_data_placement(dummy_query, monitoring_info)

			thread_list = []

			for instance in self.local_instance_manager.instance_list:
				client = self.local_instance_manager.get_local_instance_client(instance)
				thread = wieraCommon.parallel_exec(client.dataPlacementChange, [json.dumps(data_placement),])
				thread_list.append(thread)

			wieraCommon.join_threads(thread_list)

		thread_list = []
		
		for instance in self.local_instance_manager.instance_list:
			client = self.local_instance_manager.get_local_instance_client(instance)
			thread = wieraCommon.parallel_exec(client.peersInfo, [json.dumps(peer_info),])
			thread_list.append(thread)
		
		wieraCommon.join_threads(thread_list)

	def registerLocalInstance(self, instance_info, instance_ip):
#		print 'registerTier' + str(instance_info)
		instance_info = json.loads(instance_info)

		try:
			self.lock.acquire()
#			print 'lock acquired'
			result = {}

			if instance_info != None:
				hostname = instance_info['hostname']

				if 'ip' not in instance_info:
					ip = instance_ip.strip('::ffff:')
					print ip + ' from callback_ip from thrift'
				else:
					ip = instance_info['ip']
		
				stored_server_ip = self.local_instance_manager.policy.find_ip_by_hostname(hostname)

				if stored_server_ip == None:
					reason = 'Connection from ' + hostname + ':' + ip + ' is not in the expected location of Local server thus rejected.'
					print reason
					result['result'] = False
					result['value'] = reason
				elif ip != stored_server_ip:
					reason = 'Hostname "' + hostname + '" is duplicated. ip: ' + ip + ' ip: ' + stored_server_ip
					print reason
					result['result'] = False
					result['value'] = reason
				else:
					ports = instance_info['value']
					application_port = ports['application_port']

					print ports

					if 'peer_port' in ports:
						peer_port = ports['peer_port']
					else:
						peer_port = 0

					instance_port = ports['instance_port']

					self.local_instance_manager.add_local_instance(hostname, ip, application_port, peer_port, instance_port)
					

					#check all instance are connected
					#only TripS mode supports data placement
					if self.policy.check_instance_cnt() == True:
						peer_info = {}
						peer_info['value'] = self.local_instance_manager.get_connected_instances()
						peer_info['result'] = True;
						#create thread for update peer info to all 
						thread  = threading.Thread(target=self._send_peers_info, args=(peer_info,))
						thread.daemon = True
						thread.start()

						#make dummy access dataplacement

					result['value'] = json.dumps(self.policy.get_cost_info())
					result['value2'] = json.dumps(self.policy.get_goals())
					result['result'] = True

					print '[TIM-' + self.local_instance_manager.policy.policy_id + ']' + hostname + '(' + ip + ':' + str(instance_port) + ') is registered.'
			else:
				result['result'] = False
				result['value'] = 'Failed to load request to json'
		finally:
			self.lock.release()
#			print 'lock released'

		return json.dumps(result)

	def requestPolicyChange(self, policy):
		start = time.time()
		#policy = json.loads(policy)

		response = {}
		failed_list = self.local_instance_manager.broadcast(policy, 'policyChange')

		#there is failed instance
		if len(failed_list) > 0:
			for hostname in failed_list:
				print '[TIM] failed to change policy.' + hostname + ' with a reason: ' + failed_list[hostname]['value']
								
			response['result'] = False
			response['value'] = 'Failed to change policy.'
		else:
			response['result'] = True
		elapse = time.time() - start
		print str(elapse * 1000) + ' ms takes to change policy'

		return json.dumps(response)

class LocalInstanceManager:
	def __init__(self, policy):#, expected_instance_cnt):
		self.instance_list = {}
		self.ip = wieraCommon.get_public_ip()
		self.port = 0
		self.policy = policy
		self.monitoring_info = {}
		self.monitoring_lock = threading.Lock()
            

		# set handler to our implementation
		# to avoid any sync issue with portnumber
		handler = LocalInstanceToWieraHandler(policy, self)

		processor = LocalInstanceToWieraIface.Processor(handler)
		self.transport = TSocket.TServerSocket(None)
		tfactory = TTransport.TFramedTransportFactory()
		pfactory = TBinaryProtocol.TBinaryProtocolFactory()

		# set server
		self.server = TServer.TThreadPoolServer(processor, self.transport, tfactory, pfactory, daemon=True)
		self.port = self.transport.port

		#set socket thread 20 min
		self.server.setNumThreads(64)
	
		#Thrift Server Start	
		self.instance_manager_thread = threading.Thread(target=self.run_forever, args=())
		self.instance_manager_thread.daemon = True
		self.instance_manager_thread.start()

	def remove_local_instance(self, hostname):
#		self.lock.acquire()
		if hostname in self.instance_list:
			del self.instance_list[hostname]
#		self.lock.release()

	def get_local_instance_client(self, hostname):
		if hostname in self.instance_list:
			client = self.instance_list[hostname]['thrift_client']
			return client

		return None

	def get_instance_list(self):
		return self.instance_list

	def get_connected_instances(self):
		#return as a list
		instance_list = []

		for hostname in self.instance_list:
			ip = self.instance_list[hostname]['ip']
			application_port = self.instance_list[hostname]['application_port']
			peer_port = self.instance_list[hostname]['peer_port']
		
			instance_info = (hostname, ip, application_port, peer_port)
			instance_list.append(instance_info)

#		print instance_list
		
		return instance_list

	def get_manager_server_info(self):
		return (self.ip, self.port)

	def add_local_instance(self, hostname, ip, application_port, peer_port, instance_port):
		self.instance_list[hostname] = {}

		self.instance_list[hostname]['ip'] = ip
		self.instance_list[hostname]['application_port'] = application_port
		self.instance_list[hostname]['peer_port'] = peer_port
		self.instance_list[hostname]['instance_port'] = instance_port

#		print self.instance_list[hostname]

		#thrift needed.
		transport = TSocket.TSocket(ip, instance_port)
		transport = TTransport.TFramedTransport(transport)
		protocol = TBinaryProtocol.TBinaryProtocol(transport)
		client = WieraToLocalInstanceIface.Client(protocol)

		transport.open()
		self.instance_list[hostname]['thrift_client'] = client
		self.instance_list[hostname]['socket'] = transport

		try:
			ret = client.ping()
		except TException, e:
			wieraCommon.PrintException()

		#check need to propagate. 
		local_server_cnt = len(self.policy.get_available_host_list())
		connected_cnt = len(self.instance_list)

		if connected_cnt == local_server_cnt:
			return self.get_connected_instances()

		
		return None
				
	def run_forever(self):
		print '[TIM] Local Instance Manager is ready for Local Instance port: ' + str(self.port) 
		self.server.serve()

	def stop_server(self):
		print 'Try to close server and thrift client to instance.'
		self.transport.close() #server

		for hostname in self.instance_list:
			self.instance_list[hostname]['socket'].close()

	#will return failed_list
	#req is Json type
	#req_type is string type
	def broadcast(self, req, req_type, instance_list=None, timeout=5):
		failed_list = {}
		thread_list = {}

		if instance_list == None:
			instance_list = self.instance_list

		for hostname in instance_list:
			client = instance_list[hostname]['thrift_client']

			if client != None:
				if req_type == 'policyChange':
					thread = wieraCommon.parallel_exec(client.policyChange, [req,])
#					print '[debug] sent dataDistribution message to ' + hostname
					thread_list[hostname] = thread
				elif req_type == 'dataPlacement':
					thread = wieraCommon.parallel_exec(client.dataPlacementChange, [req,])
#					print '[debug] sent dataPlacement message to ' + hostname
					thread_list[hostname] = thread
				else:
					return 'Not supported request';

		wieraCommon.join_threads(thread_list.values(), timeout)

		for hostname in thread_list:
			ret = thread_list[hostname].get_result()
			json_ret = json.loads(ret)

			if json_ret['result'] == False:
				failed_list[hostname] = json_ret
				print 'there are failed nodes'
#		else:
#				print 'Broadcasting to ' + hostname + ' success: ' + json_ret['value']

		return failed_list

	
	def update_monitoring_info(self, data):
		try:
			self.monitoring_lock.acquire()
			result = {}

			if data != None:
				hostname = data['hostname']
				json_data = json.loads(data[hostname])
				self.monitoring_info[hostname] = json_data

				result['result'] = True
				result['value'] = 'Latency info has been updated'

#				print hostname + ' monitoring data has been updated'
				with open('monitoring_info', 'w') as output:
					json.dump(self.monitoring_info, output)

			else:
				result['result'] = False
				result['value'] = 'Data is not readable or not JSon format'
		except:
			print 'except happen'
			raise
		finally:
			self.monitoring_lock.release()
			
		return result			

	#need to be  implemented each DC in mind
	def get_monitoring_info(self, supported_storage=None):
		#there is no monitored _info. 
		if len(self.monitoring_info) == 0:
			#use history latency
			with open('./trips_data/monitoring_info') as data_file:
				monitoring_info = json.load(data_file)
		
			for hostname in self.instance_list:
				if supported_storage == None:
					self.monitoring_info[hostname] = monitoring_info[hostname]
				else:
					self.monitoring_info[hostname] = {}
					self.monitoring_info[hostname]['network_latency'] = monitoring_info[hostname]['network_latency']

					self.monitoring_info[hostname]['storage_latency'] = {}

					for storage in supported_storage:
						if storage in monitoring_info[hostname]['storage_latency']:
							self.monitoring_info[hostname]['storage_latency'][storage] = monitoring_info[hostname]['storage_latency'][storage]
	
		return self.monitoring_info
