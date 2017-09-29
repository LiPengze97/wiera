import socket
import time
import sys
import json
import threading
import localInstanceManager
import uuid
import wieraCommon
from pprint import *

sys.path.append('./trips')

import trips

class Policy:	
	def __init__(self, wiera_instance, policy_spec):
		self.wiera_instance = wiera_instance
		self.policy_spec = policy_spec #need to be deleted
		self.available_host_list = None
		self.cost_info = None
		self.goals = None

		if 'id' in policy_spec:
			self.policy_id = policy_spec['id']
		else:
			self.policy_id = str(uuid.uuid4())

		#should be from policy -> For now only server list
		self.local_instance_manager = localInstanceManager.LocalInstanceManager(self)

		if 'trips' in policy_spec:
			self.trips = self.init_trips()
#		self.re_evaluate_data_placement(None)

	def re_evaluate_data_placement(self, query):
		if self.check_instance_cnt() == False:
			print 'Not yet ready to evalaute data placement!!!!!!!!!!!!!!!!!!!!'
			return 
		
#		if query == None:
#		with open('./trips_data/query') as data_file:
#		query = json.load(data_file)

		monitoring_info = self.local_instance_manager.get_monitoring_info()
		data_placement = self.evaluate_data_placement(query, monitoring_info)

		result = {}
		result['result'] = False
		result['value'] = 'not handled'

		if data_placement == None or data_placement == False:
			result['value'] = 'Failed to generage data palcement'
			print 'Failed to generage data palcement'
		else:
			start = time.time()
			data_placement['prefer_storage_type'] = 'cheapest'

			failed_list = self.broadcast(json.dumps(data_placement), 'dataPlacement')
			#there is failed instance
			if len(failed_list) > 0:
				for hostname in failed_list:
					print '[TIM] failed to data placement in ' + hostname + ' with a reason: ' + failed_list[hostname]['value']
					
				result['result'] = False
				result['value'] = 'Failed happen to broadcast data placement. Failed instance(s):' + str(failed_list) 
			else:
				result['result'] = True
				result['value'] = 'Success to apply the new data placement'

#	threading.Timer(5, self.re_evaluate_data_placement, [query]).start()
		
		return result

	def evaluate_data_placement(self, query, monitoring_info):
	#	for now dynamic goals
		with open('./trips_data/goals') as data_file:
			goals = json.load(data_file)

		if self.policy_spec['data_distribution'] == 0:
			goals['consistency'] = 'strong'
		else:
			goals['consistency'] = 'eventual'
		
		self.trips.set_goals(goals)		

		return self.trips.evaluate(query, monitoring_info)

	def get_cost_info(self):
		if self.cost_info == None:
			with open('./trips_data/cost_info') as data_file:
				self.cost_info = json.load(data_file)
		return self.cost_info

	def get_goals(self):
		if self.goals == None:
			with open('./trips_data/goals') as data_file:
				self.goals = json.load(data_file)
		return self.goals

	def init_trips(self):
		trips_instance = trips.TripS(self)
		
		with open('./trips_data/cost_info') as data_file:
			self.cost_info = json.load(data_file)

		with open('./trips_data/goals') as data_file:
			self.goals = json.load(data_file)

		trips_instance.set_cost_info(self.cost_info)
		trips_instance.set_goals(self.goals)
		
		return trips_instance

	def get_desired_instance_cnt(self):
#		print policy_spec['host_list']
		return len(self.policy_spec['host_list'])

	def check_host_status(self):
		result = {}
		host_info = self.policy_spec['host_list']
		self.available_host_list = self.wiera_instance.check_available_host(host_info)

		print self.available_host_list

		if len(self.available_host_list) <= 0:
			result['result'] = False
			result['value'] = 'Not available hostname. StartInstance() has been canceled'
			print 'Not available hostname. CreateInstance() has been canceled'
			return False
		elif len(self.available_host_list) < len(host_info):
			result['result'] = False
			result['value'] = 'Not enough available hostname. StartInstance() has been canceled'
			print 'Not enough available hostname. CreateInstance() has been canceled'
			return False

		return True

	def stop_instance_manager(self):
		return self.local_instance_manager.stop_server()

	def start_instances(self):
		result = {}

		if self.check_host_status() == False:
			result['result'] = False
			result['value'] = 'Not all Local servers are ready to spwan instances'
			return result

		instance_manager_ip, instance_manager_port = self.get_instance_manager_address()

		#send request to local server
		thread_list = {}

		#generate request storage tier will be difference from each other from here

		#if 'primary' in self.policy_spec['consistency_policy']:
#		if self.policy_spec['data_distribution'] == 1:
#			request['primary'] = self.policy_spec['primary']
		
		#if 'eventual' in self.policy_spec['consistency_policy']:
#		if self.policy_spec['data_distribution'] == 3:
#			request['period'] = self.policy_spec['period']

#		if self.policy_spec['data_distribution'] == 6:
#			request['period'] = self.policy_spec['period']
#			request['data_distribution_consistency'] = self.policy_spec['data_distribution_consistency']
#			request['prefer_storage_type'] = self.policy_spec['prefer_storage_type']

#		print self.available_host_list

		for hostname in self.available_host_list:
			request = {}
			request['type'] = 'start_instance'
			request['id'] = self.policy_id
			request['instance_cnt'] = self.get_desired_instance_cnt()
			request['manager_ip'] = instance_manager_ip
			request['manager_port'] = instance_manager_port

			#this req will go to Local server
			request[hostname] = self.policy_spec['host_list'][hostname]
			req = json.dumps(request)

			port = self.available_host_list[hostname]['ports']
			local_server_client, port = self.wiera_instance.local_server_manager.get_local_server_client(hostname, port)
			
			if local_server_client != None:
				#make it thread.
				#local_server_client.startInstance(req)
				thread = wieraCommon.parallel_exec(local_server_client.startInstance, [req,])
				print '[debug] sent broadcast startInstance to ' + hostname
				thread_list[hostname] = thread

		wieraCommon.join_threads(thread_list)
#		print str(time.time() - start) + ' ms for join : ' + str(len(thread_list))

		result['result'] = True
		result['value'] = self.policy_id
	
		return result

	def get_policy_id(self):
		return self.policy_id

	def get_instance_manager_address(self):
		return self.local_instance_manager.get_manager_server_info()

	#list of Local server
	def get_available_host_list(self):
		return self.available_host_list

	#list of Local instnace
	def get_connected_instances(self):
		return self.local_instance_manager.get_connected_instances()


	def find_hostname_by_ip(self, ip):
		for hostname in self.available_host_list:
			if self.available_host_list[hostname]['ip'] == ip:
				return hostname
		return None

	def check_instance_cnt(self):
		if self.available_host_list != None and len(self.available_host_list) == len(self.local_instance_manager.get_connected_instances()):
			return True
		
		return False

	def find_ip_by_hostname(self, hostname):
		if hostname in self.available_host_list:
			return self.available_host_list[hostname]['ip']
		return None

	def is_exist_hostname(self, hostname):
		if hostname in self.available_host_list:
			return True
		return False

	def broadcast(self, contents, content_type, instance_list=None, timeout=5):
		return self.local_instance_manager.broadcast(contents, content_type, instance_list, timeout)

