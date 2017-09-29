import socket
import time
import sys
import json
import threading
import policy

class PolicyManager:
	policies = {}

	def __init__(self):
		print 'PolicyManager is running.'
	
	def add_new_policy(self, wiera, policy_spec):
#		if policy_id not in self.policies:
		new_policy = policy.Policy(wiera, policy_spec)
		new_policy_id = new_policy.get_policy_id()
		self.policies[new_policy_id] = new_policy

		return new_policy
#		else:
#			updated_policy = policy.Policy(poli
#			updated_policy = self.update_policy(self, policy_id, policy_content)
#			return updated_policy

	def remove_policy(self, policy_id):
		policy = self.get_policy(policy_id)
		
		if policy != None:
			policy.stop_instance_manager()
		
		del self.policies[policy_id]

	def get_policy(self, policy_id):
		if policy_id in self.policies:
			return self.policies[policy_id]

		return None

	def update_policy(self, policy_id, policy_content):
		print 'policy should be updated.'
