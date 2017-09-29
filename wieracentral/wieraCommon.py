import os
import socket
import math
import linecache
import sys
import time
#from geoip import geolite2
#from geoip import open_database
import pygeoip

geo_data = pygeoip.GeoIP('./geodb/GeoLiteCity.dat')

def get_location_info(ip):
	geo_info = geo_data.record_by_name(ip)

	if geo_info is not None:
		return geo_info['latitude'], geo_info['longitude']
	else:
		return 0, 0#eo_info['latitude'], geo_info['longitude']

	

def get_distance(lat1, long1, lat2, long2):
 
    # Convert latitude and longitude to 
    # spherical coordinates in radians.
    degrees_to_radians = math.pi/180.0
         
    # phi = 90 - latitude
    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians
         
    # theta = longitude
    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians
         
    # Compute spherical distance from spherical coordinates.
         
    # For two locations in spherical coordinates 
    # (1, theta, phi) and (1, theta', phi')
    # cosine( arc length ) = 
    #    sin phi sin phi' cos(theta-theta') + cos phi cos phi'
    # distance = rho * arc length
     
    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) + 
           math.cos(phi1)*math.cos(phi2))

    arc = math.acos(cos)

	# 6373 in km, 3960 mile
    # Remember to multiply arc by the radius of the earth 
    # in your favorite set of units to get length.
    return arc * 6373

def is_valid_ip(ip):
	try:
		socket.inet_aton(ip)
		return True
		# legal
	except socket.error:
		# Not legal
		return False

def get_public_ip():
	from urllib2 import urlopen
	ip = urlopen('http://checkip.amazonaws.com').read()
	ip = ip.replace('\n', '')
	return ip

def PrintException():
	exc_type, exc_obj, tb = sys.exc_info()
	f = tb.tb_frame
	lineno = tb.tb_lineno
	filename = f.f_code.co_filename
	linecache.checkcache(filename)
	line = linecache.getline(filename, lineno, f.f_globals)
	print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)

def printplus(obj):
	# Dict
	if isinstance(obj, dict):
		for k, v in sorted(obj.items()):
			print '  ' + u'{0}: {1}'.format(k, v)
		# List or tuple
	elif isinstance(obj, list) or isinstance(obj, tuple):
		for x in obj:
			print '  ' + x
	# Other
	else:
		print '  ' + obj

from threading import Thread

class ThreadWithReturn(Thread):
	def __init__(self, group=None, target=None, name=None,
					args=(), kwargs={}, Verbose=None):
		Thread.__init__(self, group, target, name, args, kwargs, Verbose)
		self._return = None
	def run(self):
		if self._Thread__target is not None:
			self._return = self._Thread__target(*self._Thread__args, **self._Thread__kwargs)
	def get_result(self):
		Thread.join(self)
		return self._return

def parallel_exec(func, param, daemon=True):
	thread = ThreadWithReturn(target=func, args=param)
	thread.daemon = daemon
	thread.start()
	return thread

def join_threads(thread_list, timeout=5):
	was_timeout = False
	
	if len(thread_list) > 0:
		if type(thread_list) is list:
			for thread in thread_list:
				start = time.time()
				thread.join(timeout)
			
#				print str((time.time() - start) * 1000) + ' ms takes to join'

				#check whether the thread is done or timeout
				if thread.isAlive() == True:
					was_timeout = True
		elif type(thread_list) is dict:
			for hostname in thread_list:
				start = time.time()
				thread = thread_list[hostname]
				thread.join(timeout)
		
#				print 'hostname: ' + hostname + ' ' + str(time.time() - start) + ' ms takes to join'

				if thread.isAlive() == True:
					was_timeout = True

	return was_timeout	
