import ConfigParser
import wieraCommon

class Conf:
	conf = {}

	def true_false(self, value):
		value = value.upper()

		if value[0] == 'T':
			return True
		else:
			return False

	def __init__(self, file_path):
		Config = ConfigParser.ConfigParser()
		Config.read(file_path)
	
		self.conf['local_server_port'] = int(Config.get('wiera', 'local_server_port'))
		self.conf['applications_port'] = int(Config.get('wiera', 'applications_port'))
		self.conf['ping_interval'] = int(Config.get('wiera', 'ping_interval'))
		self.conf['ui_command'] = self.true_false(Config.get('etc', 'ui_command'))

		#check data type
		self.conf['int_value'] = []
		self.conf['float_value'] = []
		self.conf['bool_value'] = []

		for _con in self.conf:
			con_type = type(self.conf[_con])

			if con_type is int:
				self.conf['int_value'].append(_con)
			elif con_type is float:
				self.conf['float_value'].append(_con)
			elif con_type is bool:
				self.conf['bool_value'].append(_con)
	def get(self, key):
		return self.conf[key]

	def set(self, key, value):
		if key in self.conf['int_value']:
			self.conf[key] = int(value)
		elif key in self.conf['float_value']:
			self.conf[key] = float(value)
		elif key in self.conf['bool_value']:
			self.conf[key] = self.true_false(value)
		else:
			self.conf[key] = value

	def show(self, option=None):
		if option == None:
			wieraCommon.printplus(self.conf)
		else:
			for conf in self.conf:
				if option in conf:
					print '  ' + conf + ': ' + str(self.conf[conf])
