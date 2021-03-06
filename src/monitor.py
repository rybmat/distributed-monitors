import threading
from contextlib import contextmanager

from mpi4py import MPI
from pprint import pprint

from __clock import Clock

from Queue import Queue


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

clock = Clock()

#log_file = open("logs/log_" + str(rank), 'w')
#log_file.close()

def log(txt):
	with open("logs/log_" + str(rank), 'a') as log_file:
		# log_file.write(txt + '\n')
		pass


def send(data, dest):
	data['sender'] = rank
	# log("send to " + str(dest) + ", " + str(data))
	comm.send(data, dest=dest)

def multicast(data, to=None, ommit=tuple()):
	""" multicasts data to "to" list of ranks ommmiting "ommit" iterable of ranks
	"""
	if to is None:
		to = range(size)

	for i in to:
		if i not in ommit:
			send(data, dest=i)

def receive(source=MPI.ANY_SOURCE):
	data = comm.recv(source=MPI.ANY_SOURCE)
	return data

###########################################################
###	Mutex
###########################################################
mutexes_lock = threading.Lock()
mutexes = {}

def Mutex(tag):
	with mutexes_lock:
		if tag not in mutexes:
			mutexes[tag] = __Mutex(tag)
		return mutexes[tag]

class __Mutex(object):
	""" Distributed mutex, implementation of Ricart-Agrawala algorithm
	"""

	def __init__(self, tag):
		self.interested = False
		self.in_critical = False

		self.mutex = threading.Lock()
		self.condition = threading.Condition(self.mutex)

		self.replies = [False for i in range(size)]
		self.deffered = set()

		self.tag = tag
		mutexes[self.tag] = self

	def lock(self):
		self.condition.acquire()
		self.interested = True
		clock.increase()					###############################
		self.request_clk = clock.value()
		data = {'type': 'mutex_lock', 'tag': self.tag, 'timestamp': clock.value()}
		multicast(data)		
		self.condition.wait()
		
		self.condition.release()

	def unlock(self):
		self.condition.acquire()

		if self.in_critical:
			print "out", rank
			log("out")
			self.interested, self.in_critical = False, False	
			clock.increase()				###############################

			data = {'type': 'mutex_reply', 'tag': self.tag, 'timestamp': clock.value()}
			multicast(data, to=self.deffered)
			self.deffered = set()

		self.condition.release()

	def on_request(self, request):
		""" action invoked by receiving thread when lock request received
		"""
		self.condition.acquire()
		clock.increase(request['timestamp'])	###############################
		
		#log("critical:" + str(self.in_critical) + " interested:" + str(self.interested) + " self.clk:" + str(clock.value()) + str(request))
		
		if self.in_critical or (self.interested and self.__higher_priority(request)):
	 		self.deffered.add(request['sender'])
	 	#	log("deffered:" + str(request['sender']) + " " + str(self.replies))
		else:
			clock.increase()			##########################
			data = {'type': 'mutex_reply', 'tag': self.tag, 'timestamp': clock.value()}
			send(data, dest=request['sender'])

		self.condition.release()

	def on_reply(self, msg):
		""" action when receiving thread receives reply message
		"""
		self.condition.acquire()
		
		self.replies[msg['sender']] = True
		if False not in self.replies:
			log(str(self.replies))

			self.in_critical = True
			print "critical section", rank
		#	log("critical section")

			self.replies = [False for i in range(size)]
			
			self.condition.notify()

		self.condition.release()

	def __higher_priority(self, req):
		if self.request_clk == req['timestamp']:
			return rank < req['sender']
		else:
			return self.request_clk < req['timestamp']


###########################################################
###	Conditional Variable
###########################################################

condvar_lock = threading.Lock()
condvars = {}

class ConditionalVariable(object):

	def __init__(self, tag):
		with condvar_lock:
			self.tag = tag
			condvars[self.tag] = self

		self.conditional = threading.Condition()
		self.is_waiting = False

	def wait(self, mutex):
		self.conditional.acquire()
		mutex.unlock()
		self.is_waiting = True
		self.conditional.wait()
		mutex.lock()
		self.conditional.release()

	def notify(self):
		data = {'type': 'conditional_notify', 'tag': self.tag}
		multicast(data, ommit=[rank])

	def on_notify(self):
		self.conditional.acquire()
		if self.is_waiting:
			self.conditional.notify()
			self.is_waiting = False
		self.conditional.release()


###########################################################
###	Resource
###########################################################

resources_lock = threading.Lock()
resources = {}

def Resource(obj, tag, mutex=None, master=0):
	with resources_lock:
		if tag not in resources:
			resources[tag] = __Resource(obj, tag, mutex, master)
		return resources[tag]

class __Resource(object):

	def __init__(self, obj, tag, mutex="auto", master=0):
		"""mutex is tag of mutex, not object itself"""
		self.tag = tag
		self.master = master
		self.obj = obj

		self.conditional = threading.Condition()

		if mutex == "auto":
			self.mutex = Mutex("resource_" + self.tag + "_lock")
		elif mutex is not None:
			self.mutex = Mutex(mutex)
		else: 
			self.mutex = None

		resources[self.tag] = self

	def __pull(self):
		self.conditional.acquire()
	#	log("inside __pull")
		if self.master != rank:
			data = {'type': 'resource_pull', 'tag': self.tag, 'master': self.master, 'mutex': self.mutex.tag if self.mutex else None}
			send(data, dest=self.master)
			self.conditional.wait()
		self.conditional.release()

	def __push(self):
		self.conditional.acquire()
	#	log("inside __push")
		if self.master != rank:
			data = {'type': 'resource_push', 'tag': self.tag, 'obj': self.obj, 'master': self.master, 'mutex': self.mutex.tag if self.mutex else None}
			send(data, dest=self.master)
			
			self.conditional.wait()
		self.conditional.release()

	def __enter__(self):
	#	log("inside enter")
		if self.mutex:
			self.mutex.lock()
		self.conditional.acquire()
		self.__pull()
		res = self.obj
		self.conditional.release()
		return res

	def __exit__(self, type, value, tb):
		self.conditional.acquire()
	#	log("inside exit")
		self.__push()
		if self.mutex:
			self.mutex.unlock()
		self.conditional.release()

	def on_pull(self, msg):
		""" action invoked by receiving thread when pull request """
		self.conditional.acquire()
	#	log("inside on_pull")
		data = {'type': 'resource_reply_pull', 'tag': self.tag, 'obj': self.obj, 'master': self.master, 'mutex': self.mutex.tag if self.mutex else None}
		send(data, dest=msg['sender'])
		self.conditional.release()

	def on_push(self, msg):
		""" action invoked by receiving thread when push request (only in master process) """
		self.conditional.acquire()
	#	log("inside on_push")
		self.obj = msg['obj']
		data = {'type': 'resource_reply_push', 'tag': self.tag, 'master': self.master, 'mutex': self.mutex.tag if self.mutex else None}
		send(data, dest=msg['sender'])
		self.conditional.release()

	def on_reply(self, msg):
		""" invoked by receiving thread when reply to pull or push """
		self.conditional.acquire()
	#	log("inside on_reply")
		if msg['type'] == 'resource_reply_pull' and msg['obj'] is not None:
			log("resource_reply_pull")
			#print "rpl", rank, msg
			self.obj = msg['obj']
		
		self.conditional.notify()
		self.conditional.release()


###########################################################
###	receiving thread
###########################################################

def __receive_thread():
	exit_counter = 0
	run = True
	while run:
		data = receive(source=MPI.ANY_SOURCE)
	#	log("rcv " + str(data))
		#print "recv", rank, clock.value(), data
		
		if data['type'].startswith('mutex_'):
			process_mutex_message(data)
		
		elif data['type'].startswith('conditional_'):
			process_conditional_message(data)

		elif data['type'].startswith('resource_'):
			process_resource_message(data)
		
		elif data['type'] == "exit":
			exit_counter += 1
			if exit_counter == size:
				run = False

def process_mutex_message(msg):
	mutex = Mutex(msg['tag'])
	if msg['type'] == 'mutex_lock':
		mutex.on_request(msg)
	
	elif msg['type'] == 'mutex_reply':
		mutex.on_reply(msg)

def process_conditional_message(msg):
	with condvar_lock:
		if msg['tag'] in condvars:
			if msg['type'] == 'conditional_notify':
				condvars[msg['tag']].on_notify()

def process_resource_message(msg):
	#print "recv", rank, msg
	res = Resource(None, msg['tag'], msg['mutex'], msg['master'])
#	log("recv: Resource, " + res.tag + " " + res.mutex.tag)
	if msg['type'] == 'resource_pull':
		res.on_pull(msg)

	elif msg['type'] == 'resource_push':
		res.on_push(msg)

	elif msg['type'].startswith('resource_reply'):
		res.on_reply(msg)

###########################################################

def finalize():
	multicast({'type': 'exit', 'timestamp': clock.value()})


t = threading.Thread(target=__receive_thread)
t.start()
