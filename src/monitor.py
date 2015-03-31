import threading
from contextlib import contextmanager

from mpi4py import MPI
from pprint import pprint

from __clock import Clock

from Queue import Queue



comm_lock = threading.Lock()
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

clock = Clock()

def send(data, dest):
	clock.increase()
	data['timestamp'] = clock.value()
	data['sender'] = rank
	with comm_lock:
		comm.send(data, dest=dest)

def multicast(data, to=None, ommit=tuple()):
	""" multicasts data to "to" list of ranks ommmiting "ommit" iterable of ranks
	"""
	if to is None:
		to = range(size)
	
	clock.increase()
	data['timestamp'] = clock.value()
	data['sender'] = rank

	with comm_lock:
		for i in to:
			if i not in ommit:
				comm.send(data, dest=i)

def receive(source=MPI.ANY_SOURCE):
	data = comm.recv(source=MPI.ANY_SOURCE)
	clock.increase(data['timestamp'])
	return data

###########################################################
###	Mutex
###########################################################

mutexes_lock = threading.Lock()
mutexes = {}

class Mutex(object):
	""" Distributed mutex, implementation of Ricart-Agrawala algorithm
	"""

	def __init__(self, tag):
		self.interested = False
		self.in_critical = False

		self.mutex = threading.Lock()
		self.condition = threading.Condition(self.mutex)

		self.replies_number = 0	
		self.deffered = set()

		with mutexes_lock:
			self.tag = tag
			mutexes[self.tag] = self

	def lock(self):
		self.condition.acquire()

		self.interested = True
		data = {'type': 'mutex_lock', 'tag': self.tag}
		multicast(data)		
		self.condition.wait()
		self.in_critical = True
		
		self.condition.release()
		print "critical section", rank

	def unlock(self):
		self.condition.acquire()

		if self.in_critical:
			print "out", rank
		
			self.interested, self.in_critical = False, False	
		
			data = {'type': 'mutex_reply', 'tag': self.tag}
			multicast(data, to=self.deffered)
			self.deffered = set()

			self.replies_number = 0
		self.condition.release()

	def on_request(self, request):
		""" action invoked by receiving thread when lock request received
		"""
		self.condition.acquire()

		if self.in_critical or (self.interested and self.__higher_priority(request)):
	 		self.deffered.add(request['sender'])
		else:
			data = {'type': 'mutex_reply', 'tag': self.tag}
			send(data, dest=request['sender'])

		self.condition.release()

	def on_reply(self):
		""" action when receiving thread receives reply message
		"""
		self.condition.acquire()
		
		self.replies_number += 1
		if self.replies_number == size:
			self.condition.notify()

		self.condition.release()

	def __higher_priority(self, req):
		if clock.value() == req['timestamp']:
			return rank < req['sender']
		else:
			return clock.value() < req['timestamp']


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
		mutex.unlock()
		self.conditional.acquire()
		self.is_waiting = True
		self.conditional.wait()
		self.conditional.release()
		mutex.lock()

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

class Resource(object):

	def __init__(self, obj, tag, mutex=None, master=0):
		self.tag = tag
		self.master = master
		self.obj_lock = threading.Lock()
		self.obj = obj

		self.conditional = threading.Condition()
		
		if mutex == "auto":
			self.mutex = Mutex("resource_" + self.tag + "_lock")
		else:
			self.mutex = mutex

		with resources_lock:
			if self.tag not in resources:
				resources[self.tag] = self

	def __pull(self):
		if self.master != rank:
			data = {'type': 'resource_pull', 'tag': self.tag}
			send(data, dest=self.master)
			
			self.conditional.acquire()
			self.conditional.wait()
			self.conditional.release()

	def __push(self):
		if self.master != rank:
			with self.obj_lock:
				data = {'type': 'resource_push', 'tag': self.tag, 'obj': self.obj, 'timestamp': clock.value(), 'sender': rank}
				send(data, dest=self.master)

			self.conditional.acquire()
			self.conditional.wait()
			self.conditional.release()

	def __enter__(self):
		if self.mutex:
			self.mutex.lock()
		self.__pull()
		with self.obj_lock:
			res = self.obj
		return res

	def __exit__(self, type, value, tb):
		self.__push()
		if self.mutex:
			self.mutex.unlock()

	def on_pull(self, msg):
		""" action invoked by receiving thread when pull request """
		with self.obj_lock:
			data = {'type': 'resource_reply_pull', 'tag': self.tag, 'obj': self.obj, 'timestamp': clock.value(), 'sender': rank}
			send(data, dest=msg['sender'])

	def on_push(self, msg):
		""" action invoked by receiving thread when push request (only in master process) """
		with self.obj_lock:
			self.obj = msg['obj']
		data = {'type': 'resource_reply_push', 'tag': self.tag, 'timestamp': clock.value(), 'sender': rank}
		send(data, dest=msg['sender'])

	def on_reply(self, msg):
		""" invoked by receiving thread when reply to pull or push """
		if msg['type'] == 'resource_reply_pull' and msg['obj']:
			with self.obj_lock:
				#print "rpl", rank, msg
				self.obj = msg['obj']

		self.conditional.acquire()
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
	with mutexes_lock:
		if not (msg['tag'] in mutexes):
			reply = {'type': 'mutex_reply', 'tag': msg['tag'], 'timestamp': clock.value(), 'sender': rank}
			send(reply, dest=msg['sender'])
		
		elif msg['type'] == 'mutex_lock':
			mutexes[msg['tag']].on_request(msg)
		
		elif msg['type'] == 'mutex_reply':
			mutexes[msg['tag']].on_reply()

def process_conditional_message(msg):
	with condvar_lock:
		if msg['tag'] in condvars:
			if msg['type'] == 'conditional_notify':
				condvars[msg['tag']].on_notify()

def process_resource_message(msg):
	#print "recv", rank, msg
	with resources_lock:
		if msg['tag'] not in resources:
			reply = {'type': 'resource_replay', 'tag': msg['tag'], 'timestamp': clock.value(), 'sender': rank}
			send(reply, dest=msg['sender'])

		elif msg['type'] == 'resource_pull':
			resources[msg['tag']].on_pull(msg)

		elif msg['type'] == 'resource_push':
			resources[msg['tag']].on_push(msg)

		elif msg['type'].startswith('resource_reply'):
			resources[msg['tag']].on_reply(msg)

###########################################################

def finalize():
	multicast({'type': 'exit', 'timestamp': clock.value()})


t = threading.Thread(target=__receive_thread)
t.start()
