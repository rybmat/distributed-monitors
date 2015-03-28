import threading

from mpi4py import MPI
from pprint import pprint

from __clock import Clock

clock = Clock()

comm_lock = threading.Lock()
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

def multicast(data, to=None, ommit=tuple()):
	if to is None:
		to = range(comm.Get_size())
	
	clock.increase()
	for i in to:
		if i not in ommit:
			comm.send(data, dest=i)

###########################################################
###	Mutex
###########################################################

mutexes_lock = threading.Lock()
mutexes = {}
mutex_num = 0

class Mutex(object):
	""" Distributed mutex, implementation of Ricart-Agrawala algorithm
	"""

	def __init__(self):
		self.interested = False
		self.replies_condition = threading.Condition()
		self.replies_number_lock = threading.Lock()
		self.replies_number = 0
		
		self.deffered_lock = threading.Lock()
		self.deffered = []

		with mutexes_lock:
			self.tag = globals()['mutex_num']
			mutexes[self.tag] = self
			globals()['mutex_num'] += 1


	def lock(self):
		self.interested = True
		
		data = {'type': 'mutex_lock', 'tag': self.tag, 'timestamp': clock.value(), 'sender': rank}
		multicast(data, ommit=[rank])

		self.replies_condition.acquire()
		self.replies_condition.wait()
		self.replies_condition.release()


	def unlock(self):
		if self.interested:
			with self.deffered_lock:
				self.interested = False	
				data = {'type': 'mutex_reply', 'tag': self.tag, 'timestamp': clock.value(), 'sender': rank}
				multicast(data, to=self.deffered)
				self.deffered = []

			with self.replies_number_lock:
				self.replies_number = 0

	def on_request(self, request):
		""" action invoked by receiving thread when lock request received
		"""
		if (not self.interested) or (clock.value() > request['timestamp']):
			data = {'type': 'mutex_reply', 'tag': self.tag, 'timestamp': clock.value(), 'sender': rank}
			clock.increase()
			comm.send(data, dest=request['sender'])
		else:
			with self.deffered_lock:
				self.deffered.append(request['sender'])


	def on_reply(self):
		""" action when receiving thread receives reply message
		"""
		with self.replies_number_lock:
			self.replies_number += 1
			if self.replies_number == (comm.Get_size() - 1):
				self.replies_condition.acquire()
				self.replies_condition.notify()
				self.replies_condition.release()


###########################################################
###	Conditional Variable
###########################################################

condvar_lock = threading.Lock()
condvars = {}
condvar_num = 0

class ConditionalVariable(object):

	def __init__(self):
		with condvar_lock:
			self.tag = globals()['condvar_num']
			condvars[self.tag] = self
			globals()['condvar_num'] += 1

		self.conditional = threading.Condition()
		self.is_waiting = False

	def wait(self, mutex):
		self.is_waiting = True
		self.conditional.acquire()
		mutex.unlock()
		self.conditional.wait()
		self.conditional.release()
		mutex.lock()

	def notify(self):
		data = {'type': 'conditional_notify', 'tag': self.tag, 'timestamp': clock.value(), 'sender': rank}
		multicast(data, ommit=[rank])

	def on_notify(self):
		self.conditional.acquire()
		if self.is_waiting:
			self.conditional.notify()
			self.is_waiting = False
		self.conditional.release()


###########################################################
###	receiving thread
###########################################################

def __receive_thread():
	exit_counter = 0
	run = True
	while run:
		data = comm.recv(source=MPI.ANY_SOURCE)
		clock.increase(data['timestamp'])
		#print "recv", rank, clock.value(), data
		
		if data['type'].startswith('mutex_'):
			process_mutex_message(data)
		
		elif data['type'].startswith('conditional_'):
			#print "recv", rank, clock.value(), data
			process_conditional_message(data)
		
		elif data['type'] == "exit":
			exit_counter += 1
			if exit_counter == comm.Get_size():
				run = False


def process_mutex_message(msg):
	with mutexes_lock:
		if not (msg['tag'] in mutexes):
			reply = {'type': 'mutex_reply', 'tag': msg['tag'], 'timestamp': clock.value(), 'sender': rank}
			comm.send(reply, dest=msg['sender'])
		
		elif msg['type'] == 'mutex_lock':
			mutexes[msg['tag']].on_request(msg)
		
		elif msg['type'] == 'mutex_reply':
			mutexes[msg['tag']].on_reply()

def process_conditional_message(msg):
	with condvar_lock:
		if msg['tag'] in condvars:
			if msg['type'] == 'conditional_notify':
				condvars[msg['tag']].on_notify()



def finalize():
	multicast({'type': 'exit', 'timestamp': clock.value()})


t = threading.Thread(target=__receive_thread)
t.start()
