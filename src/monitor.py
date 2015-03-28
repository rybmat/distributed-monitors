import threading

from mpi4py import MPI
from pprint import pprint

from __clock import Clock

comm_lock = threading.Lock()
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

clock = Clock()

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
		with comm_lock:
			for i in range(comm.Get_size()):
				if i != rank:
					comm.send(data, dest=i)

		self.replies_condition.acquire()
		self.replies_condition.wait()
		self.replies_condition.release()


	def unlock(self):
		if self.interested:
			with self.deffered_lock:
				self.interested = False	
				data = {'type': 'mutex_reply', 'tag': self.tag, 'timestamp': clock.value(), 'sender': rank}
				
				with comm_lock:
					for d in self.deffered:
						comm.send(data, dest=d)
				self.deffered = []

			with self.replies_number_lock:
				self.replies_number = 0

	def on_request(self, request):
		""" action invoked by receiving thread when lock request received
		"""
		if (not self.interested) or (clock.value() > request['timestamp']) or (request['sender'] == rank):
			data = {'type': 'mutex_reply', 'tag': self.tag, 'timestamp': clock.value(), 'sender': rank}
			with comm_lock:
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

		self.conditional = threading.Conditional()
		self.is_waiting = False

	def wait(self, mutex):
		mutex.unlock()
		self.conditional.acquire()
		self.is_waiting = True
		self.conditional.wait()
		self.conditional.release()
		mutex.lock()

	def notify(slef):
		data = {'type': 'conditional_notify', 'tag': self.tag, 'timestamp': clock.value(), 'sender': rank}
		with comm_lock:
			for i in range(comm.Get_size()):
				if i != rank:
					comm.send(data, dest=i)

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
	run = True
	while run:
		data = comm.recv(source=MPI.ANY_SOURCE)
		clock.increase(data['timestamp'])
		#print "recv", rank, clock.value(), data
		
		if data['type'].startswith('mutex'):
			with mutexes_lock:
				if not (data['tag'] in mutexes):
					reply = {'type': 'mutex_reply', 'tag': data['tag'], 'timestamp': clock.value(), 'sender': rank}
					with comm_lock:
						comm.send(reply, dest=data['sender'])
				elif data['type'] == 'mutex_lock':
					mutexes[data['tag']].on_request(data)
				elif data['type'] == 'mutex_reply':
					mutexes[data['tag']].on_reply()
		elif data['type'] == "exit":
			run = False

t = threading.Thread(target=__receive_thread)
t.start()


def finalize():
	comm.send({'type': 'exit', 'timestamp': clock.value()}, dest=rank)
