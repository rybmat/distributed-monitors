import threading

from mpi4py import MPI
from pprint import pprint

from __clock import Clock

comm_lock = threading.Lock()
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

clock = Clock()
mutexes_lock = threading.Lock()
mutexes = {}
mutex_num = 0

class Mutex:
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

		

	def __del__(self):
		with mutexes_lock:
			del mutexes[self.tag]


	def lock(self):
		self.interested = True
		clock.increase()
		
		data = {'type': 'mutex_lock', 'tag': self.tag, 'timestamp': clock.value(), 'sender': rank}
		with comm_lock:
			for i in range(comm.Get_size()):
				if i != rank:
					comm.send(data, dest=i)

		self.replies_condition.acquire()
		self.replies_condition.wait()
		self.replies_condition.release()


	def unlock(self):
		with self.deffered_lock:
			self.interested = False	
			data = {'type': 'mutex_reply', 'tag': self.tag, 'timestamp': clock.value(), 'sender': rank}
			
			with comm_lock:
				for d in self.deffered:
					comm.send(data, dest=d)
			self.deffered = []

		with self.replies_number_lock:
			self.replies_number = 0

		clock.increase()


	def on_request(self, request):
		if (not self.interested) or (clock.value() > request['timestamp']):
			data = {'type': 'mutex_reply', 'tag': self.tag, 'timestamp': clock.value(), 'sender': rank}
			with comm_lock:
				comm.send(data, dest=request['sender'])
		else:
			with self.deffered_lock:
				self.deffered.append(request['sender'])


	def on_reply(self):
		with self.replies_number_lock:
			self.replies_number += 1
			if self.replies_number == (comm.Get_size() - 1):
				self.replies_condition.acquire()
				self.replies_condition.notify()
				self.replies_condition.release()


__rcv_thread_run = True

def __receive_thread():
	while __rcv_thread_run:
		data = comm.recv(source=MPI.ANY_SOURCE)
		clock.increase(data['timestamp'])
		#print "recv", rank, data
		
		with mutexes_lock:
			if not (data['tag'] in mutexes):
				reply = {'type': 'mutex_reply', 'tag': data['tag'], 'timestamp': clock.value(), 'sender': rank}
				with comm_lock:
					comm.send(reply, dest=data['sender'])
			elif data['type'] == 'mutex_lock':
				mutexes[data['tag']].on_request(data)
			elif data['type'] == 'mutex_reply':
				mutexes[data['tag']].on_reply()

t = threading.Thread(target=__receive_thread)
t.start()

