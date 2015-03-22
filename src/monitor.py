import threading

from mpi4py import MPI
from pprint import pprint

from __clock import Clock

print "th", MPI.Query_thread()
comm_lock = threading.Lock()
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

clock = Clock()

mutexes_lock = threading.Lock()
mutexes = {}

class Mutex:
	def __init__(self, tag):
		self.interested = False
		self.replies_condition = threading.Condition()
		self.replies_number_lock = threading.Lock()
		self.replies_number = 0
		self.tag = tag
		
		self.deffered_lock = threading.Lock()
		self.deffered = []

		with mutexes_lock:
			mutexes[tag] = self
		

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


	def unlock(self):
		self.interested = False
		
		with self.deffered_lock:	
			data = {'type': 'mutex_reply', 'tag': self.tag, 'timestamp': clock.value(), 'sender': rank}
			
			with comm_lock:
				for d in self.deffered:
					comm.send(data, dest=d)
			self.deffered = []

		with self.replies_number_lock:
			self.replies_number = 0

		clock.increase()


	def on_request(self, request):
		print rank, "request", request
		if not self.interested or request['timestamp'] < clock.value():
			data = {'type': 'mutex_reply', 'tag': self.tag, 'timestamp': clock.value(), 'sender': rank}
			with comm_lock:
				comm.send(data, dest=request['sender'])
			print rank, "sending reply", request['sender']
		else:
			print rank, "deffered"
			with self.deffered_lock.acquire():
				self.deffered.append(request['sender'])
		print rank, "out of req"


	def on_reply(self):
		print "reply"
		with self.replies_number_lock:
			print "reply2"
			self.replies_number += 1
			if self.replies_number == (comm.Get_size() - 1):
				self.replies_condition.notify()


__rcv_thread_run = True

def __receive_thread():
	while __rcv_thread_run:
		data = comm.recv()

		# # debug
		# print rank, data
		# continue

		clock.increase(data['timestamp'])
		print "recv", rank, data
		with mutexes_lock:
			#print "inside lock"
			if not (data['tag'] in mutexes):
				#print "sending reply"
				reply = {'type': 'mutex_reply', 'tag': data['tag'], 'timestamp': clock.value(), 'sender': rank}
				with comm_lock:
					comm.send(reply, dest=data['sender'])
			elif data['type'] == 'mutex_lock':
				#print "invoking req"
				mutexes[data['tag']].on_request(data)
			elif data['type'] == 'mutex_reply':
				#print "invoking reply"
				mutexes[data['tag']].on_reply()

t = threading.Thread(target=__receive_thread)
t.start()


