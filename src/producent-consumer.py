from monitor import Mutex, ConditionalVariable, Resource, rank, finalize


class Buffer:
	def __init__(self):
		self.buff_size = 10
		self.push_pos = 0
		self.pop_pos = 0
		self.buff = [None for i in range(self.buff_size)]

	def pop(self):
		result = None
		if self.buff[self.pop_pos] is not None:
			result = self.buff[self.pop_pos]
			self.buff[self.pop_pos] = None
			self.pop_pos = (self.pop_pos + 1) % self.buff_size
		return result

	def push(self, value):
		if self.buff[self.push_pos] is None:
			self.buff[self.push_pos] = value
			self.push_pos = (self.pop_pos + 1) % self.buff_size
			return True
		else:
			return False


class BufferMonitor:
	def __init__(self):
		
		self.mutex = Mutex('mutex')
		self.buff = Resource(Buffer(), tag='buffer', mutex=None, master=0)
		self.consumer_cond = ConditionalVariable("consumer_cond")
		self.producent_cond = ConditionalVariable("producent_cond")

	def pop(self):
		self.mutex.lock()
		result = None

		with self.buff as b:
			result = b.pop()

		while result is None:
			self.consumer_cond.wait(self.mutex)

			with self.buff as b:
				result = b.pop()


		self.producent_cond.notify()
		self.mutex.unlock()

		return result

	def push(self, value):
		self.mutex.lock()

		with self.buff as b:
			result = b.push(value)

		while not result:
			self.producent_cond.wait(self.mutex)

			with self.buff as b:
				result = b.push(value)

		self.consumer_cond.notify()
		self.mutex.unlock()


def producent():
	buff = BufferMonitor()
	for i in range(100):
		buff.push(i)

def consumer():
	buff = BufferMonitor()
	for i in range(100):
		print buff.pop()

if __name__ == '__main__':
	if rank % 2 == 0:
		producent()
	else:
		consumer()

	finalize()
