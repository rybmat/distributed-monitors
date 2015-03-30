from threading import Lock

class Clock:
	"""Lamport logical clock"""

	def __init__(self, size, rank, start=0, delta = 1):
		self.clk = [start for i in range(size)]
		self.delta = delta
		self.rank = rank
		self.mutex = Lock()
	
	def increase(self):
		""" gets max from current value and external_value (default 0) and increases by delta  
		"""
		with self.mutex:
			self.clk[self.rank] += self.delta

	def merge(self, external_clk):
		with self.mutex:
			self.clk = [max(self.clk[i], external_clk[i]) for i in range(len(self.clk))]

	def value(self):
		""" returns actual value of clock """

		with self.mutex:
			value = self.clk
		
		return value



if __name__ == '__main__':
	"""tests for Clock class"""

	clk = Clock(3,1)
	assert clk.value() == [0,0,0], "fail"

	clk.increase()
	assert clk.value() == [0,1,0], "fail"

	clk.merge([0,0,1])
	assert clk.value() == [0,1,1], "fail"
	

