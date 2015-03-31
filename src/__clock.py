from threading import Lock

class Clock:
	"""Lamport logical clock"""

	def __init__(self, start=0, delta = 1):
		self.clk = start
		self.delta = delta
		self.mutex = Lock()
	
	def increase(self, external_value = 0):
		""" gets max from current value and external_value (default 0) and increases by delta  
		"""

		with self.mutex:
			self.clk = max(self.clk, external_value) + self.delta

	def value(self):
		""" returns actual value of clock """

		with self.mutex:
			value = self.clk
		
		return value



if __name__ == '__main__':
	"""tests for Clock class"""

	clk = Clock()
	clk.increase()
	assert clk.value() == 1, "fail"

	clk.increase(3)
	assert clk.value() == 4 , "fail"