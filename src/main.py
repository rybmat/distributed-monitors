from monitor import *
import time

mutex = Mutex()
cv = ConditionalVariable()

mutex.lock()
if rank != 0:
	cv.wait(mutex)
	print rank, "test"
else:
	print rank, "test"
	time.sleep(5)
	cv.notify()

mutex.unlock()
finalize()
