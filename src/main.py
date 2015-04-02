from monitor import *
import time

# mutex = GetMutex('m')

# mutex.lock()
# print rank, 'aaa'
# mutex.unlock()

# mutex.lock()
# print rank, 'bbb'
# mutex.unlock()

# mutex.lock()
# print rank, 'ccc'
# mutex.unlock()

#####################
# Fix this example
# mutex = Mutex('m')
# cv = ConditionalVariable('c')

# mutex.lock()
# print rank, "inside lock"
# if rank != 0:
# 	cv.wait(mutex)
# 	print rank, "after wait"
# else:
# 	print rank, "no wait"
# 	time.sleep(5)
# 	cv.notify()

# mutex.unlock()

##################

obj = {rank: 'a'}
res = Resource(obj, 'r', "auto")

with res as r:
	r[rank] = 'b'
	#print rank, r
time.sleep(5)
with res as r:
	print rank, r


finalize()
