from monitor import *
import time

# mutex = Mutex('m')

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

# mutex = Mutex('m')
# cv = ConditionalVariable('c')

# if rank != 0:
# 	mutex.lock()
# 	cv.wait(mutex)
# 	print rank, "after wait"
# else:
# 	time.sleep(5)
# 	mutex.lock()
# 	print rank, "no wait"
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
