from monitor import *
import time

mutex = Mutex('m')

mutex.lock()
print rank, 'aaa'
mutex.unlock()

mutex.lock()
print rank, 'bbb'
mutex.unlock()

#####################

# mutex = Mutex('m')
# cv = ConditionalVariable('c')

# mutex.lock()
# if rank != 0:
# 	cv.wait(mutex)
# 	print rank, "test"
# else:
# 	print rank, "test"
# 	time.sleep(5)
# 	cv.notify()

# mutex.unlock()

##################

# obj = {rank: 'a'}
# res = Resource(obj, 'r', "auto")

# with res as r:
# 	r[rank] = 'b'
# 	#print rank, r
# time.sleep(5)
# with res as r:
# 	print rank, r


finalize()
