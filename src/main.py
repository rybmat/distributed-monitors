from monitor import *

mutex = Mutex('aaaa')

mutex.lock()
print rank, "dupa"
mutex.unlock()

