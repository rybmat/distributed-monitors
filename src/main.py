from monitor import *

mutex = Mutex()

mutex.lock()

print rank, "test"

mutex.unlock()
finalize()
