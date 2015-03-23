from monitor import *

mutex = Mutex()

mutex.lock()

print rank, "dupa"

mutex.unlock()
finalize()
