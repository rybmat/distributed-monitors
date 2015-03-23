from monitor import *

mutex = Mutex()
m2 = Mutex()
m3 = Mutex()

mutex.lock()

print rank, "dupa"

mutex.unlock()

