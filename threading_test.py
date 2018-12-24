import _thread
import numpy as np
import time

exitmute = [_thread.allocate_lock() for i in range(4) ]


def child(id,x,y,z,n1,n2):
    z[n1:n2+1] = x[n1:n2+1] * y[n1:n2+1]
    exitmute[id].acquire()


def parent():
    thread_num = range(4)
    a = np.arange(1, 100000001)
    b = 1/a
    c = np.zeros(100000000)
    z1 = 0
    z2 = 24999999
    t=time.time()
    for i in thread_num:
        _thread.start_new_thread(child, (i, a, b, c, z1, z2))
        z1 += 25000000
        z2 += 25000000
    for mute in exitmute:
        while not mute.locked():
            pass

    print(c.sum())
    print(time.time()-t)



parent()
