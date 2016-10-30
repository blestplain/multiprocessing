'''
Created on Oct 25, 2016

apply context managers for lock
always release lock 

@author: Jun
'''

from multiprocessing import Process, Pool, Event, Lock, Value
from multiprocessing.queues import Queue
import os

N = 2
task_que = Queue(5)
no_more_task = Event()
global_lock = Lock()

def calculator(x, y):
    s = 0
    print 'Computing:', x, y
    for i in range(x,y,1):
        s += 1
    print 'Local sum:', s
    return s        


def post_tasks():
    try:
        for i in range(0, 10000, 1000):
            print 'Posting task..'
            task_que.put((i, i+1000))
            print 'Posted task..{}'.format(i)
        print 'Finished posting tasks..'
    finally:
        no_more_task.set()

def process_task():
    s = 0
    while True:
        with global_lock:
            print 'PID {} aquired lock.'.format(os.getpid())
            if not task_que.empty(): 
                task = task_que.get()
            else:
                print 'No more task?', no_more_task.is_set()
                if no_more_task.is_set():
                    print 'PID {} finishes processing'.format(os.getpid())
                    return s
                else:
                    continue
        print 'PID {}: Processing task'.format(os.getpid())
        s += calculator(task[0], task[1])

def main():
    poster = Process(target=post_tasks)
    poster.start()
    print 'Posting started..'
    
    pool = Pool(processes=N)
    results =[]    
    for i in range(N):
        print 'Starting working process:', i
        results.append(pool.apply_async(process_task))
    
    total = 0
    for r in results:
        total += r.get()
    print 'Total:', total
    
    pool.close()
    pool.join()
    
if __name__ == '__main__':
    main() 
