'''
Created on Oct 25, 2016

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
    for i in range(0, 10000, 1000):
        print 'Posting task..'
        task_que.put((i, i+1000))
        print 'Posted task..{}'.format(i)
    print 'Finished posting tasks..'
    no_more_task.set()

def process_task():
    while True:
        global_lock.acquire()
        print 'PID {} aquired lock.'.format(os.getpid())
        if not task_que.empty(): 
            task = task_que.get()
            global_lock.release()
        else: 
            # NOTE: There is a slight chance that no_more_task is set after posting last task in the queue while Queue.empty() returns True, ignoring the last posted task, due to delays between Queue.put() and Queue.empty().
            
            print 'No more task?', no_more_task.is_set()
            if no_more_task.is_set():
                print 'PID {} finishes processing'.format(os.getpid())
                global_lock.release()
                return
            else:
                global_lock.release()
                continue
        print 'PID {}: Processing task'.format(os.getpid())
        calculator(task[0], task[1])

def main():
    poster = Process(target=post_tasks)
    poster.start()
    print 'Posting started..'
    
    pool = Pool(processes=N)    
    for i in range(N):
        print 'Starting working process:', i
        pool.apply_async(process_task)
            
    pool.close()
    pool.join()
    
if __name__ == '__main__':
    main() 
