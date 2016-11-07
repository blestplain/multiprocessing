'''
multiprocessing for quick-sort 

Number of valid tasks is always the number of elements to be sorted.

The first process reaches the number of total elements uses poison pill to notify other processes that all tasks are done.

Task queue length needs to be suitable. One finished task will post two tasks to the queue.

'''

import os
import multiprocessing
from multiprocessing import Pool, JoinableQueue, Array, Value
import random
import traceback

N = 4
QUE_LEN = -1

NUM_ELEMENTS = 100
nums = Array('i', [random.randint(0, 100) for i in range(NUM_ELEMENTS)])
task_que = JoinableQueue(QUE_LEN)
finished_tasks = Value('i', 0) 

class EndOfQueue(): # poison pill
    def __init__(self):
        pass

class QuickSortTask():
    def __init__(self, left, right):
        self.left = left
        self.right = right
    
    def quick_sort_task(self):
        print 'PID {}: Quick sorting:'.format(os.getpid()), self.left, self.right
        if self.left >= self.right:
            return 1
        
        l = self.left + 1
        r = self.right
        while True:
            while l <= self.right and nums[l] < nums[self.left]:
                l+=1
            while r > self.left and nums[r]>= nums[self.left]:
                r-=1
            if l < r:
                tmp = nums[l]
                nums[l] = nums[r]
                nums[r] = tmp
            else:
                break
        tmp = nums[self.left]
        nums[self.left] = nums[r]
        nums[r] = tmp
        if self.left <= r-1:
            task_que.put(QuickSortTask(self.left, r-1))
            print 'PID {}: put task'.format(os.getpid()), self.left, r-1
        if l <= self.right:
            task_que.put(QuickSortTask(l, self.right))
            print 'PID {}: put task'.format(os.getpid()), l, self.right
        return 0

def process_que_task():
    while True:
        task = task_que.get(timeout=10)
        if isinstance(task, EndOfQueue):
            break
        result = task.quick_sort_task()
        task_que.task_done()
        finished_tasks.value += 1
        print 'Finished tasks:', finished_tasks.value
        if finished_tasks.value == NUM_ELEMENTS:
            for i in range(N-1): # put N-1 poison pills for other processes
                task_que.put(EndOfQueue())
            break
    return 0        
                    
def main():
    print 'Nums:', [n for n in nums]

    # put initial task
    task_que.put(QuickSortTask(0, len(nums)-1)) # put arguments tuple for quick sort
    pool = Pool(processes=N)
    rets = []
    for i in range(N):
        rets.append(pool.apply_async(process_que_task))    
    for r in rets:
        r.get()

    print 'Sorted nums:', [n for n in nums]

if __name__ == '__main__':
    main()


