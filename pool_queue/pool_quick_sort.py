'''
multiprocessing for quick-sort 

'''

import os
import multiprocessing
from multiprocessing import Pool, JoinableQueue, Array
import random
import traceback

N = 2
QUE_LEN = 5

nums = Array('i', [random.randint(0, 100) for i in range(10)])
task_que = JoinableQueue(QUE_LEN)

class QuickSortTask():
    def __init__(self, left, right):
        self.left = left
        self.right = right
    
    def quick_sort_task(self):
        print 'PID {}: Quick sorting:'.format(os.getpid()), self.left, self.right
        if self.left >= self.right:
            task_que.task_done()
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
        task_que.task_done()
        return 0

def process_que_task():
    while True: # how to quit?
       task = task_que.get(timeout=10)
       result = task.quick_sort_task()
    return 0        
                    
def main():
    print 'Nums:', [n for n in nums]

    # put initial task
    task_que.put(QuickSortTask(0, len(nums)-1)) # put arguments tuple for quick sort
    pool = Pool(processes=N)
    rets = []
    for i in range(N):
        rets.append(pool.apply_async(process_que_task))    
    task_que.join()
    print 'Sorted nums:', [n for n in nums]

if __name__ == '__main__':
    main()


