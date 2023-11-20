### Michael Engel ### 2022-09-17 ### ReduceME.py ###
import multiprocessing as mp
import math
from queue import Empty
import sys
from contextlib import nullcontext

#%% process functions
#%%% Binary Tree # Replace by wrapping M-ary Tree
def _reducefun_BinaryTree(
        queue,
        ID,
        threadcounter,
        counter,
        threshold,
        locky,
        reducer = None,
        ordered = False,
        timeout = 1,
        bequiet = False,
        logger = print
    ):
    while True: 
        with locky:
            if not threadcounter.value<=(threshold+1-counter.value)//2: # condition absolutely crucial at this particular place!!! Maybe switch to end of while loop?
                threadcounter.value = threadcounter.value-1
                if threadcounter.value==0 and not bequiet:
                    logger()
                break
            
        with nullcontext() if not ordered else locky: # use own lock here instead of locky -> implement soon!
            try:
                item1 = queue.get(timeout=timeout) # timeout very important (see merging not locked)
            except Empty:
                continue
                
            try:
                item2 = queue.get(timeout=timeout)
            except Empty:
                queue.put(item1)
                continue
        
        queue.put(item1+item2 if reducer is None else reducer(item1,item2))
        
        with locky:
            counter.value = counter.value+1 # Maybe put in front of merging? That could avoid a few calls to the queue but might be less robust if something unexpected happens while merging.
            if not bequiet:
                logger(f"\r{counter.value}/{threshold} calculations [{counter.value/threshold*100:.2f}%] with {threadcounter.value} threads\t",end="\r",flush=True)

    return 0

#%%% M-ary Tree
def _reducefun_MaryTree(
        queue,
        ID,
        threadcounter,
        counter,
        threshold,
        Nsamples,
        locky,
        m = 2,
        reducer = None,
        ordered = False,
        timeout = 1,
        bequiet = False,
        logger = print
    ):
    while True: 
        # check working conditions
        with locky:
            if not threadcounter.value<=threshold-counter.value-1 and not threshold-counter.value==1:
                threadcounter.value = threadcounter.value-1
                if threadcounter.value==0 and not bequiet:
                    logger()
                break
            
            # determine number of items to get
            if threshold-counter.value>1:
                n = m
            else:
                rest = ((Nsamples-1)/(m-1))%1
                if rest==0:
                    n = m
                else:
                    n = math.ceil(rest*m) # remaining samples
            
        # get items
        with nullcontext() if not ordered else locky: # use own lock here instead of locky -> implement soon!            
            # draw items from queue
            items = []
            conti = False
            for i in range(n):
                try:
                    item = queue.get(timeout=timeout) # timeout very important (see merging not locked)
                except Empty: # handle empty/timeout exception
                    conti = True # continue outer loop as worker did not receive necessary amount of items
                    if items:
                        for item_ in items:
                            queue.put(item_) # return items to queue, if, e.g., another worker took too long and the planned amount of samples has not been available in the queue -> very important to prevent deadlock!
                    break # don't accept None in items
                items.append(item) # append item if successful
                           
            if conti:
                continue
        
        # reduction
        queue.put(sum(items) if reducer is None else reducer(*items))
        
        # adapt working conditions and log
        with locky:
            counter.value = counter.value+1 # Maybe put in front of merging? That could avoid a few calls to the queue but might be less robust if something unexpected happens while merging.
            if not bequiet:
                logger(f"\r{counter.value}/{threshold} calculations [{counter.value/threshold*100:.2f}%] with {threadcounter.value} threads\t", end="\r", flush=True)
    
    return 0

#%% main methods
#%%% Binary Tree
def reduce_BinaryTree(
        samples, 
        reducer = None, 
        ordered = False,
                     
        queue = None, 
        timeout = 1,
                     
        threads = 0, 
        checkthreads = True,
                     
        bequiet = False, 
        logger = print
    ):
    #%%%% check input arguments
    # samples
    if type(samples)==list:
        Nsamples = len(samples)
    elif type(samples)==int and queue!=None:
        Nsamples = samples
    else:
        raise RuntimeError(f"ReduceME.reduce_BinaryTree: Samples of type {type(samples)} not accepted! Samples given have to be a list or an integer denoting the expected number of samples given in a queue (i.e. which could be fed by a producer node)!")
    
    # number of calculations
    threshold = Nsamples-1
    
    # queue
    if queue==None:
        queue = mp.Queue()
        [queue.put(i) for i in samples]
    else:
        queue = queue
    
    # threads
    if threads!=0:
        if checkthreads:
            threads = min(mp.cpu_count(),max(1,min(threshold,threads)))
            
        mainer = sys._getframe(1).f_globals["__name__"]
        if mainer=="__main__":
            pass
        else:
            logger("ReduceME.reduce_BinaryTree: You are not calling this function from your main! Be aware that parallelization from within a spawned subprocess may cause severe resource issues!")
    
    #%%%% query
    counter = mp.Value("i",0)
    locky = mp.Lock()
    if threads>0:
        threadcounter = mp.Value("i",threads)
        try:
            processes = []
            for ID in range(threads):
                processes.append(
                    mp.Process(
                        target = _reducefun_BinaryTree,
                        kwargs = {
                            "queue":queue,
                            "ID":ID,
                            "threadcounter":threadcounter,
                            "counter":counter,
                            "threshold":threshold,
                            "locky":locky,
                            "reducer":reducer,
                            "ordered":ordered,
                            "timeout":timeout,
                            "bequiet":bequiet,
                            "logger":logger
                        }
                    )
                )
            for process in processes:
                process.start()
        finally:
            [process.join() for process in processes]
    elif threads==0:
        threadcounter = mp.Value("i",1)
        _reducefun_BinaryTree(
            **{
                "queue":queue,
                "ID":0,
                "threadcounter":threadcounter,
                "counter":counter,
                "threshold":threshold,
                "locky":locky,
                "reducer":reducer,
                "ordered":ordered,
                "timeout":timeout,
                "bequiet":bequiet,
                "logger":logger
            }
        )
    else:
        raise RuntimeError(f"ReduceME.reduce_BinaryTree: {threads} threads not supported!")
    
    #%%%% return
    if queue.qsize()==1:
        return queue.get(timeout=timeout)
    else:
        logger(f"ReduceME.reduce_BinaryTree: something unexpected happened! Queue has still {queue.qsize()} elements but should have one!")
        return queue

#%%% M-ary Tree
def reduce_MaryTree(
        samples, 
        reducer = None, 
        ordered = False,
        m = 2,
                     
        queue = None, 
        timeout = 1,
                     
        threads = 0, 
        checkthreads = True,
                     
        bequiet = False, 
        logger = print
    ):
    #%%%% check input arguments
    if type(samples)==list:
        Nsamples = len(samples)
    elif type(samples)==int and queue!=None:
        Nsamples = samples
    else:
        raise RuntimeError(f"ReduceME.reduce_MaryTree: Samples of type {type(samples)} not accepted! Samples given have to be a list or an integer denoting the expected number of samples given in a queue (i.e. which could be fed by a producer node)!")
    threshold = math.ceil((Nsamples-1)/(m-1))
    
    if queue==None:
        queue = mp.Queue()
        [queue.put(i) for i in samples]
    else:
        queue = queue
    
    if threads!=0:
        if checkthreads:
            threads = min(mp.cpu_count(),max(1,min(threshold,threads)))
            
        mainer = sys._getframe(1).f_globals["__name__"]
        if mainer=="__main__":
            pass
        else:
            logger("ReduceME.reduce_MaryTree: You are not calling this function from your main! Be aware that parallelization from within a spawned subprocess may cause severe resource issues!")
    
    #%%%% query
    counter = mp.Value("i",0)
    locky = mp.Lock()
    if threads>0:
        threadcounter = mp.Value("i",threads)
        try:
            processes = []
            for ID in range(threads):
                processes.append(
                    mp.Process(
                        target = _reducefun_MaryTree,
                        kwargs = {
                            "queue":queue,
                            "ID":ID,
                            "threadcounter":threadcounter,
                            "counter":counter,
                            "threshold":threshold,
                            "Nsamples":Nsamples,
                            "m":m,
                            "locky":locky,
                            "reducer":reducer,
                            "ordered":ordered,
                            "timeout":timeout,
                            "bequiet":bequiet,
                            "logger":logger
                        }
                    )
                )
            for process in processes:
                process.start()
        finally:
            [process.join() for process in processes]
    elif threads==0:
        threadcounter = mp.Value("i",1)
        _reducefun_MaryTree(
            **{
                "queue":queue,
                "ID":0,
                "threadcounter":threadcounter,
                "counter":counter,
                "threshold":threshold,
                "Nsamples":Nsamples,
                "m":m,
                "locky":locky,
                "reducer":reducer,
                "ordered":ordered,
                "timeout":timeout,
                "bequiet":bequiet,
                "logger":logger
            }
        )
    else:
        raise RuntimeError(f"ReduceME.reduce_MaryTree: {threads} threads not supported!")
    
    #%%%% return
    if queue.qsize()==1:
        return queue.get(timeout=timeout)
    else:
        logger(f"ReduceME.reduce_MaryTree: something unexpected happened! Queue has still {queue.qsize()} elements but should have one!")
        return queue
