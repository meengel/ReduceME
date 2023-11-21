### Michael Engel ### 2022-09-17 ### ReduceME.py ###
import multiprocessing as mp
import math
from queue import Empty
import sys
from contextlib import nullcontext

#%% process functions
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
            if (not threadcounter.value<=threshold-counter.value-1 and not threshold-counter.value==1) or (threshold-counter.value==1 and threadcounter.value>1):
                threadcounter.value = threadcounter.value-1
                if threadcounter.value==0 and not bequiet:
                    logger("", flush=True)
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

#%%% M-ary Tree MAP
def _reducefun_MaryTree_MAP(
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
            if (not threadcounter.value<=threshold-counter.value-1 and not threshold-counter.value==1) or (threshold-counter.value==1 and threadcounter.value>1):
                threadcounter.value = threadcounter.value-1
                return {"ID":ID, "end":False, "result":threshold-counter.value}
            
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
            counter.value = counter.value+1
            if not bequiet:
                logger(f"\r{counter.value}/{threshold} calculations [{counter.value/threshold*100:.2f}%] with {threadcounter.value} threads\t", end="\r", flush=True)
                
            if threshold-counter.value==0:
                if not bequiet:
                    logger("", flush=True)
                return {"ID":ID, "end":True, "result":queue.get()}
    
    return 0

def _reducefun_MaryTree_MAP_WRAP(kwargs):
    kwargs.update({"queue":_reducefun_MaryTree_MAP_WRAP.q,
                   "threadcounter":_reducefun_MaryTree_MAP_WRAP.t,
                   "counter":_reducefun_MaryTree_MAP_WRAP.c,
                   "locky":_reducefun_MaryTree_MAP_WRAP.l,})
    return _reducefun_MaryTree_MAP(**kwargs)

def _reducefun_MaryTree_MAP_WRAP_init(q, t, c, l):
    _reducefun_MaryTree_MAP_WRAP.q = q
    _reducefun_MaryTree_MAP_WRAP.t = t
    _reducefun_MaryTree_MAP_WRAP.c = c
    _reducefun_MaryTree_MAP_WRAP.l = l

#%% main methods
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
            logger("ReduceME.reduce_MaryTree: joining processes!")
            [process.join() for process in processes]
            logger("ReduceME.reduce_MaryTree: joined processes!")
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
        logger(f"ReduceME.reduce_MaryTree: something unexpected happened! Queue has {queue.qsize()} elements but should have one!")
        return queue
    
#%%% M-ary Tree MAP
def reduce_MaryTree_MAP(
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
        raise RuntimeError(f"ReduceME.reduce_MaryTree_MAP: Samples of type {type(samples)} not accepted! Samples given have to be a list or an integer denoting the expected number of samples given in a queue (i.e. which could be fed by a producer node)!")
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
            logger("ReduceME.reduce_MaryTree_MAP: You are not calling this function from your main! Be aware that parallelization from within a spawned subprocess may cause severe resource issues!")
    
    #%%%% query
    counter = mp.Value("i",0)
    locky = mp.Lock()
    if threads>0:
        threadcounter = mp.Value("i",threads)
        kwargslist = []
        for ID in range(threads):
            kwargslist.append({
                # "queue":queue,
                "ID":ID,
                # "threadcounter":threadcounter,
                # "counter":counter,
                "threshold":threshold,
                "Nsamples":Nsamples,
                "m":m,
                # "locky":locky,
                "reducer":reducer,
                "ordered":ordered,
                "timeout":timeout,
                "bequiet":bequiet,
                "logger":logger
            })
            
        with mp.Pool(threads, _reducefun_MaryTree_MAP_WRAP_init, [queue, threadcounter, counter, locky]) as executor:
            results = executor.map(_reducefun_MaryTree_MAP_WRAP, kwargslist)
            
        for res in results:
            if res["end"]:
                result = res
                break
            
    elif threads==0:
        threadcounter = mp.Value("i",1)
        result = _reducefun_MaryTree_MAP(
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
        raise RuntimeError(f"ReduceME.reduce_MaryTree_MAP: {threads} threads not supported!")
    
    #%%%% return
    return result["result"]
