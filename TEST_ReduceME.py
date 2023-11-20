### Michael Engel ### 2023-11-20 ### TEST_ReduceME.py ###
from ReduceME import reduce_BinaryTree, reduce_MaryTree
import time

#%% dummy methods
def dummymerger_binary(item1, item2):
    time.sleep(0.1)
    return item1+item2

def dummymerger_mary(*items):
    time.sleep(0.1)
    return sum(items)

#%% main
if __name__=="__main__":
    import time
    import numpy as np
    #%%% choose
    Nsamples = 123
    samples = [1]*Nsamples
    threads = 12
    timeout = 0.2
    ordered = False # increases coordination overhead of processes if True
    m = 4 # m-ary case
    
    #%%% binary
    #%%%% query
    #%%%%% multiprocessed
    print("Start multiprocessed binary Reduction!")
    start_multi_binary = time.time()
    result_multi_binary = reduce_BinaryTree(
        samples,
        reducer = dummymerger_binary,
        ordered = ordered,
        queue = None,
        timeout = timeout,
        threads = threads,
        checkthreads = True,
        bequiet = False,
        logger = print
    )
    time_multi_binary = time.time()-start_multi_binary
    
    #%%%%% single process
    print("Start single-process binary Reduction!")
    start_single_binary = time.time()
    result_single_binary = reduce_BinaryTree(
        samples, 
        reducer = dummymerger_binary, 
        ordered = ordered,
        queue = None,
        timeout = timeout,
        threads = 0, 
        checkthreads = True,
        bequiet = False,
        logger = print
    )
    time_single_binary = time.time()-start_single_binary
    
    #%%%% results
    print("\nBinary Reducer")
    print(f"Reference:\t\t\t{np.sum(samples)}")
    print(f"Result Multi:\t\t{result_multi_binary}")
    print(f"Result Single:\t\t{result_single_binary}")
    print()
    print(f"Time Multi:\t\t{time_multi_binary}s")
    print(f"Time Single:\t\t{time_single_binary}s")
    print()
    
    #%%% m-ary
    #%%%% query
    #%%%%% multiprocessed
    print(f"Start multiprocessed {m}-ary Reduction!")
    start_multi_mary = time.time()
    result_multi_mary = reduce_MaryTree(
        samples,
        reducer = dummymerger_mary,
        ordered = ordered,
        m = m,
        queue = None,
        timeout = timeout,
        threads = threads,
        checkthreads = True,
        bequiet = False,
        logger = print
    )
    time_multi_mary = time.time()-start_multi_mary
    
    #%%%%% single process
    print(f"Start single-process {m}-ary Reduction!")
    start_single_mary = time.time()
    result_single_mary = reduce_MaryTree(
        samples, 
        reducer = dummymerger_mary, 
        ordered = ordered,
        m = m,
        queue = None,
        timeout = timeout,
        threads = 0, 
        checkthreads = True,
        bequiet = False,
        logger = print
    )
    time_single_mary = time.time()-start_single_mary
    
    #%%%% results
    print(f"\n{m}-ary Reducer")
    print(f"Reference:\t\t\t{np.sum(samples)}")
    print(f"Result Multi:\t\t{result_multi_mary}")
    print(f"Result Single:\t\t{result_single_mary}")
    print()
    print(f"Time Multi:\t\t{time_multi_mary}s")
    print(f"Time Single:\t\t{time_single_mary}s")
    print()