from multiprocessing import Process, Queue
import math

def g(x):
	return (x, x*x)

def multiprocess(nums, nprocs):
    def worker(nums, out_q):
        """ The worker function, invoked in a process. 'nums' is a
            list of numbers to factor. The results are placed in
            a dictionary that's pushed to a queue.
        """
        outdict = {}
        for n in nums:
	    print "Processing " + str(n) + " now" #debug
            outdict[n] = g(n)
        out_q.put(outdict)

    # Each process will get 'chunksize' nums and a queue to put his out
    # dict into
    out_q = Queue()
    chunksize = int(math.ceil(len(nums) / float(nprocs)))
    procs = []

    for i in range(nprocs):
        p = Process(
                target=worker,
                args=(nums[chunksize * i:chunksize * (i + 1)],
                      out_q))
        procs.append(p)
        p.start()
    
    # Wait for all worker processes to finish
    for p in procs:
        p.join()

    # Collect all results into a single result dict. We know how many dicts
    # with results to expect.
    resultdict = {}
    for i in range(nprocs):
	print "Getting results from process " + str(i)
        resultdict.update(out_q.get())


    return resultdict

derp = multiprocess(range(100),8)
print str(derp)
