import os

from multiprocessing import Process, Manager, Queue


def doubler(number,d):
    """
    A doubling function that can be used by a process
    """
    result = number * 2
    proc = os.getpid()
    #print('{0} doubled to {1} by process id: {2}'.format(
        #number, result, proc))
    d[proc] = result+d["shared_dictionary"].t
    #print(d)

class Test():
    t = 1
if __name__ == '__main__':

    numbers = [5, 10, 15, 20, 25]
    procs = []
    manager = Manager()  # create only 1 mgr
    d = manager.dict()  # create only 1 dict
    d["shared_dictionary"] = Test()


    for index, number in enumerate(numbers):
        proc = Process(target=doubler, args=(number,d))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()

    print(d)

    a = 1




