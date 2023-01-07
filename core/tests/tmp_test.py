import multiprocessing
import time

data = (
    ['a', '2'], ['b', '4'], ['c', '6'], ['d', '8'],
    ['e', '1'], ['f', '3'], ['g', '5'], ['h', '7']
)

def mp_worker(inputs, the_time=1000):
    print(" Processs %s\tWaiting %s seconds" % (inputs, the_time))
    time.sleep(int(the_time))
    print(" Process %s\tDONE" % inputs)

def mp_handler():
    multiprocessing.set_start_method('spawn')
    p = multiprocessing.Pool(2)
    p.map(mp_worker, data)

if __name__ == '__main__':
    mp_handler()
