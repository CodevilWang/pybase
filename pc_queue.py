#!/usr/bin/env python
#encoding=utf8
'''
    python的生产者消费者队列
    TODO:
        1) 消费者可以是一个object
'''
import sys
import logging
import time
from multiprocessing import Process, Queue

class ProduceComsumerQueue(object):
    def __init__(self, consumer_num, c_func, producer_num = 0, p_func = None, type_checker = None):
        self._q = Queue(10)
        self._p_num = producer_num
        self._c_num = consumer_num
        self._p_func = p_func
        self._c_func = c_func
        self._p_pool = []
        self._c_pool = []
        self._t_check = type_checker

    def get_queue(self):
        return self._q

    def en_queue(self, obj, timeout = None):
        if self._t_check and type(obj) != self._t_check:
            print "here0"
            logging.info("enqueue typecheck failed[{}/{}]".format(type(obj), self._t_check))
            return None
        try:
            if timeout:
                self._q.put(obj, True, timeout)
            else:
                self._q.put(obj, False)
        except Exception as e:
            logging.error("en_queue get exception[{}]".format(repr(e)))
            return None
        return True

    # TODO(codevil) 使用context实现
    def c_wrapper(self):
        try:
            while True:
                obj = self._q.get(True, None)
                # print "here"
                self._c_func(obj)
        except Exception as e:
            logging.error("c_wrapper exception[{}]".format(repr(e)))
            return None

    def init(self):
        if self._p_num > 0 and self._p_func:
            for i in xrange(0, self._p_num):
                p = Process(target = self._p_func, arg = self._q)
                self._p_pool.append(p)
        if self._c_num > 0 and self._c_func:
            for i in xrange(0, self._c_num):
                p = Process(target = self.c_wrapper)
                self._c_pool.append(p)


    def start(self):
        for x in self._p_pool:
            x.start()
        for x in self._c_pool:
            x.start()

    def join(self):
        for x in self._p_pool:
            x.join()

        for x in self._c_pool:
            x.join()

def print_int(value):
    # with open("~/PycharmProjects/pybase/1.txt", "w+") as f:
    sys.stderr.write("in print_int [{}]\n".format(value))

if __name__ == "__main__":
    p = ProduceComsumerQueue(consumer_num=4, c_func = print_int)
    p.init()
    p.start()
    p.en_queue(10)
    p.en_queue(11)
    time.sleep(3)
    p.en_queue(13)
    p.en_queue(14)
    time.sleep(3)
    p.en_queue(15)
    p.en_queue(16)
    p.join()



