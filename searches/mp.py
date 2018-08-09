import os
import redis
from frame_partial_reader.frame_partial_reader import Frame_partial_reader
import json

from multiprocessing import Process, Manager, Queue


def doubler(number):
    #todo подключение к redis, чтение
    r1 = redis.StrictRedis(host='localhost', port=6379, db=0)
    r2 = redis.StrictRedis(host='localhost', port=6379, db=1)
    r3 = redis.StrictRedis(host='localhost', port=6379, db=2)
    result = number * 2
    proc = os.getpid()
    #print('{0} doubled to {1} by process id: {2}'.format(
        #number, result, proc))
    #d[proc] = result+d["shared_dictionary"].t
    #print(d)

class Test():
    t = 1
if __name__ == '__main__':

    numbers = [5, 10, 15, 20, 25]
    procs = []
    #manager = Manager()  # create only 1 mgr
    #d = manager.dict()  # create only 1 dict
    #d["shared_dictionary"] = Test()
    #todo взять и наполнить бд из файла или взять и создать с нуля
    #todo redis наполнить значениями по ключу артикул
    pr = Frame_partial_reader()
    pr.read("parts_with_sg_mg.csv")

    goods_classifier = pr.parse_result
    r1 = redis.StrictRedis(host='localhost', port=6379, db=0)
    for key, value in goods_classifier.items():
        r1.set(key,json.dumps(value))

    # self.goods_classifier = pr.nsi_dict#это для тестирования!

    brands_dict = pr.brands
    r2 = redis.StrictRedis(host='localhost', port=6379, db=1)
    for key, value in brands_dict.items():
        r2.set(key, json.dumps(value))

    groups_dict = pr.groups
    r3 = redis.StrictRedis(host='localhost', port=6379, db=2)
    for key, value in groups_dict.items():
        r3.set(key, json.dumps(value))



    for index, number in enumerate(numbers):
        proc = Process(target=doubler, args=(number,))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()

    #print(d)
    #todo записать бд в файл - можно своими средствами, просто пройдясь по ней и сериализуя
    #todo освободить память redis - flushall ключи из бд
    r1.flushall()
    r2.flushall()
    r3.flushall()

    a = 1




