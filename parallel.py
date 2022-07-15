from multiprocessing import Process, Queue
import pandas as pd
def multiply(a,b,que): #add a argument to function for assigning a queue
    que.put(a*b) #we're putting return value into queue


# if __name__ == '__main__':
#     queue1 = Queue()
#     queue2 = Queue()#create a queue object
#     p1 = Process(target=multiply, args=(5, 4, queue1)) #we're setting 3rd argument to queue1
#     p1.start()
#     p2 = Process(target=multiply, args=(7, 4, queue2))
#     p2.start()
#     res1 = queue1.get()
#     res2 = queue2.get()
#     print(res1)
#     print(res2)#and we're getting return value: 20
#     p1.join()
#     p2.join()
#     print("ok.")

data_with_duplicates_path + '/' + 'axonQA_{}_with_duplicates.csv'.format(filedate)