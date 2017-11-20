'''
################################## server.py #############################
# 
################################## server.py #############################
'''
import time
import grpc
import datastore_pb2
import datastore_pb2_grpc
import rocksdb
import uuid
import random
import sys
from concurrent import futures
from queue import Queue
_ONE_DAY_IN_SECONDS = 60 * 60 * 24

db = rocksdb.DB("master.db", rocksdb.Options(create_if_missing=True))

class MyDatastoreServicer(datastore_pb2.DatastoreServicer):
    '''
    '''

    def __init__(self):
        '''
        '''
        # TODO
        print("init")

    def Register(self, request, context):
        '''
        Client registers and server reads operations from queue and returns to client
        '''
        # TODO
        print("Follower Node Registered")

        while True:
            while not q.empty():
                    yield q.get() 


#Queue that stores operations on MasterDB
q = Queue()

#Decorators for each operation
def InsertIntoFollower(func):
    def wrapper(*args,**kwargs):
        q.put(datastore_pb2.Response(op_type="insert",key=args[0],value=args[1]))
        print("Sent Insert Operation to Follower")    
        func(*args,**kwargs)
    return wrapper

def UpdateAtFollower(func):
    def wrapper(*args,**kwargs):
        q.put(datastore_pb2.Response(op_type="update",key=args[0],value=args[1]))
        print("Sent Update Operation to Follower")    
        func(*args,**kwargs)
    return wrapper

def DeleteFromFollower(func):
    def wrapper(*args,**kwargs):
        q.put(datastore_pb2.Response(op_type="delete",key=args[0],value=0))
        print("Sent Delete Operation to Follower")    
        func(*args,**kwargs)
    return wrapper

#Operations on Master DB
@InsertIntoFollower
def InsertIntoMasterDb(key,value):
    print("Inserted Into Master Db\n")
    db.put(str(key).encode(),str(value).encode())

@UpdateAtFollower
def UpdateAtMasterDb(key,value):
    print("Updated At Master Db\n")
    db.put(str(key).encode(),str(value).encode())

@DeleteFromFollower
def DeleteFromMasterDb(key):
    print("Deleted From Master Db\n")
    db.delete(str(key).encode())

def run(host, port):
    '''
    Run the GRPC server
    '''
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    datastore_pb2_grpc.add_DatastoreServicer_to_server(MyDatastoreServicer(), server)
    server.add_insecure_port('%s:%d' % (host, port))
    server.start()

    print("Master Node started at...%d" % port)

    try:
        while True:
            time.sleep(3)

            #Generating random keys and values
            key = random.randint(1,20)
            value = random.randint(100,200)

            #Insert Operation
            print("Insert Operation At Master -> key:{0}, value:{1}".format(str(key),str(value)))
            InsertIntoMasterDb(key,value)

            #Update Operation
            print("Update Operation At Master -> key:{0}, value:{1}".format(str(key),str(value)))
            UpdateAtMasterDb(key,value)
            
            #Delete Operation
            print("Delete Operation At Follower -> key:{0}".format(str(key)))
            DeleteFromMasterDb(key)
            
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    run('0.0.0.0', 3000)
