'''
################################## client.py #############################
# 
################################## client.py #############################
'''
import grpc
import datastore_pb2
import rocksdb
import time

db = rocksdb.DB("follower.db", rocksdb.Options(create_if_missing=True))

class DatastoreClient():
    '''
    '''
    def __init__(self, host='0.0.0.0', port=3000):
        '''
        '''
        self.channel = grpc.insecure_channel('%s:%d' % (host, port))
        self.stub = datastore_pb2.DatastoreStub(self.channel)

    def Register(self):
        '''
        Client connects to the server indefinitely through this method
        '''
        return self.stub.Register(datastore_pb2.Request(key=1))

if __name__ == '__main__':

    client = DatastoreClient()

    print("Slave node started")

    try:
        #Iterating over responses from Server and conducting operations on FollowerDB
        for op in client.Register():
            if op.op_type == "insert":
                print("Insert Operation At Follower -> key:{0}, value:{1}\n".format(str(op.key),str(op.value)))
                db.put(str(op.key).encode(),str(op.value).encode())
            elif op.op_type == "update":
                print("Update Operation At Follower -> key:{0}, value:{1}\n".format(str(op.key),str(op.value)))
                db.put(str(op.key).encode(),str(op.value).encode())
            else:
                print("Delete Operation At Follower -> key:{0}\n".format(str(op.key)))
                db.delete(str(op.key).encode())    
    except KeyboardInterrupt:
            print("Client Exited")
