from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import random
from constant import dim, nb
import time
from constant import IP

connections.connect("default", host=IP, port="19530")

collection_name = 'test_skip_index_2kw'
collection = Collection(collection_name)
collection.release()

print("release done")
