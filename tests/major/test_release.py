from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import random
from constant import dim, nb, collection_name
import time
from constant import IP

connections.connect("default", host=IP, port="19530")

collection = Collection(collection_name)
collection.release()

print("release done")
