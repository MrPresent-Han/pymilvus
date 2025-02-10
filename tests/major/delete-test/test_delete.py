from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import random
import numpy as np
#from constant import IP, dim, nb, batch_num, collection_name, random_base, random_idx_range, flush_batch_num
import time
import secrets


IP="127.0.0.1"
collection_name="test_major_delete"
connections.connect("default", host=IP, port="19530")
collection = Collection(collection_name)

#res = collection.delete("varchar_1=='f8d5a851-0fc1-4f84-808c-450721a-23'")
#print(f"delete res1:{res}")

res = collection.delete("varchar_1=='f8d5a851-0fc1-4f84-808c-450721a-24'")
print(f"delete res2:{res}")
