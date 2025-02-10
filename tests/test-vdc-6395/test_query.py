from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import random
import numpy as np
#from constant import IP, dim, nb, batch_num, collection_name, random_base, random_idx_range, flush_batch_num
import time
import secrets


IP = "127.0.0.1"
collection_name = "test_vdc_6395"
connections.connect("default", host=IP, port="19530")
collection = Collection(collection_name)
collection.load()
res = collection.query(
    expr="c_int64>0",
    #limit=500,
    output_fields=["float_vector"],
)

print(f"res:{res}")
