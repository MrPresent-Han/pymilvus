from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import random
import numpy as np
from constant import IP, dim, nb, batch_num, collection_name, random_base, random_idx_range, flush_batch_num
import time
import secrets

def generate_random_hex_string(length):
    return secrets.token_hex(length // 2)

connections.connect("default", host=IP, port="19530")

clean_exist = True
prepare_data = True

fields = [
    FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True),
    FieldSchema(name="c_int64", dtype=DataType.INT64),
    FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
    FieldSchema(name="str_10", dtype=DataType.VARCHAR, max_length=10),
    FieldSchema(name="str_100", dtype=DataType.VARCHAR, max_length=100)
]
schema = CollectionSchema(fields=fields)
if clean_exist and utility.has_collection(collection_name):
    utility.drop_collection(collection_name)

collection = Collection(collection_name, schema=schema, num_shards=1)
vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]

buffered_num = 0
# insert data
if prepare_data:
    for i in range(batch_num):
        random_idx = random.randint(0, random_idx_range)
        random_lower = random_idx*random_base
        random_upper = (random_idx+1)*random_base
        data = [
            [i for i in range(nb * i, nb * (i + 1))],
            [random.randint(random_lower, random_upper) for i in range(nb)],
            vectors,
            [generate_random_hex_string(10) for i in range(nb)],
            [generate_random_hex_string(100) for i in range(nb)],
        ]
        collection.insert(data)
        print(f"insert data {i} batch done")
        buffered_num+=nb
        if buffered_num == flush_batch_num:
            collection.flush()
            buffered_num = 0
    if buffered_num > 0:
        collection.flush()
        buffered_num=0
    print("start building flat index")
    collection.create_index("float_vector", {"index_type": "FLAT", "metric_type": "COSINE"})
    print("end building flat index")

print("start loading flat index")
collection.load()
print("end loading flat index")

