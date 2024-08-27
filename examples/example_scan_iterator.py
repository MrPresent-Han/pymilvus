from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import random
import numpy as np
import secrets


def generate_random_hex_string(length):
    return secrets.token_hex(length // 2)


IP = "localhost"
connections.connect("default", host=IP, port="19530")

dim = 128
clean_exist = False
prepare_data = True

fields = [
    FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True),
    FieldSchema(name="int64", dtype=DataType.INT64),
    FieldSchema(name="float", dtype=DataType.FLOAT),
    FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
    FieldSchema(name="bool", dtype=DataType.BOOL),
    FieldSchema(name="string", dtype=DataType.VARCHAR, max_length=512)
]
schema = CollectionSchema(fields=fields)
collection_name = 'test_group_by_' + generate_random_hex_string(24)
if clean_exist and utility.has_collection(collection_name):
    utility.drop_collection(collection_name)

collection = Collection(collection_name, schema=schema)
nb = 1500
batch_num = 3
vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
# insert data
if prepare_data:
    for i in range(batch_num):
        data = [
            [i for i in range(nb * i, nb * (i + 1))],
            [i % 33 for i in range(nb)],
            [np.float32(i) for i in range(nb)],
            vectors,
            [bool(random.randrange(2)) for i in range(nb)],
            [str(i % 44) for i in range(nb * i, nb * (i + 1))],
        ]
        collection.insert(data)
        print("insert data done")
        collection.flush()
    collection.create_index("float_vector", {"metric_type": "COSINE"})

# create collection and load
collection.load()
batch_size = 100
search_params = {"metric_type": "COSINE"}
limit = 3000
expr = "int64>0"

scan_it = collection.scan_iterator(batch_size=batch_size, limit=limit, expr=expr)
print("init scan done")
