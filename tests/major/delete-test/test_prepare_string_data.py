from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import random
import numpy as np

IP = "127.0.0.1"
dim = 8
collection_name = "test_major_delete"

connections.connect("default", host=IP, port="19530")

clean_exist = True
prepare_data = True

fields = [
    FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
    FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
    FieldSchema(name="varchar_1", dtype=DataType.VARCHAR, max_length=36),
]
schema = CollectionSchema(fields=fields)
if clean_exist and utility.has_collection(collection_name):
    utility.drop_collection(collection_name)

collection = Collection(collection_name, schema=schema, num_shards=1)


prefix = "f8d5a851-0fc1-4f84-808c-450721a-"
pk_base = 0
# insert data
if prepare_data:
    #0.'f8d5a851-0fc1-4f84-808c-450721a-0' ~ 'f8d5a851-0fc1-4f84-808c-450721a-19': 每个值5万，一共100万
    nb = 50000
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    for i in range(0, 20):
        str_val = prefix + str(i)
        data = [
            [i for i in range(pk_base, pk_base + nb)],
            vectors,
            [str_val for i in range(nb * i, nb * (i + 1))],
        ]
        collection.insert(data)
        print(f"insert data {pk_base} batch done")
        collection.flush()
        pk_base+=nb

    #1. ‘f8d5a851-0fc1-4f84-808c-450721a-20’： 50万
    str_val = prefix + "20"
    for i in range(0, 10):
        data = [
            [i for i in range(pk_base, pk_base + nb)],
            vectors,
            [str_val for i in range(nb * i, nb * (i + 1))],
        ]
        collection.insert(data)
        print(f"insert data {pk_base} batch done")
        collection.flush()
        pk_base+=nb

    #2. ‘f8d5a851-0fc1-4f84-808c-450721a-21’： 250万
    str_val = prefix + "21"
    for i in range(0, 50):
        data = [
            [i for i in range(pk_base, pk_base + nb)],
            vectors,
            [str_val for i in range(nb * i, nb * (i + 1))],
        ]
        collection.insert(data)
        print(f"insert data {pk_base} batch done")
        collection.flush()
        pk_base+=nb

    #3. ‘f8d5a851-0fc1-4f84-808c-450721a-22’： 500万
    str_val = prefix + "22"
    for i in range(0, 100):
        data = [
            [i for i in range(pk_base, pk_base + nb)],
            vectors,
            [str_val for i in range(nb * i, nb * (i + 1))],
        ]
        collection.insert(data)
        print(f"insert data {pk_base} batch done")
        collection.flush()
        pk_base+=nb

    #4. ‘f8d5a851-0fc1-4f84-808c-450721a-23’： 1000万
    str_val = prefix + "23"
    for i in range(0, 200):
        data = [
            [i for i in range(pk_base, pk_base + nb)],
            vectors,
            [str_val for i in range(nb * i, nb * (i + 1))],
        ]
        collection.insert(data)
        print(f"insert data {pk_base} batch done")
        collection.flush()
        pk_base+=nb

    #5. ‘f8d5a851-0fc1-4f84-808c-450721a-24’： 2000万
    str_val = prefix + "24"
    for i in range(0, 400):
        data = [
            [i for i in range(pk_base, pk_base + nb)],
            vectors,
            [str_val for i in range(nb * i, nb * (i + 1))],
        ]
        collection.insert(data)
        print(f"insert data {pk_base} batch done")
        collection.flush()
        pk_base+=nb

    #6. 'f8d5a851-0fc1-4f84-808c-450721a-25' ~ 'f8d5a851-0fc1-4f84-808c-450721a-2024': 每个值6000条，一共1200万
    nb = 6000
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    for i in range(25, 2025):
        str_val = prefix + str(i)
        data = [
            [i for i in range(pk_base, pk_base + nb)],
            vectors,
            [str_val for i in range(nb * i, nb * (i + 1))],
        ]
        collection.insert(data)
        print(f"insert data {pk_base} batch done")
        collection.flush()
        pk_base+=nb

    print("start building flat index")
    collection.create_index("float_vector", {"index_type": "FLAT", "metric_type": "COSINE"})
    print("end building flat index")

print("start loading flat index")
collection.load()
print("end loading flat index")

