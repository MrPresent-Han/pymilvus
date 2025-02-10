from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import random
import secrets

def generate_random_hex_string(length):
    return secrets.token_hex(length // 2)


IP = "localhost"
collection_name = "test_vdc_6395"
dim = 128
nb = 10000
batch_num = 300
flush_batch_num = 100000
clean_exist = True
prepare_data = True

connections.connect("default", host=IP, port="19530")

fields = [
    FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True),
    FieldSchema(name="c_int64", dtype=DataType.INT64),
    FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
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
        data = [
            [i for i in range(nb * i, nb * (i + 1))],
            [random.randint(nb * i, nb * (i + 1)) for i in range(nb)],
            vectors,
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
        buffered_num = 0
    print("start building flat index")
    collection.create_index("float_vector", {"index_type": "IVF_SQ8", "metric_type": "COSINE", "params": {"nlist": 64}})
    print("end building flat index")

print("start loading flat index")
collection.load()
print("end loading flat index")

