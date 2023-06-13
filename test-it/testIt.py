from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import random

HOST = "10.102.9.7"
PORT = 19530

connections.connect("default", host=HOST, port=PORT)

dim = 8
fields = [
    FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True, description='pk'),
    FieldSchema(name="int64", dtype=DataType.INT64),
    FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim)
]
schema = CollectionSchema(fields=fields)
collection = Collection("test_search_iterator", schema=schema)
# utility.drop_collection('test_search_iterator')

nb = 1000
vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
data = [
    [i for i in range(nb)],
    [i for i in range(nb)],
    vectors
]
collection.insert(data)

collection.create_index("float_vector", {"metric_type": "L2"})
collection.load()

limit = 5
expression = "5 <= int64 < 995"
search_params = {"metric_type": "L2"}
collection.search(vectors[:1], "float_vector", search_params, limit, output_fields=['int64'], expr=expression)
res = collection.search_iterator(vectors[:1], "float_vector", search_params, limit,
                                 output_fields=['int64'], expr=expression)


collection.drop()