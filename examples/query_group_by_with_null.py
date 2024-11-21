import numpy as np
from pymilvus import (
    connections,
    utility,
    FieldSchema, CollectionSchema, DataType,
    Collection)
import random

names = ["Rachel", "Joe", "Chandler", "Phebe", "Ross", "Monica", None, None]
collection_name = 'test_query_group_by'
clean_exist = False
prepare_data = False
to_flush = True
batch_num = 1
num_entities, dim = 100, 8
none_ratio = 0.2
fmt = "\n=== {:30} ===\n"

print(fmt.format("start connecting to Milvus"))
connections.connect("default", host="localhost", port="19530")

if clean_exist and utility.has_collection(collection_name):
    utility.drop_collection(collection_name)

fields = [
    FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True, auto_id=False),
    FieldSchema(name="c1", dtype=DataType.VARCHAR, max_length=512, nullable=True),
    FieldSchema(name="c2", dtype=DataType.INT16, nullable=True),
    FieldSchema(name="c3", dtype=DataType.INT32, nullable=True),
    FieldSchema(name="c4", dtype=DataType.DOUBLE, nullable=True),
    FieldSchema(name="c5", dtype=DataType.FLOAT_VECTOR, dim=dim),
    FieldSchema(name="c6", dtype=DataType.VARCHAR, max_length=512, nullable=True),
]

schema = CollectionSchema(fields)

print(fmt.format(f"Create collection `{collection_name}`"))
collection = Collection(collection_name, schema, consistency_level="Strong")

if prepare_data:
    rng = np.random.default_rng(seed=19530)
    print(fmt.format("Start inserting entities"))
    for i in range(batch_num):
        pk_data = [i for i in range(num_entities * i, num_entities * (i + 1))]
        c1_data = [random.choice(names) for _ in range(num_entities)]
        c2_data = [random.randint(0, 4) for i in range(num_entities)]
        c3_data = [random.choice(list(range(5)) + [None]) for _ in range(num_entities)]
        c4_data = [random.uniform(0.0, 100.0) for i in range(num_entities)]
        c5_data = rng.random((num_entities, dim))
        c6_data = [random.choice(names) for _ in range(num_entities)]
        entities = [
            pk_data,
            c1_data,
            c2_data,
            c3_data,
            c4_data,
            c5_data,
            c6_data,
        ]
        print(f"c1 with indices:")
        for index, element in enumerate(c1_data):
            print(f"Index {index}: {element}")
        #print(f"c2:{c2_data}")
        #print(f"c3:{c3_data}")
        #print(f"c4:{c4_data}")
        #print(f"c6:{c6_data}")

        insert_result = collection.insert(entities)
        if to_flush:
            print(f"flush batch:{i}")
            collection.flush()
        print(f"inserted batch:{i}")
    print(fmt.format("Start Creating index IVF_FLAT"))
    index = {
        "index_type": "IVF_FLAT",
        "metric_type": "L2",
        "params": {"nlist": 128},
    }
    collection.create_index("c5", index)
print(f"Number of entities in Milvus: {collection.num_entities}")  # check the num_entities
collection.load()

print(f"start to query collection")  # check the num_entities
res = collection.query(expr="c2 < 10", group_by_fields=["c1", "c6"], output_fields=["c1", "c6", "sum(c2)"], timeout=120.0, ignore_null_keys="true")
#res = collection.query(expr="c2 < 10", group_by_fields=["c1"], output_fields=["c1"], timeout=120.0, ignore_null_keys="false")
#res = collection.query(expr="c2 < 10", output_fields=["sum(c2)", "sum(c3)", "sum(c4)"], timeout=120.0)
count = len(res)
print(f"result_count:{count}")
for i in range(count):
    print(f"res={res[i]}")


