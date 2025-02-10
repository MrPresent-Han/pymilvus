from pymilvus.milvus_client.milvus_client import MilvusClient
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
)
import numpy as np
import time
import random
import string

def generate_random_string(length: int) -> str:
    if length < 1:
        raise ValueError("Length must be greater than 0")
    characters = string.ascii_letters + string.digits  # Includes a-z, A-Z, 0-9
    return ''.join(random.choice(characters) for _ in range(length))

collection_name = "varchar_col"

length = 1000
ID = "id"
VEC_FIELD = "float_vector"
VARCHAR_FIELD = "varchar_1"
DIM = 3
BATCH_COUNT = 200
NUM_ENTITIES = 20000
rng = np.random.default_rng(seed=19530)
prepare_new_data = False
do_iteration = False
do_release = True
do_load = True
round = 1
do_query = False
clean_exist = False
random_strs = []
str_count = 5000
for i in range(str_count):
    random_strs.append(generate_random_string(length))

def main():
    milvus_client = MilvusClient("http://localhost:19530")
    if clean_exist:
        if milvus_client.has_collection(collection_name):
            milvus_client.drop_collection(collection_name)
    if prepare_new_data:
        # set up schema
        fields = [
            FieldSchema(name=ID, dtype=DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema(name=VEC_FIELD, dtype=DataType.FLOAT_VECTOR, dim=DIM),
            FieldSchema(name=VARCHAR_FIELD, dtype=DataType.VARCHAR, max_length=length),
        ]
        schema = CollectionSchema(fields)
        milvus_client.create_collection(collection_name, dimension=DIM, schema=schema)

        for k in range(BATCH_COUNT):
            # set up data
            entities = []
            for i in range(NUM_ENTITIES*k, NUM_ENTITIES*(k+1)):
                entity = {
                    ID: i,
                    VEC_FIELD: rng.random((1, DIM))[0],
                    VARCHAR_FIELD: random_strs[i % str_count],
                }
                entities.append(entity)
            start_insert = time.time()
            milvus_client.insert(collection_name, entities)
            print(f"inserted data info flush collection:{collection_name}, batch:{k}, "
                  f"insert_duration:{time.time() - start_insert}")

        start_flush = time.time()
        milvus_client.flush(collection_name)
        print(f"flushed data info flush collection:{collection_name}, batch:{k}, "
              f"flush_duration:{time.time() - start_flush}")

        index_params = milvus_client.prepare_index_params()
        index_params.add_index(
            field_name=VEC_FIELD,
            index_type='FLAT',
            metric_type='L2'
        )
        start_index = time.time()
        milvus_client.create_index(collection_name, index_params)
        print(f"flushed data info flush collection:{collection_name}, "
              f"index_duration:{time.time() - start_index}")

    load_duration = 0
    for i in range(round):
        if do_release:
            milvus_client.release_collection(collection_name)
        if do_load:
            start = time.time()
            print(f"start to load collection:{collection_name}")
            milvus_client.load_collection(collection_name, load_fields=[ID, VEC_FIELD, VARCHAR_FIELD])
            duration = time.time() - start
            load_duration+=duration
            print(f"loaded collection:{collection_name}, duration:{duration}")

    print(f"total_load_duration:{load_duration}, round:{round}, avg:{load_duration/round}")
    if do_query:
        for i in range(1000):
            res = milvus_client.query(collection_name, filter="ARRAY_CONTAINS(array_varchar_1, \"0\")", limit=5)
            print(f"res={res}")

if __name__ == '__main__':
    main()
