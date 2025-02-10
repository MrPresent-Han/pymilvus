from pymilvus.milvus_client.milvus_client import MilvusClient
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
)
import numpy as np
import time
import random
import string

collection_name = "scene_array_varchar_1"

ID = "id"
VEC_FIELD = "float_vector"
ARR_FIELD = "array_varchar_1"
DIM = 3
BATCH_COUNT = 350
NUM_ENTITIES = 20000
rng = np.random.default_rng(seed=19530)
prepare_new_data = True
do_iteration = False
do_release = True
do_load = True

def main():
    milvus_client = MilvusClient("http://localhost:19530")
    if prepare_new_data:
        # set up schema
        fields = [
            FieldSchema(name=ID, dtype=DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema(name=VEC_FIELD, dtype=DataType.FLOAT_VECTOR, dim=DIM),
            FieldSchema(name=ARR_FIELD, dtype=DataType.ARRAY, element_type=DataType.VARCHAR, max_length=100, max_capacity=10),
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
                    ARR_FIELD: ["0", "0", "0", "0", "0", "0", "0", "0", "0", "0"]
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

    if do_release:
        milvus_client.release_collection(collection_name)
    if do_load:
        start = time.time()
        print(f"start to load collection:{collection_name}")
        #milvus_client.load_collection(collection_name, load_fields=[ID, VEC_FIELD])
        milvus_client.load_collection(collection_name, load_fields=[ID, VEC_FIELD, ARR_FIELD])
        duration = time.time() - start
        print(f"loaded collection:{collection_name}, duration:{duration}")

    if do_iteration:
        iterator = milvus_client.query_iterator(collection_name, output_fields=[ID, VEC_FIELD, ARR_FIELD])
        page_idx = 0
        while True:
            res = iterator.next()
            if len(res) == 0:
                print("query iteration finished, close")
                iterator.close()
                break
            for i in range(len(res)):
                print(res[i][ID])
            page_idx += 1
            print(f"page{page_idx}-------------------------")

if __name__ == '__main__':
    main()
