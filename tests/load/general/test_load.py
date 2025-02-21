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

#collection_name = "Collection_BrMjiOhTez"
collection_name = "general_8m"
#collection_name = "scene_varchar_2m"
prepare_new_data = False
ID = "id"
VEC_FIELD = "float_vector"
VARCHAR_FIELD = "varchar_col"
INT_FIELD = "int_col"
DIM = 768
BATCH_COUNT = 200
NUM_ENTITIES = 10000
rng = np.random.default_rng(seed=19530)
str_max_len = 10
str_pool_size = 10000
clear_exist = False
host = "https://in01-86fbd863fa4a26b.aws-us-west-2.vectordb-uat3.zillizcloud.com:19536"
#host = "http://localhost:19530"


def main():
    #milvus_client = MilvusClient("http://10.15.33.54:19530", user="root", password="o5)TrT*(w]<fhsAlhR*16vaMh7Dq$>+P")
    milvus_client = MilvusClient(host, user="root", password="h7,kL&+z(~+6Hx4SYVQaG5=Rz$3c8NB|")
    if prepare_new_data:
        if clear_exist and milvus_client.has_collection(collection_name):
            milvus_client.drop_collection(collection_name)
        # set up schema
        fields = [
            FieldSchema(name=ID, dtype=DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema(name=VEC_FIELD, dtype=DataType.FLOAT_VECTOR, dim=DIM),
            FieldSchema(name=VARCHAR_FIELD, dtype=DataType.VARCHAR, max_length=str_max_len),
            FieldSchema(name=INT_FIELD, dtype=DataType.INT64),
        ]
        schema = CollectionSchema(fields)
        milvus_client.create_collection(collection_name, dimension=DIM, schema=schema)
        strs = []
        for i in range(str_pool_size):
            strs.append(generate_random_string(str_max_len))

        for k in range(BATCH_COUNT):
            # set up data
            entities = []
            for i in range(NUM_ENTITIES*k, NUM_ENTITIES*(k+1)):
                entity = {
                    ID: i,
                    VEC_FIELD: rng.random((1, DIM))[0],
                    VARCHAR_FIELD: strs[random.randint(1, str_pool_size) % str_pool_size],
                    INT_FIELD: random.randint(1, NUM_ENTITIES*(k+1))
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
            index_type='HNSW',
            metric_type='L2'
        )
        start_index = time.time()
        milvus_client.create_index(collection_name, index_params)
        print(f"flushed data info flush collection:{collection_name}, "
              f"index_duration:{time.time() - start_index}")

    milvus_client.release_collection(collection_name)
    start = time.time()
    print(f"start to load collection:{collection_name}")
    milvus_client.load_collection(collection_name)
    duration = time.time() - start
    print(f"loaded collection:{collection_name}, duration:{duration}")


if __name__ == '__main__':
    main()
