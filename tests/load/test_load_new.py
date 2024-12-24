from pymilvus.milvus_client.milvus_client import MilvusClient
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
)
import numpy as np
import time
import random
import string

collection_name = "test_milvus_load_2"
prepare_new_data = True

USER_ID = "id"
PICTURE = "picture"
DIM = 8
BATCH_COUNT = 400
NUM_ENTITIES = 20000
rng = np.random.default_rng(seed=19530)
AGE = "age"
DEPOSIT = "deposit"
PICTURE = "picture"
ACCOUNT = "account"
DETAIL = "detail"
characters = string.ascii_letters + string.digits

def generate_random_string(length=100):
    return ''.join(random.choice(characters) for _ in range(length))

def main():
    milvus_client = MilvusClient("http://localhost:19530")
    if prepare_new_data:
        # set up schema
        fields = [
            FieldSchema(name=USER_ID, dtype=DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema(name=AGE, dtype=DataType.INT64),
            FieldSchema(name=DEPOSIT, dtype=DataType.FLOAT),
            FieldSchema(name=PICTURE, dtype=DataType.FLOAT_VECTOR, dim=DIM),
            FieldSchema(name=ACCOUNT, dtype=DataType.VARCHAR, max_length=1000),
            FieldSchema(name=DETAIL, dtype=DataType.JSON),
        ]
        schema = CollectionSchema(fields)
        milvus_client.create_collection(collection_name, dimension=DIM, schema=schema)

        for k in range(BATCH_COUNT):
            # set up data
            entities = []
            for i in range(NUM_ENTITIES*k, NUM_ENTITIES*(k+1)):
                v_str = generate_random_string(100)
                entity = {
                    USER_ID: i,
                    AGE: i % 100,
                    DEPOSIT: float(i % 100),
                    PICTURE: rng.random((1, DIM))[0],
                    ACCOUNT: v_str,
                    DETAIL: {"category": v_str, "price": i % 100, "brand": v_str + "dddd"}
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
            field_name=PICTURE,
            index_type='FLAT',
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
