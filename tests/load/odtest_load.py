from pymilvus.milvus_client.milvus_client import MilvusClient
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
)
import numpy as np
import time

collection_name_prefix = "test_milvus_load_"
collection_count = 1
prepare_new_data = True

USER_ID = "id"
PICTURE = "picture"
DIM = 128
NUM_ENTITIES = 20000
rng = np.random.default_rng(seed=19530)


def main():
    milvus_client = MilvusClient("http://localhost:19530")
    if prepare_new_data:
        # set up schema
        fields = [
            FieldSchema(name=USER_ID, dtype=DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema(name=PICTURE, dtype=DataType.FLOAT_VECTOR, dim=DIM)
        ]
        schema = CollectionSchema(fields)

        # set up data
        entities = []
        for i in range(NUM_ENTITIES):
            entity = {
                USER_ID: i,
                PICTURE: rng.random((1, DIM))[0]
            }
            entities.append(entity)

        for i in range(collection_count):
            collection_name = collection_name_prefix + str(i)
            milvus_client.create_collection(collection_name, dimension=DIM, schema=schema)
            milvus_client.insert(collection_name, entities)
            print(f"inserted data info flush collection:{collection_name}")
            milvus_client.flush(collection_name)
            print(f"Finish flush collection:{collection_name}")
            index_params = milvus_client.prepare_index_params()
            index_params.add_index(
                field_name=PICTURE,
                index_type='HNSW',
                metric_type='L2'
            )
            milvus_client.create_index(collection_name, index_params)
            print(f"created index for collection:{collection_name}")

    # ensure release
    for i in range(collection_count):
        collection_name = collection_name_prefix + str(i)
        milvus_client.release_collection(collection_name)
        print(f"released collection:{collection_name}")

    # load
    for i in range(collection_count):
        collection_name = collection_name_prefix + str(i)
        start = time.time()
        milvus_client.load_collection(collection_name)
        duration = time.time() - start
        print(f"loaded collection:{collection_name}, duration:{duration}")


if __name__ == '__main__':
    main()
