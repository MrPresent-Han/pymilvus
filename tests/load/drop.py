from pymilvus.milvus_client.milvus_client import MilvusClient
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
)
import numpy as np
import time
import random
import string

collection_name = "scene_int64_1"
prepare_new_data = True

ID = "id"
VEC_FIELD = "float_vector"
INT64_FIELD = "int64_1"
DIM = 3
BATCH_COUNT = 1
NUM_ENTITIES = 2000000
rng = np.random.default_rng(seed=19530)

def main():
    milvus_client = MilvusClient("http://10.104.25.157:19530")
    #milvus_client.drop_collection(collection_name)
    milvus_client.load_collection()
    print(f"dropped collection:{collection_name}")

if __name__ == '__main__':
    main()
