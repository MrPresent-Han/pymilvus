import os
import time
import random
import string
import numpy as np
import pandas as pd
from pymilvus import (
    connections,
    utility,
    FieldSchema, CollectionSchema, DataType,
    Collection,
)

from loguru import logger
import os
import struct
import sys
path = "/home/major"
log_path = path + "/log" + "/vector_only_major_20m_new_0.log"
logger.add(log_path)
#nas_path = "/Users/binbin/binbin/nas/milvus/raw_data/laion5b_parquet/laion1B_float16/"
nas_path = "/root/"
if not os.path.exists(log_path):
    os.system(f"mkdir -p {log_path}")


fmt = "\n=== {:30} ===\n"
dim = 768

logger.info(fmt.format("start connecting to Milvus"))
host = os.environ.get('MILVUS_HOST')
if host == None:
    host = ""
logger.info(fmt.format(f"Milvus host: {host}"))
#connections.connect(uri="https://in01-f3bad0de26c8b7d.aws-us-west-2.vectordb-uat3.zillizcloud.com:19530", user="root", password="w3)pfRvX0v3/R,o,<Nwn5Qj?=lD/>hxu")

#connections.connect(uri="https://in01-1793fa07f548886.gcp-us-west1.vectordb-uat3.zillizcloud.com:443", user="root", password="T9?O8%V;fu?vG,jC==+V?Pc8,9&Pkey}")
#connections.connect(uri="https://in01-72a07b22f27977e.gcp-us-west1.vectordb-uat3.zillizcloud.com:443 ", user="root", password="K4*BTYX]a.$yb$^lT,6kOT^WE<tR@67p")
connections.connect(uri="https://in01-3dce474892e7e03.gcp-us-west1.vectordb-uat3.zillizcloud.com:443", user="root", password="m4[N]y{Z,9VZ[At2Xy~e(ZVwP?:Eg}=A")


default_fields = [
    FieldSchema(name="count", dtype=DataType.INT64, is_primary=True),
    FieldSchema(name="key", dtype=DataType.INT64),
    FieldSchema(name="random", dtype=DataType.DOUBLE),
    FieldSchema(name="var", dtype=DataType.VARCHAR, max_length=100, is_primary=False),
    FieldSchema(name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=dim, is_clustering_key=True)
]
default_schema = CollectionSchema(fields=default_fields, description="test clustering-key collection")
collection_name = "major_compaction_collection_enable_vector_clustering_key_10M_laion_new"


# 1. create collection
collection = Collection(name=collection_name)



collections_num = utility.list_collections()

# 2.read inserted vectors from files
nb = 10000000
data_set = "laion"
if len(collections_num) == 0:
    logger.info("reading vectors from file started")
    if data_set == "cohere":
        df = pd.read_parquet('train.parquet')
        vectors = df['emb'].tolist()
    if data_set == "sift":
        file_num = 100000
        vectors = np.load('binary_768d_00000.npy')
        for i in range(1,int(nb/file_num)):
            single_vector_batch = np.load(f'binary_768d_0000{i}.npy')
            logger.info("read %d file done: %d" % (i, len(single_vector_batch)))
            assert len(single_vector_batch)== file_num
            vectors = np.concatenate((vectors, single_vector_batch), axis=0)
        vectors = vectors.tolist()
    if data_set == "laion":
        vectors = []
        i = 0
        #vectors = np.load(f'{nas_path}binary_768d_0000{i}.parquet')
        df = pd.read_parquet(f'{nas_path}binary_768d_0000{i}.parquet')
        vectors = df['float32_vector'].tolist()
        i = 1
        while(len(vectors) <= nb):
            if i < 10:
                #single_vector_batch = np.load(f'{nas_path}binary_768d_0000{i}.parquet')
                df = pd.read_parquet(f'{nas_path}binary_768d_0000{i}.parquet')
                single_vector_batch = df['float32_vector'].tolist()
            else:
                df = pd.read_parquet(f'{nas_path}binary_768d_000{i}.parquet')
                single_vector_batch = df['float32_vector'].tolist()
                #single_vector_batch = np.load(f'{nas_path}binary_768d_000{i}.parquet')
            vectors = np.append(vectors, single_vector_batch, axis=0)
            logger.info("read %d entities done: add %d" % (len(vectors), len(single_vector_batch)))
            i = i + 1
        #vectors = vectors.tolist()
    logger.info("read %d vectors from file completed" % (len(vectors)))
    assert len(vectors) >= nb

logger.info("collection num is %d" % (collection.num_entities))
res = utility.get_query_segment_info(collection_name)
logger.info("before major compaction, segments number is %d"%len(res))
logger.info(res)

# 7. release and drop index
#collection.release()
#collection.drop_index()

# 8. create auto index
#logger.info(fmt.format("Start Creating index AUTOINDEX"))
#index = {
#    "index_type": "AUTOINDEX",
#    "metric_type": "L2",
#    "params": {},
#}

#index_start = time.time()
#logger.info("creating index")
#collection.create_index("embeddings", index)
#logger.info("waiting for index completed")
#utility.wait_for_index_building_complete(collection_name)
#res = utility.index_building_progress(collection_name)
#logger.info(res)
#index_end = time.time()

#logger.info(f"create index done in {index_end-index_start} s")

#logger.info(fmt.format("Load"))
#collection.load()


logger.info("Starting major compaction")
start = time.time()
collection.compact(is_major=True)
res = collection.get_compaction_state(is_major=True)
logger.info(res)
logger.info("Waiting for major compaction complete")
collection.wait_for_compaction_completed(is_major=True)
end = time.time()
logger.info("Major compaction complete in %f s" %(end - start))
res = collection.get_compaction_state(is_major=True)
logger.info(res)

res = utility.get_query_segment_info(collection_name)
logger.info("after major compaction, segments number is %d"%len(res))
logger.info(res)

