from pymilvus.milvus_client.milvus_client import MilvusClient
import time

collection_name = "scene_varchar_4kw"
VEC_FIELD = "float_vector"
#milvus_client = MilvusClient("http://10.104.20.40:19530")
milvus_client = MilvusClient("http://localhost:19530")
#milvus_client.drop_index(collection_name=collection_name, index_name="float_vector")


index_params = milvus_client.prepare_index_params()
index_params.add_index(
    field_name=VEC_FIELD,
    index_type='FLAT',
    metric_type='L2'
)
start_index = time.time()
milvus_client.create_index(collection_name, index_params)
print("index done")