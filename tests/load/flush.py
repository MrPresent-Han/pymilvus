from pymilvus.milvus_client.milvus_client import MilvusClient
import time

col_name = "scene_array_varchar_1"
VEC_FIELD = "float_vector"

milvus_client = MilvusClient("http://localhost:19530")
milvus_client.flush(collection_name=col_name)
index_params = milvus_client.prepare_index_params()
index_params.add_index(
    field_name=VEC_FIELD,
    index_type='FLAT',
    metric_type='L2'
)
start_index = time.time()
milvus_client.create_index(collection_name=col_name, index_params=index_params)
print(f"flushed data info flush collection:{col_name}, "
      f"index_duration:{time.time() - start_index}")
milvus_client.load_collection(collection_name=col_name)
print("load done")