from pymilvus.milvus_client.milvus_client import MilvusClient

col_name = "scene_array_varchar_1"

milvus_client = MilvusClient("http://10.104.20.40:19530")
milvus_client.load_collection(collection_name=col_name)
print("load done")