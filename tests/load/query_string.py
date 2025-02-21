from pymilvus.milvus_client.milvus_client import MilvusClient

collection_name = "scene_varchar_1"
milvus_client = MilvusClient("http://localhost:19530")
res = milvus_client.query(collection_name, filter="varchar_col=\"A\"", limit=10)
print(f"res:{res}")