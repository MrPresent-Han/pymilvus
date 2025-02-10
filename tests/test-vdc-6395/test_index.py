from pymilvus import Collection, connections

IP = "localhost"
collection_name = "test_vdc_6395"

connections.connect("default", host=IP, port="19530")

collection = Collection(collection_name)
print("start building flat index")
collection.create_index("float_vector", {"index_type": "IVF_SQ8", "metric_type": "COSINE", "params": {"nlist": 64}})
print("end building flat index")