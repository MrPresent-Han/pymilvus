from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import random
import numpy as np
from constant import IP, dim, nb, batch_num, collection_name
import time
import secrets

def generate_random_hex_string(length):
    return secrets.token_hex(length // 2)

connections.connect("default", host=IP, port="19530")

collection = Collection(collection_name)
    
collection.drop_index()
print("drop index done")
#collection.create_index("float_vector", {"index_type": "FLAT", "metric_type": "COSINE"})
#print("end building flat index")


#start = time.time()
#collection.compact(is_major=True)
#res = collection.get_compaction_state(is_major=True)
#print(res)
#print("Waiting for major compaction complete")
#collection.wait_for_compaction_completed(is_major=True)
#end = time.time()
#print("Major compaction complete in %f s" %(end - start))
#res = collection.get_compaction_state(is_major=True)
#print(res)
#res = utility.get_query_segment_info(collection_name)
#print("after major compaction, segments number is %d"%len(res))
#collection.drop_index()
#print("start rebuild hnsw index")
#collection.create_index("float_vector", {"metric_type": "COSINE"})

#print("rebuid index done")
