from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import random
import numpy as np
import time
import secrets

collection_name = "test_major_delete"
connections.connect("default", host="localhost", port="19530")
collection = Collection(collection_name)

print("Starting major compaction")
start = time.time()
collection.compact(is_major=True)
res = collection.get_compaction_state(is_major=True)
print(res)
print("Waiting for major compaction complete")
collection.wait_for_compaction_completed(is_major=True)
end = time.time()
print("Major compaction complete in %f s" %(end - start))
res = collection.get_compaction_state(is_major=True)
print(res)

res = utility.get_query_segment_info(collection_name)
print("after major compaction, segments number is %d"%len(res))
print(res)