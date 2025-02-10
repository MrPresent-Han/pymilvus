from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import random
from constant import dim, nb
import time
from constant import IP

connections.connect("default", host=IP, port="19530")

collection_name = 'test_segment_prune'
collection = Collection(collection_name)
collection.load()

EXPECT_NUM = 10000000
TEST_DURATION = 1200
start = time.time()
while time.time() - start <= TEST_DURATION:
    result = collection.query(expr="pk >= 0", output_fields=["count(*)"])
    res_count = result[0]['count(*)']
    if res_count != EXPECT_NUM:
        print(f"Wrong number, res_count:{res_count}")
        break
    else:
        print(f"Correct number, res_count:{res_count}")
    time.sleep(1)

print("Done")