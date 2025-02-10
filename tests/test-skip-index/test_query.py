from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import random
from constant import dim, nb, collection_name
import time
from constant import IP

connections.connect("default", host=IP, port="19530")
collection = Collection(collection_name)
collection.load()
batch_size = 1000
expr = "0 <= c_int64 <= 10000"
#expr = "500 <= c_int64 <= 2000"
#expr = "500 <= c_int64 <= 600"

number = 5000
duration_sum = 0

for i in range(number):
    start_search = time.time()
    res = collection.query(expr=expr, output_fields=["str_100"], limit=batch_size)
    search_duration = time.time() - start_search;
    print(f"result_len:{len(res)}") 
    duration_sum += search_duration

avg = duration_sum/number
print(f"avg:{avg}")
