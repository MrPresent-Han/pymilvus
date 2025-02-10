from pymilvus import CollectionSchema, FieldSchema, Collection, connections, DataType, Partition, utility
import random
from constant import dim, nb, collection_name
import time
from constant import IP

connections.connect("default", host=IP, port="19530")
collection = Collection(collection_name)
collection.load()
batch_size = 1000
#expr_base = "str_10<='0000001030'"
#expr_base = "str_10>='0000000000'"
#expr_base = "'0001000000' <= str_10 <= '0005000000'"

number = 3000
duration_sum = 0

for i in range(number):
    start_search = time.time()
    res = collection.query(expr=expr_base, output_fields=[], limit=batch_size)
    search_duration = time.time() - start_search
    print(f"result_len:{len(res)}") 
    duration_sum += search_duration
    avg = duration_sum/(i+1)
    print(f"current avg:{avg}")

avg = duration_sum/number
print(f"avg:{avg}")
