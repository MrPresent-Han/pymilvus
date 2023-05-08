import sys

from pymilvus import (
    connections,
    Collection,
)

HOST = "localhost"
PORT = "19530"
COLLECTION_NAME = "hello_milvus"
CONSISTENCY_LEVEL = "Eventually"
LIMIT = 5
USER_ID = "id"
AGE = "age"
DEPOSIT = "deposit"
PICTURE = "picture"

print("start connecting to Milvus")
connections.connect("default", host=HOST, port=PORT)
hello_milvus = Collection(COLLECTION_NAME, consistency_level=CONSISTENCY_LEVEL)

query_iterator = hello_milvus.query_iterator(expr=f"0 <= {AGE}",  output_fields=[USER_ID, AGE],
                                             offset=0, limit=10, consistency_level=CONSISTENCY_LEVEL)


log_file = open("./test.out", 'w')
sys.stdout = log_file

while True:
    res = query_iterator.next()
    if len(res) == 0:
        print("query has empty, break")
        query_iterator.close()
        break
    for i in range(len(res)):
        print(res[i])
sys.stdout.flush()
