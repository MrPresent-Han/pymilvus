import numpy as np

from pymilvus import (
    connections,
    utility,
    FieldSchema, CollectionSchema, DataType,
    Collection,
)

HOST = "localhost"
PORT = "19530"
COLLECTION_NAME = "test_iterator"
USER_ID = "id"
MAX_LENGTH = 65535
AGE = "age"
DEPOSIT = "deposit"
PICTURE = "picture"
CONSISTENCY_LEVEL = "Eventually"
LIMIT = 5
NUM_ENTITIES = 3000
DIM = 8


def re_create_collection():
    if utility.has_collection(COLLECTION_NAME):
        utility.drop_collection(COLLECTION_NAME)
        print(f"dropped existed collection{COLLECTION_NAME}")

    fields = [
        FieldSchema(name=USER_ID, dtype=DataType.VARCHAR, is_primary=True,
                    auto_id=False, max_length=MAX_LENGTH),
        FieldSchema(name=AGE, dtype=DataType.INT64),
        FieldSchema(name=DEPOSIT, dtype=DataType.DOUBLE),
        FieldSchema(name=PICTURE, dtype=DataType.FLOAT_VECTOR, dim=DIM)
    ]

    schema = CollectionSchema(fields)
    print(f"Create collection {COLLECTION_NAME}")
    collection = Collection(COLLECTION_NAME, schema, consistency_level=CONSISTENCY_LEVEL)
    return collection


def insert_data(collection):
    rng = np.random.default_rng(seed=19530)
    entities = [
        [str(ni) for ni in range(NUM_ENTITIES)],
        [int(ni % 100) for ni in range(NUM_ENTITIES)],
        [float(ni) for ni in range(NUM_ENTITIES)],
        rng.random((NUM_ENTITIES, DIM)),
    ]
    collection.insert(entities)
    print(f"Finish insert, number of entities in Milvus: {collection.num_entities}")


def prepare_index(collection):
    index = {
        "index_type": "IVF_FLAT",
        "metric_type": "L2",
        "params": {"nlist": 128},
    }

    collection.create_index(PICTURE, index)
    print("Finish Creating index IVF_FLAT")
    collection.load()
    print("Finish Loading index IVF_FLAT")


def prepare_data():
    connections.connect("default", host=HOST, port=PORT)
    collection = re_create_collection()
    insert_data(collection)
    prepare_index(collection)
    return collection


def query_iterate_collection(collection):
    expr = f"{AGE} >= 10"
    query_iterator = collection.query_iterator(expr=expr, output_fields=[USER_ID, AGE],
                                               offset=0, limit=10, consistency_level=CONSISTENCY_LEVEL)
    id_set = set()
    target_id_count = 2700
    while True:
        res = query_iterator.next()
        if len(res) == 0:
            if len(id_set) != target_id_count:
                print("Wrong, miss some entities")
                exit(1)
            print("query iteration finished, close")
            query_iterator.close()
            break
        for i in range(len(res)):
            print(res[i])
            id_set.add(res[i]['id'])


def search_iterator_collection(collection):
    SEARCH_NQ = 1
    DIM = 8
    rng = np.random.default_rng(seed=19530)
    vectors_to_search = rng.random((SEARCH_NQ, DIM))
    search_params = {
        "metric_type": "L2",
        "params": {"nprobe": 10, "radius": 1.0, "range_filter": 20},
    }
    search_iterator = collection.search_iterator(vectors_to_search, PICTURE, search_params, limit=10,
                                                 output_fields=[USER_ID])
    while True:
        res = search_iterator.next()
        if len(res[0]) == 0:
            print("query iteration finished, close")
            search_iterator.close()
            break
        for i in range(len(res[0])):
            print(res[0][i])
        print("-------------------------")

def main():
    collection = prepare_data()
    # query_iterate_collection(collection)
    search_iterator_collection(collection)


if __name__ == '__main__':
    main()
